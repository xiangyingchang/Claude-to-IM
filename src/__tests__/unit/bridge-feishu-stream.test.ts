import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { ToolCallInfo } from '../../lib/bridge/types.js';
import {
  FeishuStreamCoordinator,
  type FeishuStreamRenderer,
  type FeishuStreamTransport,
} from '../../lib/bridge/adapters/feishu-stream-coordinator.js';

interface Operation {
  type: 'create' | 'update' | 'interactive' | 'delete';
  id?: string;
  content?: string;
}

function createHarness(
  options: ConstructorParameters<typeof FeishuStreamCoordinator>[2] = {},
  updateFailure?: () => Error | null,
) {
  let nextId = 0;
  let activeWrites = 0;
  let maxActiveWrites = 0;
  const operations: Operation[] = [];
  const contents = new Map<string, string>();

  const enterWrite = async <T>(fn: () => T | Promise<T>): Promise<T> => {
    activeWrites++;
    maxActiveWrites = Math.max(maxActiveWrites, activeWrites);
    try {
      // Make accidental parallel writes observable without making the test slow.
      await new Promise((resolve) => setTimeout(resolve, 1));
      return await fn();
    } finally {
      activeWrites--;
    }
  };

  const transport: FeishuStreamTransport = {
    createPost: async (_chatId, content) => enterWrite(() => {
      const id = `m${++nextId}`;
      contents.set(id, content);
      operations.push({ type: 'create', id, content });
      return id;
    }),
    updatePost: async (id, content) => enterWrite(() => {
      const failure = updateFailure?.();
      if (failure) throw failure;
      contents.set(id, content);
      operations.push({ type: 'update', id, content });
    }),
    createInteractive: async (_chatId, content) => enterWrite(() => {
      const id = `m${++nextId}`;
      contents.set(id, content);
      operations.push({ type: 'interactive', id, content });
      return id;
    }),
    deleteMessage: async (id) => enterWrite(() => {
      operations.push({ type: 'delete', id });
      contents.delete(id);
    }),
  };

  const renderer: FeishuStreamRenderer = {
    post: (text) => `post:${text}`,
    progress: (tools: ToolCallInfo[]) => {
      const lines = tools.map((tool) => `${tool.status}:${tool.id}`);
      return { lines, content: `progress:${lines.join('|')}` };
    },
    finalizeProgress: (content) => content.replaceAll('running', 'complete'),
    finalCard: (text, _tools, footer) => `card:${text}|${footer.status}|${footer.elapsed}`,
    hasComplexMarkdown: (text) => text.includes('```'),
  };

  const coordinator = new FeishuStreamCoordinator(transport, renderer, {
    contentThrottleMs: 0,
    progressThrottleMs: 0,
    now: () => 1000,
    ...options,
  });

  return { coordinator, operations, getMaxActiveWrites: () => maxActiveWrites };
}

async function flush(coordinator: FeishuStreamCoordinator, chatId = 'chat-1'): Promise<void> {
  await coordinator.flush(chatId);
}

function tool(id: string, status: ToolCallInfo['status'] = 'running'): ToolCallInfo {
  return { id, name: 'Bash', status, input: `echo ${id}` };
}

describe('FeishuStreamCoordinator', () => {
  it('keeps content and tool bubbles in Hermes order without repeating prefixes', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1', 'in-1');
    coordinator.onText('chat-1', 'A');
    coordinator.onTools('chat-1', [tool('1')]);
    coordinator.onTools('chat-1', [tool('1', 'complete')]);
    coordinator.onText('chat-1', 'AB');
    coordinator.onTools('chat-1', [tool('1', 'complete'), tool('2')]);
    coordinator.onText('chat-1', 'ABC');
    await flush(coordinator);

    const creates = operations.filter((operation) => operation.type === 'create');
    const updates = operations.filter((operation) => operation.type === 'update');
    assert.equal(creates[0]?.content, 'post:💭 Thinking...');
    assert.ok(creates.some((operation) => operation.content?.startsWith('progress:running:1')));
    assert.ok(creates.some((operation) => operation.content?.includes('running:2')));

    const textUpdates = updates.filter((operation) => operation.content?.startsWith('post:'));
    assert.ok(textUpdates.some((operation) => operation.content === 'post:A'));
    assert.ok(textUpdates.some((operation) => operation.content === 'post:B'));
    assert.ok(textUpdates.some((operation) => operation.content === 'post:C'));
    assert.equal(textUpdates.some((operation) => operation.content === 'post:AB'), false);
    assert.equal(textUpdates.some((operation) => operation.content === 'post:ABC'), false);
  });

  it('serializes all network writes for a chat', async () => {
    const harness = createHarness();
    const { coordinator } = harness;

    coordinator.start('chat-1');
    coordinator.onText('chat-1', 'one');
    coordinator.onText('chat-1', 'two');
    coordinator.onText('chat-1', 'three');
    await flush(coordinator);

    assert.equal(harness.getMaxActiveWrites(), 1);
  });

  it('rolls over before the edit budget can hit Feishu limit', async () => {
    const { coordinator, operations } = createHarness({ maxEditsPerMessage: 2 });

    coordinator.start('chat-1');
    coordinator.onText('chat-1', 'A');
    coordinator.onText('chat-1', 'AB');
    coordinator.onText('chat-1', 'ABC');
    await flush(coordinator);

    const creates = operations.filter((operation) => operation.type === 'create');
    assert.equal(creates.length, 2);
    assert.equal(creates[1]?.content, 'post:C');
    assert.equal(operations.filter((operation) => operation.type === 'update').length, 2);
  });

  it('falls back to a fresh post when Feishu rejects an edit with 230072', async () => {
    let failed = false;
    const { coordinator, operations } = createHarness({}, () => {
      if (failed) return null;
      failed = true;
      const error = new Error('The message has reached the number of times it can be edited');
      (error as Error & { code?: number }).code = 230072;
      return error;
    });

    coordinator.start('chat-1');
    coordinator.onText('chat-1', 'A');
    await flush(coordinator);

    const creates = operations.filter((operation) => operation.type === 'create');
    assert.equal(creates.length, 2);
    assert.equal(creates[1]?.content, 'post:A');
    assert.equal(operations.filter((operation) => operation.type === 'update').length, 0);
  });

  it('puts the final answer after tool progress and removes fragmented previews', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1');
    coordinator.onText('chat-1', 'A');
    coordinator.onTools('chat-1', [tool('1')]);
    coordinator.onText('chat-1', 'AB');
    const delivered = await coordinator.end('chat-1', 'completed', 'AB');

    assert.equal(delivered, true);
    const finalCreate = operations
      .filter((operation) => operation.type === 'create')
      .at(-1);
    assert.equal(finalCreate?.content?.includes('post:AB'), true);
    const progressIndex = operations.findIndex((operation) => operation.content?.startsWith('progress:'));
    const finalIndex = operations.lastIndexOf(finalCreate!);
    assert.equal(progressIndex >= 0 && progressIndex < finalIndex, true);
    assert.ok(operations.some((operation) => operation.type === 'delete'));
  });

  it('replaces all content previews with one final card for complex markdown', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1');
    coordinator.onText('chat-1', 'answer ```ts\nconst x = 1;\n```');
    const delivered = await coordinator.end('chat-1', 'completed', 'answer ```ts\nconst x = 1;\n```');

    assert.equal(delivered, true);
    assert.equal(operations.filter((operation) => operation.type === 'interactive').length, 1);
    assert.ok(operations.some((operation) => operation.type === 'delete'));
  });
});
