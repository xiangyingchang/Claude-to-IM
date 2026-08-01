import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { StreamEvent, ToolCallInfo } from '../../lib/bridge/types.js';
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
    post: (text: string) => `post:${text}`,
    tools: (tools: ToolCallInfo[]) => `tools:${tools.map((tool) => `${tool.status}:${tool.id}`).join('|')}`,
    finalCard: (text: string, footer: { status: string; elapsed: string }) => `card:${text}|${footer.status}|${footer.elapsed}`,
    hasComplexMarkdown: (text: string) => text.includes('```'),
  };

  const coordinator = new FeishuStreamCoordinator(transport, renderer, {
    contentThrottleMs: 0,
    now: () => 1000,
    ...options,
  });

  return { coordinator, operations, getMaxActiveWrites: () => maxActiveWrites };
}

function emit(coordinator: FeishuStreamCoordinator, event: StreamEvent, chatId = 'chat-1'): void {
  coordinator.onEvent(chatId, event);
}

function text(sequence: number, fullText: string): StreamEvent {
  return { type: 'text', fullText, sequence };
}

function toolStart(sequence: number, toolId: string): StreamEvent {
  return { type: 'tool_start', toolId, toolName: 'Bash', toolInput: `echo ${toolId}`, sequence };
}

function toolEnd(sequence: number, toolId: string, status: 'complete' | 'error' = 'complete'): StreamEvent {
  return { type: 'tool_end', toolId, status, sequence };
}

describe('FeishuStreamCoordinator', () => {
  it('groups consecutive tools into one message between text messages', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1', 'in-1');
    emit(coordinator, text(1, 'A'));
    emit(coordinator, toolStart(2, '1'));
    emit(coordinator, toolEnd(3, '1'));
    emit(coordinator, toolStart(4, '2'));
    emit(coordinator, toolEnd(5, '2'));
    emit(coordinator, text(6, 'AB'));
    emit(coordinator, text(7, 'ABC'));
    const delivered = await coordinator.end('chat-1', 'completed', 'ABC');

    assert.equal(delivered, true);
    const creates = operations.filter((operation) => operation.type === 'create');
    const groupCreates = creates.filter((operation) => operation.content?.startsWith('tools:'));
    assert.equal(groupCreates.length, 1, 'two tools in one contiguous phase must create one group message');
    assert.equal(groupCreates[0]?.content, 'tools:running:1');

    const groupId = groupCreates[0]?.id;
    assert.ok(groupId);
    assert.ok(operations.some((operation) => operation.type === 'update' && operation.id === groupId && operation.content === 'tools:complete:1|complete:2'));

    const finalIndex = operations.findIndex((operation) => operation.type === 'create' && operation.content === 'post:BC');
    const groupLastIndex = operations.reduce((last, operation, index) => operation.id === groupId ? index : last, -1);
    assert.ok(groupLastIndex >= 0 && groupLastIndex < finalIndex, 'final message must be created after the tool group');
    assert.equal(operations[finalIndex]?.content?.includes('Completed'), false);

    // The final text after the last tool was held until completion; no staging
    // message was created and therefore no recall is needed.
    assert.equal(operations.filter((operation) => operation.type === 'delete').length, 0);
    assert.equal(operations.filter((operation) => operation.type === 'interactive').length, 0);
  });

  it('opens a new tool-group message after a later text segment', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1');
    emit(coordinator, text(1, 'before'));
    emit(coordinator, toolStart(2, '1'));
    emit(coordinator, toolEnd(3, '1'));
    emit(coordinator, text(4, 'beforemiddle'));
    emit(coordinator, toolStart(5, '2'));
    emit(coordinator, toolEnd(6, '2'));
    emit(coordinator, text(7, 'beforemiddleafter'));
    const delivered = await coordinator.end('chat-1', 'completed', 'beforemiddleafter');

    assert.equal(delivered, true);
    const groups = operations.filter((operation) => operation.type === 'create' && operation.content?.startsWith('tools:'));
    assert.equal(groups.length, 2);
    const group1Index = operations.indexOf(groups[0]!);
    const group2Index = operations.indexOf(groups[1]!);
    assert.ok(group1Index < group2Index);
    assert.equal(operations.filter((operation) => operation.type === 'create' && operation.content?.startsWith('post:before')).length, 1);
  });

  it('serializes all network writes for a chat', async () => {
    const harness = createHarness();
    const { coordinator } = harness;

    coordinator.start('chat-1');
    emit(coordinator, text(1, 'one'));
    emit(coordinator, text(2, 'two'));
    emit(coordinator, text(3, 'three'));
    emit(coordinator, toolStart(4, '1'));
    emit(coordinator, toolEnd(5, '1'));
    await coordinator.end('chat-1', 'completed', 'three');

    assert.equal(harness.getMaxActiveWrites(), 1);
  });

  it('rolls over text before the edit budget is exhausted', async () => {
    const { coordinator, operations } = createHarness({ maxEditsPerMessage: 2 });

    coordinator.start('chat-1');
    emit(coordinator, text(1, 'A'));
    emit(coordinator, text(2, 'AB'));
    emit(coordinator, text(3, 'ABC'));
    emit(coordinator, text(4, 'ABCD'));
    await coordinator.end('chat-1', 'completed', 'ABCD');

    const creates = operations.filter((operation) => operation.type === 'create');
    assert.ok(creates.some((operation) => operation.content === 'post:D'));
    assert.equal(operations.filter((operation) => operation.type === 'update').length, 2);
  });

  it('falls back to a fresh text segment when Feishu rejects an edit with 230072', async () => {
    let failed = false;
    const { coordinator, operations } = createHarness({}, () => {
      if (failed) return null;
      failed = true;
      const error = new Error('The message has reached the number of times it can be edited');
      (error as Error & { code?: number }).code = 230072;
      return error;
    });

    coordinator.start('chat-1');
    emit(coordinator, text(1, 'A'));
    emit(coordinator, text(2, 'AB'));
    await coordinator.end('chat-1', 'completed', 'AB');

    assert.ok(operations.some((operation) => operation.type === 'create' && operation.content === 'post:B'));
    assert.equal(operations.filter((operation) => operation.type === 'update').length, 0);
  });

  it('keeps the process messages and puts a complex Markdown final card last', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1');
    emit(coordinator, text(1, '先说明'));
    emit(coordinator, toolStart(2, '1'));
    emit(coordinator, toolEnd(3, '1'));
    emit(coordinator, text(4, '先说明\n\n最终 ```ts\nconst x = 1;\n```'));
    const delivered = await coordinator.end('chat-1', 'completed', '先说明\n\n最终 ```ts\nconst x = 1;\n```');

    assert.equal(delivered, true);
    const interactiveIndex = operations.findIndex((operation) => operation.type === 'interactive');
    assert.ok(interactiveIndex > operations.findIndex((operation) => operation.type === 'create' && operation.content?.startsWith('tools:')));
    assert.equal(operations.filter((operation) => operation.type === 'interactive').length, 1);
    assert.equal(operations.filter((operation) => operation.type === 'delete').length, 0);
  });

  it('ignores stale events so a late callback cannot reopen an old group', async () => {
    const { coordinator, operations } = createHarness();

    coordinator.start('chat-1');
    emit(coordinator, text(2, 'A'));
    emit(coordinator, toolStart(3, '1'));
    emit(coordinator, text(4, 'AB'));
    emit(coordinator, toolStart(3, 'late'));
    await coordinator.end('chat-1', 'completed', 'AB');

    const groups = operations.filter((operation) => operation.type === 'create' && operation.content?.startsWith('tools:'));
    assert.equal(groups.length, 1);
    assert.equal(groups[0]?.content, 'tools:running:1');
  });
});
