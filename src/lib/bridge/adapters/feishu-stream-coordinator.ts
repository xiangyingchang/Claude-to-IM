import type { StreamEvent, ToolCallInfo } from '../types.js';

export type FeishuStreamStatus = 'completed' | 'interrupted' | 'error';

export interface FeishuStreamTransport {
  createPost(chatId: string, content: string, replyToMessageId?: string): Promise<string | null>;
  updatePost(messageId: string, content: string): Promise<void>;
  createInteractive(chatId: string, content: string): Promise<string | null>;
  deleteMessage(messageId: string): Promise<void>;
}

export interface FeishuStreamRenderer {
  post(text: string): string;
  /** Render one contiguous group of tool executions as one message. */
  tools(tools: ToolCallInfo[]): string;
  finalCard(text: string, footer: { status: string; elapsed: string }): string;
  hasComplexMarkdown(text: string): boolean;
}

export interface FeishuStreamCoordinatorOptions {
  contentThrottleMs?: number;
  maxEditsPerMessage?: number;
  maxToolGroupEditsPerMessage?: number;
  now?: () => number;
  onError?: (scope: string, error: unknown) => void;
}

interface TextSegment {
  messageId: string;
  /** Global text offset represented by this segment. */
  baseLength: number;
  /** Number of the tool group that preceded this segment. */
  toolGroupSerial: number;
  sentText: string;
  sentContent: string;
  editCount: number;
  lastUpdateAt: number;
}

interface ToolGroup {
  serial: number;
  messageId: string | null;
  tools: ToolCallInfo[];
  lastSentContent: string;
  editCount: number;
}

interface StreamState {
  chatId: string;
  replyToMessageId?: string;
  firstMessagePending: boolean;
  startTime: number;

  /** Full assistant text received from the engine. */
  pendingText: string;
  /** Global offset represented by the current text segment. */
  segmentBaseLength: number;
  currentSegment: TextSegment | null;
  textSegments: TextSegment[];
  contentTimer: ReturnType<typeof setTimeout> | null;

  /** At most one active group exists between two text segments. */
  currentToolGroup: ToolGroup | null;
  toolGroupSerial: number;
  completedToolGroupMessageIds: string[];
  lastSequence: number;

  finalMessageId: string | null;
  finalized: boolean;
}

/**
 * Serializes every Feishu write for one chat and materializes the ordered
 * assistant timeline:
 *
 *   text message -> one contiguous tool-group message -> text message -> final
 *
 * A tool group may contain any number of tool calls. It is opened by the first
 * tool_start event, updated as more tools start/finish, and sealed only when a
 * later visible text event arrives. This is the message granularity used by
 * the ChatGPT/Codex execution view and avoids one-message-per-tool spam.
 */
export class FeishuStreamCoordinator {
  private readonly transport: FeishuStreamTransport;
  private readonly renderer: FeishuStreamRenderer;
  private readonly contentThrottleMs: number;
  private readonly maxEditsPerMessage: number;
  private readonly maxToolGroupEditsPerMessage: number;
  private readonly now: () => number;
  private readonly onError?: (scope: string, error: unknown) => void;

  private readonly states = new Map<string, StreamState>();
  private readonly writeTails = new Map<string, Promise<unknown>>();

  constructor(
    transport: FeishuStreamTransport,
    renderer: FeishuStreamRenderer,
    options: FeishuStreamCoordinatorOptions = {},
  ) {
    this.transport = transport;
    this.renderer = renderer;
    this.contentThrottleMs = options.contentThrottleMs ?? 500;
    this.maxEditsPerMessage = options.maxEditsPerMessage ?? 12;
    this.maxToolGroupEditsPerMessage = options.maxToolGroupEditsPerMessage ?? 15;
    this.now = options.now ?? Date.now;
    this.onError = options.onError;
  }

  start(chatId: string, replyToMessageId?: string): void {
    void this.enqueue(chatId, async () => {
      this.ensureState(chatId, replyToMessageId);
    });
  }

  /** Consume one ordered text/tool event from the engine. */
  onEvent(chatId: string, event: StreamEvent): void {
    void this.enqueue(chatId, async () => {
      const state = this.ensureState(chatId);
      if (state.finalized || event.sequence <= state.lastSequence) return;
      state.lastSequence = event.sequence;

      switch (event.type) {
        case 'text':
          await this.acceptText(state, event.fullText);
          break;
        case 'tool_start':
          await this.startTool(state, event.toolId, event.toolName, event.toolInput);
          break;
        case 'tool_end':
          await this.endTool(state, event.toolId, event.status);
          break;
      }
    });
  }

  /** Flush pending writes before a permission card or another ordered message. */
  flush(chatId: string): Promise<void> {
    return this.enqueue(chatId, async () => {
      const state = this.states.get(chatId);
      if (!state || state.finalized) return;
      this.clearContentTimer(state);
      await this.materializeHeldText(state);
      await this.flushText(state);
      await this.flushToolGroup(state);
    });
  }

  end(chatId: string, status: FeishuStreamStatus, responseText: string): Promise<boolean> {
    return this.enqueue(chatId, async () => this.finalize(chatId, status, responseText));
  }

  cleanup(chatId: string): void {
    const state = this.states.get(chatId);
    if (state) this.clearContentTimer(state);
    this.states.delete(chatId);
  }

  cleanupAll(): void {
    for (const chatId of this.states.keys()) this.cleanup(chatId);
  }

  hasActive(chatId: string): boolean {
    return this.states.has(chatId);
  }

  /** Test/debug snapshot without exposing mutable internal state. */
  snapshot(chatId: string): Record<string, unknown> | null {
    const state = this.states.get(chatId);
    if (!state) return null;
    return {
      currentMessageId: state.currentSegment?.messageId ?? null,
      contentMessageIds: state.textSegments.map((segment) => segment.messageId),
      toolGroupMessageId: state.currentToolGroup?.messageId ?? null,
      completedToolGroupMessageIds: [...state.completedToolGroupMessageIds],
      toolGroupSerial: state.toolGroupSerial,
      lastSequence: state.lastSequence,
      finalized: state.finalized,
      finalMessageId: state.finalMessageId,
    };
  }

  private enqueue<T>(chatId: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.writeTails.get(chatId) ?? Promise.resolve();
    const next = previous.then(operation, operation);
    this.writeTails.set(chatId, next);
    next.finally(() => {
      if (this.writeTails.get(chatId) === next) this.writeTails.delete(chatId);
    }).catch(() => {});
    return next;
  }

  private ensureState(chatId: string, replyToMessageId?: string): StreamState {
    const existing = this.states.get(chatId);
    if (existing) return existing;

    const state: StreamState = {
      chatId,
      replyToMessageId,
      firstMessagePending: true,
      startTime: this.now(),
      pendingText: '',
      segmentBaseLength: 0,
      currentSegment: null,
      textSegments: [],
      contentTimer: null,
      currentToolGroup: null,
      toolGroupSerial: 0,
      completedToolGroupMessageIds: [],
      lastSequence: 0,
      finalMessageId: null,
      finalized: false,
    };
    this.states.set(chatId, state);
    return state;
  }

  private async acceptText(state: StreamState, fullText: string): Promise<void> {
    const previousText = state.pendingText;
    state.pendingText = fullText;

    const textAfterBoundary = this.currentText(state);
    const hasVisibleNewText = textAfterBoundary.trim().length > 0
      && (fullText !== previousText || !state.currentSegment);

    // Text is the boundary that seals the preceding contiguous tool group.
    // Ignore an empty delimiter so a stray newline cannot split a group.
    if (state.currentToolGroup && hasVisibleNewText) {
      await this.closeToolGroup(state);
    }

    if (!textAfterBoundary.trim()) return;

    // The first text after a tool group may be the eventual final answer. Hold
    // it in memory until the turn ends; if another tool starts, that tool is
    // the proof this text was an intermediate segment and we materialize it
    // immediately before the next tool group. This avoids sending a staging
    // message and then visibly recalling it when the final answer arrives.
    if (!state.currentSegment && state.toolGroupSerial > 0) return;

    if (!state.currentSegment) {
      await this.createTextSegment(state, textAfterBoundary);
      return;
    }

    if (textAfterBoundary === state.currentSegment.sentText) return;

    const elapsed = this.now() - state.currentSegment.lastUpdateAt;
    if (state.currentSegment.lastUpdateAt > 0 && elapsed < this.contentThrottleMs) {
      this.scheduleContentFlush(state, this.contentThrottleMs - elapsed);
      return;
    }

    this.clearContentTimer(state);
    await this.flushText(state);
  }

  private currentText(state: StreamState): string {
    if (state.pendingText.length < state.segmentBaseLength) return state.pendingText;
    return state.pendingText.slice(state.segmentBaseLength);
  }

  private async createTextSegment(state: StreamState, text: string): Promise<void> {
    const messageId = await this.createPost(state, this.renderer.post(text));
    if (!messageId) return;

    const segment: TextSegment = {
      messageId,
      baseLength: state.segmentBaseLength,
      toolGroupSerial: state.toolGroupSerial,
      sentText: text,
      sentContent: this.renderer.post(text),
      editCount: 0,
      lastUpdateAt: this.now(),
    };
    state.currentSegment = segment;
    state.textSegments.push(segment);
  }

  private async flushText(state: StreamState): Promise<void> {
    const text = this.currentText(state);
    if (!text.trim()) return;

    if (!state.currentSegment) {
      await this.createTextSegment(state, text);
      return;
    }

    const segment = state.currentSegment;
    if (text === segment.sentText) return;

    const content = this.renderer.post(text);
    if (content === segment.sentContent) return;

    if (segment.editCount >= this.maxEditsPerMessage) {
      await this.rolloverText(state, text);
      return;
    }

    try {
      segment.editCount++;
      await this.transport.updatePost(segment.messageId, content);
      segment.sentText = text;
      segment.sentContent = content;
      segment.lastUpdateAt = this.now();
    } catch (error) {
      segment.editCount = Math.max(0, segment.editCount - 1);
      segment.lastUpdateAt = 0;
      this.report('stream.content.update', error);
      if (this.isEditLimitError(error)) await this.rolloverText(state, text);
    }
  }

  private async rolloverText(state: StreamState, text: string): Promise<void> {
    const previousText = state.currentSegment?.sentText ?? '';
    const tail = previousText && text.startsWith(previousText)
      ? text.slice(previousText.length)
      : text;
    const messageId = await this.createPost(state, this.renderer.post(tail || '💭 Thinking...'));
    if (!messageId) return;

    const segment: TextSegment = {
      messageId,
      baseLength: state.pendingText.length - tail.length,
      toolGroupSerial: state.toolGroupSerial,
      sentText: tail,
      sentContent: this.renderer.post(tail || '💭 Thinking...'),
      editCount: 0,
      lastUpdateAt: this.now(),
    };
    state.segmentBaseLength = segment.baseLength;
    state.currentSegment = segment;
    state.textSegments.push(segment);
  }

  private async startTool(
    state: StreamState,
    toolId: string,
    toolName: string,
    toolInput?: string,
  ): Promise<void> {
    if (!state.currentToolGroup) {
      this.clearContentTimer(state);
      await this.materializeHeldText(state);
      await this.closeTextSegment(state);
      state.toolGroupSerial++;
      state.currentToolGroup = {
        serial: state.toolGroupSerial,
        messageId: null,
        tools: [],
        lastSentContent: '',
        editCount: 0,
      };
    }

    const group = state.currentToolGroup;
    const existing = group.tools.find((tool) => tool.id === toolId);
    if (existing) {
      // Codex emits a lightweight tool_use at content_block_start and the
      // complete arguments again in the assistant block. Keep one visible
      // row, but merge the later name/input so file paths and commands are not
      // lost after the first `{}` event.
      let changed = false;
      if (toolName && (!existing.name || existing.name === 'tool') && existing.name !== toolName) {
        existing.name = toolName;
        changed = true;
      }
      if (toolInput && existing.input !== toolInput) {
        existing.input = toolInput;
        changed = true;
      }
      if (changed) await this.flushToolGroup(state);
      return;
    }
    group.tools.push({
      id: toolId,
      name: toolName,
      status: 'running',
      input: toolInput,
    });
    await this.flushToolGroup(state);
  }

  private async endTool(
    state: StreamState,
    toolId: string,
    status: 'complete' | 'error',
  ): Promise<void> {
    if (!state.currentToolGroup) {
      this.clearContentTimer(state);
      await this.materializeHeldText(state);
      await this.closeTextSegment(state);
      state.toolGroupSerial++;
      state.currentToolGroup = {
        serial: state.toolGroupSerial,
        messageId: null,
        tools: [],
        lastSentContent: '',
        editCount: 0,
      };
    }

    const group = state.currentToolGroup;
    const existing = group.tools.find((tool) => tool.id === toolId);
    if (existing) {
      existing.status = status;
    } else {
      group.tools.push({ id: toolId, name: 'tool', status });
    }
    await this.flushToolGroup(state);
  }

  private async closeTextSegment(state: StreamState): Promise<void> {
    if (state.currentSegment) {
      await this.flushText(state);
      // A permission/API barrier may arrive immediately after a throttled
      // update. Give the same message one forced retry before sealing it.
      if (state.currentSegment && state.currentSegment.sentText !== this.currentText(state)) {
        await this.flushText(state);
      }
    }
    state.currentSegment = null;
    state.segmentBaseLength = state.pendingText.length;
  }

  /** Materialize text after a closed tool group only when it is known to be intermediate. */
  private async materializeHeldText(state: StreamState): Promise<void> {
    if (state.currentSegment || state.currentToolGroup || state.toolGroupSerial === 0) return;
    const text = this.currentText(state);
    if (text.trim()) await this.createTextSegment(state, text);
  }

  private async flushToolGroup(state: StreamState): Promise<void> {
    const group = state.currentToolGroup;
    if (!group || group.tools.length === 0) return;

    const content = this.renderer.tools(group.tools);
    if (content === group.lastSentContent) return;

    if (!group.messageId) {
      const messageId = await this.createPost(state, content);
      if (!messageId) return;
      group.messageId = messageId;
      group.lastSentContent = content;
      group.editCount = 0;
      return;
    }

    if (group.editCount >= this.maxToolGroupEditsPerMessage) {
      await this.rolloverToolGroup(state, content);
      return;
    }

    try {
      group.editCount++;
      await this.transport.updatePost(group.messageId, content);
      group.lastSentContent = content;
    } catch (error) {
      group.editCount = Math.max(0, group.editCount - 1);
      this.report('stream.tool-group.update', error);
      if (this.isEditLimitError(error)) await this.rolloverToolGroup(state, content);
    }
  }

  private async rolloverToolGroup(state: StreamState, content: string): Promise<void> {
    const group = state.currentToolGroup;
    if (!group) return;
    const oldMessageId = group.messageId;
    const messageId = await this.createPost(state, content);
    if (!messageId) return;
    group.messageId = messageId;
    group.lastSentContent = content;
    group.editCount = 0;
    if (oldMessageId) {
      try {
        await this.transport.deleteMessage(oldMessageId);
      } catch (error) {
        this.report('stream.tool-group.delete-old', error);
      }
    }
  }

  private async closeToolGroup(state: StreamState): Promise<void> {
    const group = state.currentToolGroup;
    if (!group) return;
    await this.flushToolGroup(state);
    if (group.messageId) state.completedToolGroupMessageIds.push(group.messageId);
    state.currentToolGroup = null;
  }

  private async finalize(
    chatId: string,
    status: FeishuStreamStatus,
    responseText: string,
  ): Promise<boolean> {
    const state = this.states.get(chatId);
    if (!state) return false;
    if (state.finalized) return state.finalMessageId !== null;

    this.clearContentTimer(state);
    if (responseText.length > state.pendingText.length) state.pendingText = responseText;

    // Text after the final tool group is intentionally held for the final
    // message. Flushing it here would create a staging post that must then be
    // recalled. Text before any tool still streams normally.
    if (state.currentSegment || state.toolGroupSerial === 0) {
      await this.flushText(state);
    }
    if (state.currentToolGroup && status === 'interrupted') {
      for (const tool of state.currentToolGroup.tools) {
        if (tool.status === 'running') tool.status = 'error';
      }
    }
    await this.flushToolGroup(state);
    await this.closeToolGroup(state);

    const statusLabels: Record<FeishuStreamStatus, string> = {
      completed: '✅ Completed',
      interrupted: '⚠️ Interrupted',
      error: '❌ Error',
    };
    const finalStatus = statusLabels[status];
    const elapsed = this.formatElapsed(this.now() - state.startTime);
    const currentText = state.currentSegment?.sentText || this.currentText(state);
    const finalText = state.toolGroupSerial > 0
      ? (currentText || responseText || '💭 Thinking...')
      : (responseText || currentText || '💭 Thinking...');
    // A successful answer is already self-explanatory. Keep diagnostic status
    // only for interruption/error so the final normal text stays clean.
    const footer = status === 'completed'
      ? { status: '', elapsed: '' }
      : { status: finalStatus, elapsed };
    const finalSuffix = status === 'completed'
      ? ''
      : `\n\n${finalStatus} · ${elapsed}`;

    const complexFinal = this.renderer.hasComplexMarkdown(finalText);
    let delivered = false;
    if (!complexFinal && state.currentSegment && state.currentSegment.toolGroupSerial === state.toolGroupSerial) {
      // A normal Markdown text segment is already the final visible message.
      // Promote it in place instead of recalling it and creating a duplicate.
      state.finalized = true;
      state.finalMessageId = state.currentSegment.messageId;
      return true;
    }

    // Complex Markdown needs a card message type. Only this exceptional path
    // removes a previously-created post staging segment.
    if (complexFinal) {
      await this.removeFinalPreviewSegments(state);
      delivered = await this.tryCreateFinal(
        state,
        () => this.transport.createInteractive(chatId, this.renderer.finalCard(finalText, footer)),
        'stream.final.card',
      );
    }
    if (!delivered) {
      delivered = await this.tryCreateFinal(
        state,
        () => this.createPost(state, this.renderer.post(`${finalText}${finalSuffix}`)),
        'stream.final.post',
      );
    }

    state.finalized = delivered;
    return delivered;
  }

  private async removeFinalPreviewSegments(state: StreamState): Promise<void> {
    const finalSerial = state.toolGroupSerial;
    const candidates = finalSerial === 0
      ? state.textSegments
      : state.currentSegment?.sentText.trim()
        ? state.textSegments.filter((segment) => segment.toolGroupSerial === finalSerial)
        : [];

    for (const segment of candidates) {
      try {
        await this.transport.deleteMessage(segment.messageId);
      } catch (error) {
        this.report('stream.final.preview-delete', error);
      }
    }
  }

  private async tryCreateFinal(
    state: StreamState,
    create: () => Promise<string | null>,
    scope: string,
  ): Promise<boolean> {
    try {
      const messageId = await create();
      if (!messageId) return false;
      state.finalMessageId = messageId;
      return true;
    } catch (error) {
      this.report(scope, error);
      return false;
    }
  }

  private async createPost(state: StreamState, content: string): Promise<string | null> {
    try {
      const replyToMessageId = state.firstMessagePending ? state.replyToMessageId : undefined;
      const messageId = await this.transport.createPost(state.chatId, content, replyToMessageId);
      if (messageId) state.firstMessagePending = false;
      return messageId;
    } catch (error) {
      this.report('stream.post.create', error);
      return null;
    }
  }

  private scheduleContentFlush(state: StreamState, delay: number): void {
    if (state.contentTimer) return;
    state.contentTimer = setTimeout(() => {
      state.contentTimer = null;
      void this.enqueue(state.chatId, async () => {
        const current = this.states.get(state.chatId);
        if (current !== state || state.finalized) return;
        await this.flushText(state);
      });
    }, Math.max(0, delay));
  }

  private clearContentTimer(state: StreamState): void {
    if (!state.contentTimer) return;
    clearTimeout(state.contentTimer);
    state.contentTimer = null;
  }

  private isEditLimitError(error: unknown): boolean {
    const value = error as { code?: number; response?: { code?: number }; message?: string } | null;
    const code = value?.code ?? value?.response?.code;
    const message = error instanceof Error ? error.message : String(error ?? '');
    return code === 230072 || /230072|edit.{0,20}(limit|number)|reached the number of times/i.test(message);
  }

  private report(scope: string, error: unknown): void {
    if (this.onError) {
      this.onError(scope, error);
      return;
    }
    console.warn(`[feishu-stream] ${scope}:`, error instanceof Error ? error.message : error);
  }

  private formatElapsed(ms: number): string {
    if (ms < 1000) return `${ms}ms`;
    const sec = ms / 1000;
    if (sec < 60) return `${sec.toFixed(1)}s`;
    const min = Math.floor(sec / 60);
    const remSec = Math.floor(sec % 60);
    return `${min}m ${remSec}s`;
  }
}
