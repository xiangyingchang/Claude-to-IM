import type { ToolCallInfo } from '../types.js';

export type FeishuStreamStatus = 'completed' | 'interrupted' | 'error';

export interface FeishuStreamTransport {
  createPost(chatId: string, content: string, replyToMessageId?: string): Promise<string | null>;
  updatePost(messageId: string, content: string): Promise<void>;
  createInteractive(chatId: string, content: string): Promise<string | null>;
  deleteMessage(messageId: string): Promise<void>;
}

export interface FeishuProgressRender {
  lines: string[];
  content: string;
}

export interface FeishuStreamRenderer {
  post(text: string): string;
  progress(tools: ToolCallInfo[]): FeishuProgressRender;
  finalizeProgress(content: string): string;
  finalCard(text: string, tools: ToolCallInfo[], footer: { status: string; elapsed: string }): string;
  hasComplexMarkdown(text: string): boolean;
}

export interface FeishuStreamCoordinatorOptions {
  contentThrottleMs?: number;
  progressThrottleMs?: number;
  maxEditsPerMessage?: number;
  maxProgressEditsPerMessage?: number;
  now?: () => number;
  onError?: (scope: string, error: unknown) => void;
}

interface StreamState {
  chatId: string;
  replyToMessageId?: string;
  startTime: number;

  /** The latest full assistant text received from the engine. */
  pendingText: string;
  /** Global-text offset represented by the current content message. */
  baseLength: number;
  /** Raw text currently represented by the current content message. */
  currentSegmentText: string;
  /** Whether the current message has been closed at a tool boundary. */
  segmentClosed: boolean;
  thinking: boolean;

  currentMessageId: string | null;
  contentMessageIds: string[];
  contentEditCount: number;
  lastContentUpdateAt: number;
  lastSentContent: string;
  lastSentText: string;
  contentTimer: ReturnType<typeof setTimeout> | null;

  toolCalls: ToolCallInfo[];
  progressMessageId: string | null;
  progressMessageIds: string[];
  progressLines: string[];
  lastProgressContent: string;
  lastProgressUpdateAt: number;
  progressEditCount: number;
  progressTimer: ReturnType<typeof setTimeout> | null;
}

/**
 * Serializes all Feishu writes for one chat and models the visible stream as
 * content segments separated by tool-progress bubbles.
 *
 * The coordinator deliberately accepts the engine's accumulated text API for
 * compatibility with the bridge. It owns the segment offset, so a boundary
 * can never accidentally write the whole accumulated answer into the next
 * message.
 */
export class FeishuStreamCoordinator {
  private readonly transport: FeishuStreamTransport;
  private readonly renderer: FeishuStreamRenderer;
  private readonly contentThrottleMs: number;
  private readonly progressThrottleMs: number;
  private readonly maxEditsPerMessage: number;
  private readonly maxProgressEditsPerMessage: number;
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
    this.progressThrottleMs = options.progressThrottleMs ?? 1500;
    this.maxEditsPerMessage = options.maxEditsPerMessage ?? 12;
    this.maxProgressEditsPerMessage = options.maxProgressEditsPerMessage ?? 15;
    this.now = options.now ?? Date.now;
    this.onError = options.onError;
  }

  start(chatId: string, replyToMessageId?: string): void {
    void this.enqueue(chatId, async () => {
      await this.ensureState(chatId, replyToMessageId);
    });
  }

  onText(chatId: string, fullText: string): void {
    void this.enqueue(chatId, async () => {
      const state = await this.ensureState(chatId);
      if (!state) return;

      state.pendingText = fullText;
      if (state.thinking && fullText.trim()) state.thinking = false;

      if (state.segmentClosed) {
        await this.openContentSegment(state);
      }

      const currentText = this.currentText(state);
      if (currentText === state.lastSentText) return;

      const elapsed = this.now() - state.lastContentUpdateAt;
      if (state.lastContentUpdateAt > 0 && elapsed < this.contentThrottleMs) {
        this.scheduleContentFlush(state, this.contentThrottleMs - elapsed);
        return;
      }

      this.clearContentTimer(state);
      await this.flushContent(state);
    });
  }

  onTools(chatId: string, tools: ToolCallInfo[]): void {
    void this.enqueue(chatId, async () => {
      const state = await this.ensureState(chatId);
      if (!state) return;

      const knownToolIds = new Set(state.toolCalls.map((tool) => tool.id));
      const hasNewTool = tools.some(
        (tool) => tool.status === 'running' && !knownToolIds.has(tool.id),
      );
      const hasCurrentText = Boolean(this.currentText(state).trim());

      // A new tool after assistant text is a hard ordering barrier:
      // content -> old progress finalization -> new progress -> next content.
      if (hasNewTool && hasCurrentText && !state.segmentClosed) {
        this.clearContentTimer(state);
        await this.closeContentSegment(state);
        await this.finalizeProgress(state);
        this.resetProgress(state);
      }

      state.toolCalls = tools.map((tool) => ({ ...tool }));
      const rendered = this.renderer.progress(state.toolCalls);
      if (
        rendered.content === state.lastProgressContent
        && rendered.lines.join('\n') === state.progressLines.join('\n')
      ) {
        return;
      }

      state.progressLines = rendered.lines;
      state.lastProgressContent = rendered.content;

      const elapsed = this.now() - state.lastProgressUpdateAt;
      const needsCreate = !state.progressMessageId;
      if (!needsCreate && state.lastProgressUpdateAt > 0 && elapsed < this.progressThrottleMs) {
        this.scheduleProgressFlush(state, this.progressThrottleMs - elapsed);
        return;
      }

      this.clearProgressTimer(state);
      await this.flushProgress(state, false);
    });
  }

  /** Flush pending text before a permission card or another ordered message. */
  flush(chatId: string): Promise<void> {
    return this.enqueue(chatId, async () => {
      const state = this.states.get(chatId);
      if (!state) return;
      this.clearContentTimer(state);
      this.clearProgressTimer(state);
      await this.flushContent(state);
    });
  }

  end(chatId: string, status: FeishuStreamStatus, responseText: string): Promise<boolean> {
    return this.enqueue(chatId, async () => this.finalize(chatId, status, responseText));
  }

  cleanup(chatId: string): void {
    const state = this.states.get(chatId);
    if (state) {
      this.clearContentTimer(state);
      this.clearProgressTimer(state);
    }
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
      currentMessageId: state.currentMessageId,
      contentMessageIds: [...state.contentMessageIds],
      baseLength: state.baseLength,
      currentSegmentText: state.currentSegmentText,
      segmentClosed: state.segmentClosed,
      progressMessageId: state.progressMessageId,
      progressMessageIds: [...state.progressMessageIds],
      contentEditCount: state.contentEditCount,
      progressEditCount: state.progressEditCount,
    };
  }

  private enqueue<T>(chatId: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.writeTails.get(chatId) ?? Promise.resolve();
    const next = previous.then(() => operation(), () => operation());
    this.writeTails.set(chatId, next);
    next.finally(() => {
      if (this.writeTails.get(chatId) === next) this.writeTails.delete(chatId);
    }).catch(() => {});
    return next;
  }

  private async ensureState(chatId: string, replyToMessageId?: string): Promise<StreamState | null> {
    const existing = this.states.get(chatId);
    if (existing) return existing;

    try {
      const messageId = await this.transport.createPost(
        chatId,
        this.renderer.post('💭 Thinking...'),
        replyToMessageId,
      );
      if (!messageId) return null;

      const state: StreamState = {
        chatId,
        replyToMessageId,
        startTime: this.now(),
        pendingText: '',
        baseLength: 0,
        currentSegmentText: '',
        segmentClosed: false,
        thinking: true,
        currentMessageId: messageId,
        contentMessageIds: [messageId],
        contentEditCount: 0,
        lastContentUpdateAt: 0,
        lastSentContent: '',
        lastSentText: '',
        contentTimer: null,
        toolCalls: [],
        progressMessageId: null,
        progressMessageIds: [],
        progressLines: [],
        lastProgressContent: '',
        lastProgressUpdateAt: 0,
        progressEditCount: 0,
        progressTimer: null,
      };
      this.states.set(chatId, state);
      return state;
    } catch (error) {
      this.report('stream.create', error);
      return null;
    }
  }

  private async openContentSegment(state: StreamState): Promise<void> {
    if (!state.segmentClosed) return;
    try {
      const messageId = await this.transport.createPost(
        state.chatId,
        this.renderer.post('💭 Thinking...'),
        state.replyToMessageId,
      );
      if (!messageId) return;

      state.currentMessageId = messageId;
      state.contentMessageIds.push(messageId);
      state.segmentClosed = false;
      state.currentSegmentText = '';
      state.lastSentContent = '';
      state.lastSentText = '';
      state.contentEditCount = 0;
      state.lastContentUpdateAt = 0;
    } catch (error) {
      this.report('stream.segment.create', error);
    }
  }

  private currentText(state: StreamState): string {
    return state.pendingText.slice(state.baseLength);
  }

  private async flushContent(state: StreamState): Promise<void> {
    if (!state.currentMessageId || state.segmentClosed) return;

    const text = this.currentText(state);
    // Callers only invoke this after deciding whether throttling applies;
    // never turn an already-successful update into another identical edit.
    if (text === state.lastSentText) return;
    const display = text || '💭 Thinking...';
    const content = this.renderer.post(display);
    if (content === state.lastSentContent) return;

    if (state.contentEditCount >= this.maxEditsPerMessage) {
      await this.rolloverContent(state, text);
      return;
    }

    const messageId = state.currentMessageId;
    try {
      state.contentEditCount++;
      await this.transport.updatePost(messageId, content);
      state.lastSentContent = content;
      state.lastSentText = text;
      state.currentSegmentText = text;
      state.lastContentUpdateAt = this.now();
    } catch (error) {
      state.contentEditCount = Math.max(0, state.contentEditCount - 1);
      state.lastContentUpdateAt = 0;
      this.report('stream.content.update', error);

      if (this.isEditLimitError(error)) {
        await this.rolloverContent(state, text);
      }
    }
  }

  private async rolloverContent(state: StreamState, text: string): Promise<void> {
    const previousText = state.lastSentText;
    const tail = previousText && text.startsWith(previousText)
      ? text.slice(previousText.length)
      : text;

    try {
      const messageId = await this.transport.createPost(
        state.chatId,
        this.renderer.post(tail || '💭 Thinking...'),
        state.replyToMessageId,
      );
      if (!messageId) return;

      state.currentMessageId = messageId;
      state.contentMessageIds.push(messageId);
      state.baseLength = state.pendingText.length - tail.length;
      state.currentSegmentText = tail;
      state.lastSentText = tail;
      state.lastSentContent = this.renderer.post(tail || '💭 Thinking...');
      state.contentEditCount = 0;
      state.lastContentUpdateAt = this.now();
      state.segmentClosed = false;
    } catch (error) {
      this.report('stream.content.rollover', error);
    }
  }

  private async closeContentSegment(state: StreamState): Promise<void> {
    if (state.segmentClosed) return;
    await this.flushContent(state);
    state.segmentClosed = true;
    state.baseLength = state.pendingText.length;
    state.lastContentUpdateAt = 0;
  }

  private async flushProgress(state: StreamState, force: boolean): Promise<void> {
    if (state.progressLines.length === 0) return;
    const content = state.lastProgressContent;
    if (!content) return;

    if (!state.progressMessageId) {
      try {
        const messageId = await this.transport.createPost(state.chatId, content);
        if (messageId) {
          state.progressMessageId = messageId;
          state.progressMessageIds.push(messageId);
          state.lastProgressUpdateAt = this.now();
          state.progressEditCount = 0;
        }
      } catch (error) {
        this.report('stream.progress.create', error);
      }
      return;
    }

    if (!force && state.progressEditCount >= this.maxProgressEditsPerMessage) {
      state.progressMessageId = null;
      state.progressEditCount = 0;
      await this.flushProgress(state, true);
      return;
    }

    const messageId = state.progressMessageId;
    try {
      state.progressEditCount++;
      await this.transport.updatePost(messageId, content);
      state.lastProgressUpdateAt = this.now();
    } catch (error) {
      state.progressEditCount = Math.max(0, state.progressEditCount - 1);
      state.lastProgressUpdateAt = 0;
      this.report('stream.progress.update', error);
      if (this.isEditLimitError(error)) {
        state.progressMessageId = null;
        state.progressEditCount = 0;
        await this.flushProgress(state, true);
      }
    }
  }

  private async finalizeProgress(state: StreamState): Promise<void> {
    if (!state.progressMessageId || state.progressLines.length === 0) return;
    const finalized = this.renderer.finalizeProgress(state.lastProgressContent);
    if (!finalized || finalized === state.lastProgressContent) return;
    state.lastProgressContent = finalized;
    await this.flushProgress(state, true);
  }

  private resetProgress(state: StreamState): void {
    this.clearProgressTimer(state);
    state.progressMessageId = null;
    state.progressLines = [];
    state.lastProgressContent = '';
    state.lastProgressUpdateAt = 0;
    state.progressEditCount = 0;
  }

  private async finalize(
    chatId: string,
    status: FeishuStreamStatus,
    responseText: string,
  ): Promise<boolean> {
    const state = this.states.get(chatId);
    if (!state) return false;

    this.clearContentTimer(state);
    this.clearProgressTimer(state);

    if (responseText.length > state.pendingText.length) {
      state.pendingText = responseText;
    }
    if (!state.segmentClosed) {
      await this.flushContent(state);
    } else if (state.pendingText.length > state.baseLength) {
      await this.openContentSegment(state);
      await this.flushContent(state);
    }
    await this.finalizeProgress(state);

    const statusLabels: Record<FeishuStreamStatus, string> = {
      completed: '✅ Completed',
      interrupted: '⚠️ Interrupted',
      error: '❌ Error',
    };
    const elapsed = this.formatElapsed(this.now() - state.startTime);
    const finalStatus = statusLabels[status];
    const currentText = state.currentSegmentText || this.currentText(state);
    const finalContent = this.renderer.post(
      `${currentText || '💭 Thinking...'}\n\n${finalStatus} · ${elapsed}`,
    );

    // Complex answers get one clean final card. This also removes all
    // fragmented preview messages, so the final answer cannot be duplicated.
    if (this.renderer.hasComplexMarkdown(responseText)) {
      const delivered = await this.sendFinalReplacement(
        state,
        () => this.transport.createInteractive(
          chatId,
          this.renderer.finalCard(responseText, state.toolCalls, {
            status: finalStatus,
            elapsed,
          }),
        ),
        'stream.final.card',
      );
      if (delivered) return true;
    }

    if (state.currentMessageId && !state.segmentClosed) {
      try {
        if (state.contentEditCount >= this.maxEditsPerMessage) {
          throw new Error('stream final update reached edit budget');
        }
        state.contentEditCount++;
        await this.transport.updatePost(state.currentMessageId, finalContent);
        state.lastSentContent = finalContent;
        state.lastSentText = currentText;
        state.currentSegmentText = currentText;
        return true;
      } catch (error) {
        state.contentEditCount = Math.max(0, state.contentEditCount - 1);
        this.report('stream.final.update', error);
      }
    }

    // Final fallback is intentionally a post, even for complex Markdown. It
    // guarantees that bridge-manager does not send a second full answer.
    const fallback = await this.sendFinalReplacement(
      state,
      () => this.transport.createPost(chatId, this.renderer.post(
        `${responseText || currentText || '💭 Thinking...'}\n\n${finalStatus} · ${elapsed}`,
      )),
      'stream.final.fallback',
    );
    return fallback;
  }

  private async sendFinalReplacement(
    state: StreamState,
    send: () => Promise<string | null>,
    scope: string,
  ): Promise<boolean> {
    try {
      const messageId = await send();
      if (!messageId) return false;
      await Promise.allSettled(
        state.contentMessageIds
          .filter((id) => id !== messageId)
          .map((id) => this.transport.deleteMessage(id)),
      );
      return true;
    } catch (error) {
      this.report(scope, error);
      return false;
    }
  }

  private scheduleContentFlush(state: StreamState, delay: number): void {
    if (state.contentTimer) return;
    state.contentTimer = setTimeout(() => {
      state.contentTimer = null;
      void this.enqueue(state.chatId, async () => {
        const current = this.states.get(state.chatId);
        if (current !== state || state.segmentClosed) return;
        await this.flushContent(state);
      });
    }, Math.max(0, delay));
  }

  private scheduleProgressFlush(state: StreamState, delay: number): void {
    if (state.progressTimer) return;
    state.progressTimer = setTimeout(() => {
      state.progressTimer = null;
      void this.enqueue(state.chatId, async () => {
        const current = this.states.get(state.chatId);
        if (current !== state) return;
        await this.flushProgress(state, true);
      });
    }, Math.max(0, delay));
  }

  private clearContentTimer(state: StreamState): void {
    if (!state.contentTimer) return;
    clearTimeout(state.contentTimer);
    state.contentTimer = null;
  }

  private clearProgressTimer(state: StreamState): void {
    if (!state.progressTimer) return;
    clearTimeout(state.progressTimer);
    state.progressTimer = null;
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
