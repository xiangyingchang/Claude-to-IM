/**
 * Feishu (Lark) Adapter — implements BaseChannelAdapter for Feishu Bot API.
 *
 * Uses the official @larksuiteoapi/node-sdk WSClient for real-time event
 * subscription and REST Client for message sending / resource downloading.
 * Routes messages through an internal async queue (same pattern as Telegram).
 *
 * Rendering strategy (aligned with Openclaw):
 * - Code blocks / tables → interactive card (schema 2.0 markdown)
 * - Other text → post (msg_type: 'post') with md tag
 * - Permission prompts → interactive card with action buttons
 *
 * card.action.trigger events are handled via EventDispatcher (Openclaw pattern):
 * button clicks are converted to synthetic text messages and routed through
 * the normal /perm command processing pipeline.
 */

import crypto from 'crypto';
import * as lark from '@larksuiteoapi/node-sdk';
import type {
  ChannelType,
  InboundMessage,
  OutboundMessage,
  SendResult,
} from '../types.js';
import type { FileAttachment } from '../types.js';
import type { ToolCallInfo } from '../types.js';
import { BaseChannelAdapter, registerAdapterFactory } from '../channel-adapter.js';
import { getBridgeContext } from '../context.js';
import { lookupToolDisplay } from '../tool-display.js';
import {
  htmlToFeishuMarkdown,
  preprocessFeishuMarkdown,
  hasComplexMarkdown,
  buildCardContent,
  buildPostContent,
  buildFinalCardJson,
  buildPermissionButtonCard,
  buildButtonCard,
  formatElapsed,
} from '../markdown/feishu.js';

/** Max number of message_ids to keep for dedup. */
const DEDUP_MAX = 1000;

/** Max file download size (20 MB). */
const MAX_FILE_SIZE = 20 * 1024 * 1024;

/** Feishu emoji type for typing indicator (same as Openclaw). */
const TYPING_EMOJI = 'Typing';

/** State for an active streaming message (post-based progressive edit). */
interface FeishuCardState {
  messageId: string;
  sequence: number;
  startTime: number;
  toolCalls: ToolCallInfo[];
  thinking: boolean;
  pendingText: string | null;
  lastUpdateAt: number;
  throttleTimer: ReturnType<typeof setTimeout> | null;
  /** Progressive edit counter — Feishu caps edits per message at 20. */
  editCount: number;
  /** Length of text already shown in prior segment(s); new segment shows only the delta. */
  baseLength: number;
  /** Last content string sent, for dedup (skip no-op edits). */
  lastSentContent: string;
  /** Message ID of the standalone tool-progress bubble (Hermes-style). */
  toolProgressMsgId: string | null;
  /** Accumulated tool lines for the progress bubble. */
  progressLines: string[];
  /** Timestamp of last progress-bubble edit (throttle). */
  lastProgressEditAt: number;
  /** Independent throttle timer for the tool-progress bubble. */
  progressThrottleTimer: ReturnType<typeof setTimeout> | null;
  /** Edit count for the tool-progress bubble — Feishu caps edits per message at 20. */
  progressEditCount: number;
}

/** Streaming update throttle interval (ms). */
const CARD_THROTTLE_MS = 500;

/** Max progressive edits per message before we start a fresh message.
 *  Feishu's edit-message API allows at most 20 edits per message; we stay
 *  well under with room for the finalize edit. */
const MAX_EDITS_PER_MESSAGE = 12;

/** Max edits for the tool-progress bubble before it segments (same 20 cap). */
const MAX_PROGRESS_EDITS = 15;

/** Shape of the SDK's im.message.receive_v1 event data. */
type FeishuMessageEventData = {
  sender: {
    sender_id?: {
      open_id?: string;
      union_id?: string;
      user_id?: string;
    };
    sender_type: string;
    tenant_key?: string;
  };
  message: {
    message_id: string;
    chat_id: string;
    chat_type: string;
    message_type: string;
    content: string;
    create_time: string;
    mentions?: Array<{
      key: string;
      id: { open_id?: string; union_id?: string; user_id?: string };
      name: string;
    }>;
  };
};


/** MIME type guesses by message_type. */
const MIME_BY_TYPE: Record<string, string> = {
  image: 'image/png',
  file: 'application/octet-stream',
  audio: 'audio/ogg',
  video: 'video/mp4',
  media: 'application/octet-stream',
};

export class FeishuAdapter extends BaseChannelAdapter {
  readonly channelType: ChannelType = 'feishu';

  private running = false;
  private queue: InboundMessage[] = [];
  private waiters: Array<(msg: InboundMessage | null) => void> = [];
  private wsClient: lark.WSClient | null = null;
  private restClient: lark.Client | null = null;
  private seenMessageIds = new Map<string, boolean>();
  private botOpenId: string | null = null;
  /** All known bot IDs (open_id, user_id, union_id) for mention matching. */
  private botIds = new Set<string>();
  /** Track last incoming message ID per chat for typing indicator. */
  private lastIncomingMessageId = new Map<string, string>();
  /** Track active typing reaction IDs per chat for cleanup. */
  private typingReactions = new Map<string, string>();
  /** In-flight guard for standalone tool-progress bubble creation (avoids duplicate bubbles on parallel tool events). */
  private toolProgressCreatePromises = new Map<string, Promise<void>>();
  /** Active streaming card state per chatId. */
  private activeCards = new Map<string, FeishuCardState>();
  /** In-flight card creation promises per chatId — prevents duplicate creation. */
  private cardCreatePromises = new Map<string, Promise<boolean>>();

  // ── Lifecycle ───────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.running) return;

    const configError = this.validateConfig();
    if (configError) {
      console.warn('[feishu-adapter] Cannot start:', configError);
      return;
    }

    const appId = getBridgeContext().store.getSetting('bridge_feishu_app_id') || '';
    const appSecret = getBridgeContext().store.getSetting('bridge_feishu_app_secret') || '';
    const domainSetting = getBridgeContext().store.getSetting('bridge_feishu_domain') || 'feishu';
    const domain = domainSetting === 'lark'
      ? lark.Domain.Lark
      : lark.Domain.Feishu;

    // Create REST client
    this.restClient = new lark.Client({
      appId,
      appSecret,
      domain,
    });

    // Resolve bot identity for @mention detection
    await this.resolveBotIdentity(appId, appSecret, domain);

    this.running = true;

    // Create EventDispatcher and register event handlers.
    const dispatcher = new lark.EventDispatcher({}).register({
      'im.message.receive_v1': async (data) => {
        await this.handleIncomingEvent(data as FeishuMessageEventData);
      },
      'card.action.trigger': (async (data: unknown) => {
        return await this.handleCardAction(data);
      }) as any,
    });

    // Create and start WSClient
    this.wsClient = new lark.WSClient({
      appId,
      appSecret,
      domain,
    });

    // Monkey-patch WSClient.handleEventData to support card action events (type: "card").
    // The SDK's WSClient only processes type="event" messages. Card action callbacks
    // arrive as type="card" and would be silently dropped without this patch.
    const wsClientAny = this.wsClient as any;
    if (typeof wsClientAny.handleEventData === 'function') {
      const origHandleEventData = wsClientAny.handleEventData.bind(wsClientAny);
      wsClientAny.handleEventData = (data: any) => {
        const msgType = data.headers?.find?.((h: any) => h.key === 'type')?.value;
        if (msgType === 'card') {
          console.log('[feishu-adapter] handleEventData type: card (patched → event)');
          const patchedData = {
            ...data,
            headers: data.headers.map((h: any) =>
              h.key === 'type' ? { ...h, value: 'event' } : h,
            ),
          };
          return origHandleEventData(patchedData);
        }
        return origHandleEventData(data);
      };
    }

    this.wsClient.start({ eventDispatcher: dispatcher });

    console.log('[feishu-adapter] Started (botOpenId:', this.botOpenId || 'unknown', ')');
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    // Close WebSocket connection (SDK exposes close())
    if (this.wsClient) {
      try {
        this.wsClient.close({ force: true });
      } catch (err) {
        console.warn('[feishu-adapter] WSClient close error:', err instanceof Error ? err.message : err);
      }
      this.wsClient = null;
    }
    this.restClient = null;

    // Reject all waiting consumers
    for (const waiter of this.waiters) {
      waiter(null);
    }
    this.waiters = [];

    // Clean up active cards
    for (const [, state] of this.activeCards) {
      if (state.throttleTimer) clearTimeout(state.throttleTimer);
      if (state.progressThrottleTimer) clearTimeout(state.progressThrottleTimer);
    }
    this.activeCards.clear();
    this.cardCreatePromises.clear();

    // Clear state
    this.seenMessageIds.clear();
    this.lastIncomingMessageId.clear();
    this.typingReactions.clear();

    console.log('[feishu-adapter] Stopped');
  }

  isRunning(): boolean {
    return this.running;
  }

  // ── Queue ───────────────────────────────────────────────────

  consumeOne(): Promise<InboundMessage | null> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);

    if (!this.running) return Promise.resolve(null);

    return new Promise<InboundMessage | null>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  private enqueue(msg: InboundMessage): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(msg);
    } else {
      this.queue.push(msg);
    }
  }

  // ── Typing indicator (Openclaw-style reaction) ─────────────

  /**
   * Add a "Typing" emoji reaction to the user's message and create streaming card.
   * Called by bridge-manager via onMessageStart().
   */
  onMessageStart(chatId: string): void {
    const messageId = this.lastIncomingMessageId.get(chatId);

    // Create streaming card (fire-and-forget — fallback to traditional if fails)
    if (messageId) {
      this.createStreamingCard(chatId, messageId).catch(() => {});
    }

    // Typing indicator (same as before)
    if (!messageId || !this.restClient) return;
    this.restClient.im.messageReaction.create({
      path: { message_id: messageId },
      data: { reaction_type: { emoji_type: TYPING_EMOJI } },
    }).then((res) => {
      const reactionId = (res as any)?.data?.reaction_id;
      if (reactionId) {
        this.typingReactions.set(chatId, reactionId);
      }
    }).catch((err) => {
      const code = (err as { code?: number })?.code;
      if (code !== 99991400 && code !== 99991403) {
        console.warn('[feishu-adapter] Typing indicator failed:', err instanceof Error ? err.message : err);
      }
    });
  }

  /**
   * Remove the "Typing" emoji reaction and clean up card state.
   * Called by bridge-manager via onMessageEnd().
   */
  onMessageEnd(chatId: string): void {
    // Clean up any orphaned card state (normally cleaned by finalizeCard)
    this.cleanupCard(chatId);

    // Remove typing reaction (same as before)
    const reactionId = this.typingReactions.get(chatId);
    const messageId = this.lastIncomingMessageId.get(chatId);
    if (!reactionId || !messageId || !this.restClient) return;
    this.typingReactions.delete(chatId);
    this.restClient.im.messageReaction.delete({
      path: { message_id: messageId, reaction_id: reactionId },
    }).catch(() => { /* ignore */ });
  }

  // ── Card Action Handler ─────────────────────────────────────

  /**
   * Handle card.action.trigger events (button clicks on permission cards).
   * Converts button clicks to synthetic InboundMessage with callbackData.
   * Must return within 3 seconds (Feishu timeout), so uses a 2.5s race.
   */
  private async handleCardAction(data: unknown): Promise<unknown> {
    const FALLBACK_TOAST = { toast: { type: 'info' as const, content: '已收到' } };

    try {
      const event = data as any;
      const value = event?.action?.value ?? {};
      const callbackData = value.callback_data;
      if (!callbackData) return FALLBACK_TOAST;

      // Extract chat/user context
      const chatId = event?.context?.open_chat_id || value.chatId || '';
      const messageId = event?.context?.open_message_id || event?.open_message_id || '';
      const userId = event?.operator?.open_id || event?.open_id || '';

      if (!chatId) return FALLBACK_TOAST;

      const callbackMsg: import('../types.js').InboundMessage = {
        messageId: messageId || `card_action_${Date.now()}`,
        address: {
          channelType: 'feishu',
          chatId,
          userId,
        },
        text: '',
        timestamp: Date.now(),
        callbackData,
        callbackMessageId: messageId,
      };
      this.enqueue(callbackMsg);

      return { toast: { type: 'info' as const, content: '已收到，正在处理...' } };
    } catch (err) {
      console.error('[feishu-adapter] Card action handler error:', err instanceof Error ? err.message : err);
      return FALLBACK_TOAST;
    }
  }

  // ── Streaming (progressive post-message edit) ─────────────────

  /**
   * Create a new streaming message and send it as a post.
   * Returns true if created successfully.
   */
  private createStreamingCard(chatId: string, replyToMessageId?: string): Promise<boolean> {
    if (!this.restClient || this.activeCards.has(chatId)) return Promise.resolve(false);

    // In-flight guard: if creation is already in progress, return the existing promise
    const existing = this.cardCreatePromises.get(chatId);
    if (existing) return existing;

    const promise = this._doCreateStreamingCard(chatId, replyToMessageId);
    this.cardCreatePromises.set(chatId, promise);
    promise.finally(() => this.cardCreatePromises.delete(chatId));
    return promise;
  }

  private async _doCreateStreamingCard(chatId: string, replyToMessageId?: string): Promise<boolean> {
    if (!this.restClient) return false;

    try {
      const content = buildPostContent('💭 Thinking...');

      let msgResp;
      if (replyToMessageId) {
        msgResp = await this.restClient.im.message.reply({
          path: { message_id: replyToMessageId },
          data: { content, msg_type: 'post' },
        });
      } else {
        msgResp = await this.restClient.im.message.create({
          params: { receive_id_type: 'chat_id' },
          data: {
            receive_id: chatId,
            msg_type: 'post',
            content,
          },
        });
      }

      const messageId = msgResp?.data?.message_id;
      if (!messageId) {
        console.warn('[feishu-adapter] Streaming message send returned no message_id');
        return false;
      }

      // Store streaming state
      this.activeCards.set(chatId, {
        messageId,
        sequence: 0,
        startTime: Date.now(),
        toolCalls: [],
        thinking: true,
        pendingText: null,
        lastUpdateAt: 0,
        throttleTimer: null,
        editCount: 0,
        baseLength: 0,
        lastSentContent: '',
        toolProgressMsgId: null,
        progressLines: [],
        lastProgressEditAt: 0,
        progressThrottleTimer: null,
        progressEditCount: 0,
      });

      console.log(`[feishu-adapter] Streaming message created: msgId=${messageId}`);
      return true;
    } catch (err) {
      console.warn('[feishu-adapter] Failed to create streaming message:', err instanceof Error ? err.message : err);
      return false;
    }
  }

  /**
   * Update streaming card content with throttling.
   */
  private updateCardContent(chatId: string, text: string): void {
    const state = this.activeCards.get(chatId);
    if (!state || !this.restClient) return;

    // Clear thinking state once text arrives
    if (state.thinking && text.trim()) {
      state.thinking = false;
    }
    state.pendingText = text;

    const elapsed = Date.now() - state.lastUpdateAt;
    if (elapsed < CARD_THROTTLE_MS && state.lastUpdateAt > 0) {
      // Schedule trailing-edge flush
      if (!state.throttleTimer) {
        state.throttleTimer = setTimeout(() => {
          state.throttleTimer = null;
          this.flushCardUpdate(chatId);
        }, CARD_THROTTLE_MS - elapsed);
      }
      return;
    }

    // Clear pending timer and flush immediately
    if (state.throttleTimer) {
      clearTimeout(state.throttleTimer);
      state.throttleTimer = null;
    }
    this.flushCardUpdate(chatId);
  }

  /**
   * Flush pending streaming update to Feishu API via message update (PUT).
   */
  private flushCardUpdate(chatId: string): void {
    const state = this.activeCards.get(chatId);
    if (!state || !this.restClient) return;

    // Feishu caps edits per message at 20. When we approach the cap, finalize
    // the current message and start a fresh one for continued streaming.
    if (state.editCount >= MAX_EDITS_PER_MESSAGE) {
      const fullText = state.pendingText || '';
      // Finalize current message (quietly — no status footer, just current text)
      this.restClient.im.message.update({
        path: { message_id: state.messageId },
        data: { msg_type: 'post', content: buildPostContent(fullText || '💭 Thinking...') },
      }).catch((err: unknown) => {
        console.warn('[feishu-adapter] segment-finalize update failed:', err instanceof Error ? err.message : err);
      });
      this.activeCards.delete(chatId);
      // Start a fresh message; new segment shows only the delta.
      const replyTo = this.lastIncomingMessageId.get(chatId);
      // Carry the tool-progress bubble across segments so it is NOT orphaned
      // (code-review finding #2: fresh bubble per segment would accumulate).
      const carryProgressMsgId = state.toolProgressMsgId;
      const carryProgressLines = state.progressLines;
      this.createStreamingCard(chatId, replyTo).then((ok) => {
        if (!ok) return;
        const st = this.activeCards.get(chatId);
        if (st) {
          st.baseLength = fullText.length;
          // Reuse the same tool-progress bubble across segments.
          st.toolProgressMsgId = carryProgressMsgId;
          st.progressLines = carryProgressLines;
          st.lastProgressEditAt = state.lastProgressEditAt;
          st.progressEditCount = state.progressEditCount;
          if (st.progressLines.length > 0) {
            // Keep rendering the carried lines (status updates still land here).
            this.flushToolProgress(chatId);
          }
        }
        // Re-queue current text so the next delta continues into the new message.
        this.updateCardContent(chatId, fullText);
      }).catch(() => {});
      return;
    }

    // Show only the delta since the last segment (baseLength).
    // Tool progress lives in a separate bubble — text stream is pure text.
    const shown = (state.pendingText || '').slice(state.baseLength);
    const content = shown || '💭 Thinking...';
    const postContent = buildPostContent(content);

    // Dedup: skip if content is identical to the last edit (no-op).
    if (postContent === state.lastSentContent) return;

    state.sequence++;
    const messageId = state.messageId;

    // Count optimistically BEFORE sending: Feishu counts every successful PUT
    // against the 20-edit cap, so we must reserve budget synchronously —
    // otherwise concurrent flushes all read the same stale editCount and blow
    // past the cap (error 230072).
    state.editCount++;
    state.lastSentContent = postContent;
    state.lastUpdateAt = Date.now();

    // Fire-and-forget — streaming updates are non-critical
    this.restClient.im.message.update({
      path: { message_id: messageId },
      data: { msg_type: 'post', content: postContent },
    }).catch((err: unknown) => {
      // A failed edit still consumed budget server-side only if it reached the
      // API; be conservative and log, but do not roll back editCount (retrying
      // same content is usually pointless).
      console.warn('[feishu-adapter] message.update failed:', err instanceof Error ? err.message : err);
    });
  }

  /**
   * Update tool progress — Hermes-style: render tool calls as a standalone
   * progress bubble (emoji + verb + preview, bash commands as fenced code
   * blocks) separate from the text stream, mirroring Hermes'
   * send_progress_messages(). The bubble is created on the first tool event
   * and edited in place with throttling.
   */
  private updateToolProgress(chatId: string, tools: ToolCallInfo[]): void {
    const state = this.activeCards.get(chatId);
    if (!state || !this.restClient) return;
    state.toolCalls = tools;

    // Build compact progress lines (one per tool), Hermes-style:
    //   📖 Reading feishu-adapter.ts L218-247
    //   💻 Running git status        (bash → fenced code block)
    //   ✏️ Editing file.ts
    // Running tool gets 🔄 prefix; completed ✅; failed ❌.
    const lines: string[] = [];
    for (const tc of tools) {
      const icon = tc.status === 'running' ? '🔄' : tc.status === 'complete' ? '✅' : '❌';
      const name = tc.name || 'tool';
      const input = tc.input || '';
      const { emoji, verb } = lookupToolDisplay(name);
      const isTerminal = /bash|shell|terminal|codex_shell|^run$/i.test(name);
      if (isTerminal && input) {
        // Terminal command → header + fenced code block (Hermes style).
        const cmd = input.includes('\n') ? `${input.split('\n')[0].slice(0, 60)} ...` : input;
        lines.push(`${icon} ${emoji} ${verb}\n\`\`\`\n${cmd}\n\`\`\``);
      } else if (input) {
        lines.push(`${icon} ${emoji} ${verb} \`${input}\``);
      } else {
        lines.push(`${icon} ${emoji} ${verb}`);
      }
    }

    // Skip if nothing changed (dedup).
    const joined = lines.join('\n');
    if (joined === state.progressLines.join('\n')) return;

    state.progressLines = lines;

    // Throttle edits like Hermes (_PROGRESS_EDIT_INTERVAL = 1.5s).
    const now = Date.now();
    if (state.toolProgressMsgId && (now - state.lastProgressEditAt) < 1500) {
      // Schedule a trailing-edge flush.
      if (!state.progressThrottleTimer) {
        state.progressThrottleTimer = setTimeout(() => {
          state.progressThrottleTimer = null;
          this.flushToolProgress(chatId);
        }, 1500 - (now - state.lastProgressEditAt));
      }
      return;
    }

    this.flushToolProgress(chatId);
  }

  /**
   * Create or edit the standalone tool-progress bubble.
   * First call creates a new post message; later calls edit it in place.
   * In-flight guarded: parallel tool events before the first create resolves
   * must NOT spawn duplicate bubbles (race found in code review).
   */
  private flushToolProgress(chatId: string): void {
    const state = this.activeCards.get(chatId);
    if (!state || !this.restClient || state.progressLines.length === 0) return;

    const content = buildPostContent(state.progressLines.join('\n'));

    if (state.toolProgressMsgId) {
      // Feishu caps edits per message at 20 — segment the bubble when we
      // approach the cap: keep the old bubble as-is and start a fresh one
      // on the next flush (toolProgressMsgId reset → create path).
      if (state.progressEditCount >= MAX_PROGRESS_EDITS) {
        state.toolProgressMsgId = null;
        state.progressEditCount = 0;
        // Re-dispatch so the current lines land in a fresh bubble.
        this.flushToolProgress(chatId);
        return;
      }
      // Edit existing bubble.
      this.restClient.im.message.update({
        path: { message_id: state.toolProgressMsgId },
        data: { msg_type: 'post', content },
      }).then(() => {
        state.lastProgressEditAt = Date.now();
      }).catch((err: unknown) => {
        console.warn('[feishu-adapter] tool progress update failed:', err instanceof Error ? err.message : err);
      });
      state.progressEditCount++;
      return;
    }

    // In-flight guard: if a create is already pending for this chat, wait for
    // it (then the next flush will edit the now-existing bubble). This prevents
    // duplicate bubbles when several tool events arrive before the first
    // im.message.create resolves (very common with parallel tool calls).
    const existingCreate = this.toolProgressCreatePromises.get(chatId);
    if (existingCreate) {
      existingCreate.then(() => {
        // Re-run flush when creation resolved. toolProgressMsgId is set on
        // success (edit path); on segmentation reset it is null but lines are
        // buffered — retry the create path rather than dropping the update.
        const st = this.activeCards.get(chatId);
        if (st && (st.toolProgressMsgId || st.progressLines.length > 0)) {
          this.flushToolProgress(chatId);
        }
      }).catch(() => {});
      return;
    }

    // Create a new bubble (first tool event).
    const createPromise = this.restClient.im.message.create({
      params: { receive_id_type: 'chat_id' },
      data: { receive_id: chatId, msg_type: 'post', content },
    }).then((res) => {
      const mid = res?.data?.message_id;
      if (mid) {
        state.toolProgressMsgId = mid;
        state.lastProgressEditAt = Date.now();
      }
    }).catch((err: unknown) => {
      console.warn('[feishu-adapter] tool progress create failed:', err instanceof Error ? err.message : err);
    }).finally(() => {
      this.toolProgressCreatePromises.delete(chatId);
    });

    this.toolProgressCreatePromises.set(chatId, createPromise);
  }

  /**
   * Finalize the standalone tool-progress bubble when the response completes:
   * replace 🔄 running icons with ✅/❌ and push a final edit, so the bubble
   * never stays stale "running" (code-review finding #2).
   */
  private finalizeToolProgressBubble(chatId: string, state: FeishuCardState): void {
    if (!state.toolProgressMsgId || !this.restClient || state.progressLines.length === 0) return;

    // Replace any 🔄 (running) prefixes with ✅ (complete) — best-effort:
    // we don't track per-tool final status here, so any still-running tool is
    // marked done. Failed tools were already rendered with ❌.
    const finalized = state.progressLines.join('\n').replace(/🔄/g, '✅');
    if (finalized === state.progressLines.join('\n')) return;

    this.restClient.im.message.update({
      path: { message_id: state.toolProgressMsgId },
      data: { msg_type: 'post', content: buildPostContent(finalized) },
    }).catch((err: unknown) => {
      console.warn('[feishu-adapter] tool progress finalize failed:', err instanceof Error ? err.message : err);
    });
  }

  /**
   * Finalize the streaming message: edit with final content + footer.
   * For complex markdown (code blocks/tables) which message.update cannot
   * render, send an interactive card and retract the preview message.
   */
  private async finalizeCard(
    chatId: string,
    status: 'completed' | 'interrupted' | 'error',
    responseText: string,
  ): Promise<boolean> {
    // Wait for in-flight card creation to complete before finalizing
    const pending = this.cardCreatePromises.get(chatId);
    if (pending) {
      try { await pending; } catch { /* creation failed — no card to finalize */ }
    }

    const state = this.activeCards.get(chatId);
    if (!state || !this.restClient) return false;

    // Clear any pending throttle timer
    if (state.throttleTimer) {
      clearTimeout(state.throttleTimer);
      state.throttleTimer = null;
    }
    if (state.progressThrottleTimer) {
      clearTimeout(state.progressThrottleTimer);
      state.progressThrottleTimer = null;
    }

    try {
      const statusLabels: Record<string, string> = {
        completed: '✅ Completed',
        interrupted: '⚠️ Interrupted',
        error: '❌ Error',
      };
      const elapsedMs = Date.now() - state.startTime;
      const statusLabel = statusLabels[status] || status;
      const elapsedLabel = formatElapsed(elapsedMs);

      // Complex markdown (code blocks/tables) cannot be edited in place via
      // message.update (only text/post supported). Send as an interactive card
      // and retract the streaming preview so the user sees one clean result.
      if (hasComplexMarkdown(responseText)) {
        const finalCardJson = buildFinalCardJson(responseText, state.toolCalls, {
          status: statusLabel,
          elapsed: elapsedLabel,
        });
        try {
          const cardRes = await this.restClient.im.message.create({
            params: { receive_id_type: 'chat_id' },
            data: {
              receive_id: chatId,
              msg_type: 'interactive',
              content: finalCardJson,
            },
          });
          if (cardRes?.data?.message_id) {
            // Retract the streaming preview message (best effort).
            await this.restClient.im.message.delete({
              path: { message_id: state.messageId },
            }).catch(() => {});
            console.log(`[feishu-adapter] Final card sent (complex md), preview retracted: status=${status}, elapsed=${elapsedLabel}`);
            return true;
          }
          console.warn('[feishu-adapter] Final card send failed:', cardRes?.msg, cardRes?.code);
        } catch (err) {
          console.warn('[feishu-adapter] Final card send error:', err instanceof Error ? err.message : err);
        }
        // Fall through to post edit if card path failed.
      }

      // Simple text — edit the streaming post with final content + footer.
      const display = responseText || '💭 Thinking...';
      const finalContent = `${display}\n\n${statusLabel} · ${elapsedLabel}`;
      await this.restClient.im.message.update({
        path: { message_id: state.messageId },
        data: { msg_type: 'post', content: buildPostContent(finalContent) },
      });

      console.log(`[feishu-adapter] Message finalized: msgId=${state.messageId}, status=${status}, elapsed=${elapsedLabel}`);

      // Finalize the tool-progress bubble (code-review finding #2):
      // replace 🔄 running icons with ✅/❌ so the bubble doesn't stay stale.
      this.finalizeToolProgressBubble(chatId, state);

      return true;
    } catch (err) {
      console.warn('[feishu-adapter] Finalize failed:', err instanceof Error ? err.message : err);
      return false;
    } finally {
      this.activeCards.delete(chatId);
    }
  }

  /**
   * Clean up card state without finalizing (e.g. on unexpected errors).
   */
  private cleanupCard(chatId: string): void {
    this.cardCreatePromises.delete(chatId);
    const state = this.activeCards.get(chatId);
    if (!state) return;
    if (state.throttleTimer) {
      clearTimeout(state.throttleTimer);
    }
    if (state.progressThrottleTimer) {
      clearTimeout(state.progressThrottleTimer);
    }
    this.activeCards.delete(chatId);
  }

  /**
   * Check if there is an active streaming card for a given chat.
   */
  hasActiveCard(chatId: string): boolean {
    return this.activeCards.has(chatId);
  }

  // ── Streaming adapter interface ────────────────────────────────

  /**
   * Called by bridge-manager on each text SSE event.
   * Creates streaming card on first call, then updates content.
   */
  onStreamText(chatId: string, fullText: string): void {
    if (!this.activeCards.has(chatId)) {
      // Card should have been created by onMessageStart, but create lazily if not
      const messageId = this.lastIncomingMessageId.get(chatId);
      this.createStreamingCard(chatId, messageId).then((ok) => {
        if (ok) this.updateCardContent(chatId, fullText);
      }).catch(() => {});
      return;
    }
    this.updateCardContent(chatId, fullText);
  }

  onToolEvent(chatId: string, tools: ToolCallInfo[]): void {
    // Update the tool-progress bubble FIRST (state must still exist), then
    // check for a tool boundary (Hermes on_segment_break): when a NEW tool
    // starts running, finalize the current text message so subsequent text
    // appears as a fresh message below the tool progress bubble. This stops
    // the text message from growing unboundedly across many tool calls.
    const state = this.activeCards.get(chatId);
    const hasNewTool = state && state.pendingText
      ? tools.some((t) => t.status === 'running' && !new Set(state.toolCalls.map((t2) => t2.id)).has(t.id))
      : false;
    this.updateToolProgress(chatId, tools);
    if (hasNewTool) {
      this.segmentBreak(chatId);
    }
  }

  /**
   * Hermes-style segment break: finalize the current streaming text message
   * as-is (no status footer), then reset so the next onStreamText delta is
   * rendered into a brand-new message. Tool-progress bubbles and edit
   * accounting carry across the break.
   */
  private segmentBreak(chatId: string): void {
    const state = this.activeCards.get(chatId);
    if (!state || !this.restClient || !state.pendingText) return;

    const fullText = state.pendingText;
    // Quiet finalize: edit current message to its full text (no footer).
    this.restClient.im.message.update({
      path: { message_id: state.messageId },
      data: { msg_type: 'post', content: buildPostContent(fullText) },
    }).catch((err: unknown) => {
      console.warn('[feishu-adapter] segment-break finalize failed:', err instanceof Error ? err.message : err);
    });

    // Carry tool-progress bubble + edit accounting into the next message.
    const carryProgressMsgId = state.toolProgressMsgId;
    const carryProgressLines = state.progressLines;
    const carryLastProgressEditAt = state.lastProgressEditAt;
    const carryProgressEditCount = state.progressEditCount;
    const carryBaseLength = fullText.length;

    this.activeCards.delete(chatId);
    const replyTo = this.lastIncomingMessageId.get(chatId);
    this.createStreamingCard(chatId, replyTo).then((ok) => {
      if (!ok) return;
      const st = this.activeCards.get(chatId);
      if (st) {
        st.baseLength = carryBaseLength;
        st.toolProgressMsgId = carryProgressMsgId;
        st.progressLines = carryProgressLines;
        st.lastProgressEditAt = carryLastProgressEditAt;
        st.progressEditCount = carryProgressEditCount;
        if (st.progressLines.length > 0) {
          this.flushToolProgress(chatId);
        }
      }
    }).catch(() => {});
  }

  async onStreamEnd(chatId: string, status: 'completed' | 'interrupted' | 'error', responseText: string): Promise<boolean> {
    return this.finalizeCard(chatId, status, responseText);
  }

  // ── Send ────────────────────────────────────────────────────

  async send(message: OutboundMessage): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Feishu client not initialized' };
    }

    let text = message.text;

    // Convert HTML to markdown for Feishu rendering (e.g. command responses)
    if (message.parseMode === 'HTML') {
      text = htmlToFeishuMarkdown(text);
    }

    // Preprocess markdown for Claude responses
    if (message.parseMode === 'Markdown') {
      text = preprocessFeishuMarkdown(text);
    }

    // If there are inline buttons, send card with action buttons.
    // Route to the appropriate card builder based on button prefix.
    if (message.inlineButtons && message.inlineButtons.length > 0) {
      const firstBtn = message.inlineButtons.flat()[0];
      if (firstBtn?.callbackData?.startsWith('perm:')) {
        return this.sendPermissionCard(message.address.chatId, text, message.inlineButtons);
      }
      return this.sendButtonCard(message.address.chatId, text, message.inlineButtons);
    }

    // Rendering strategy (aligned with Openclaw):
    // - Code blocks / tables → interactive card (schema 2.0 markdown)
    // - Other text → post (md tag)
    if (hasComplexMarkdown(text)) {
      return this.sendAsCard(message.address.chatId, text);
    }
    return this.sendAsPost(message.address.chatId, text);
  }

  /**
   * Send text as an interactive card (schema 2.0 markdown).
   * Used for code blocks and tables — card renders them properly.
   */
  private async sendAsCard(chatId: string, text: string): Promise<SendResult> {
    const cardContent = buildCardContent(text);

    try {
      const res = await this.restClient!.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'interactive',
          content: cardContent,
        },
      });

      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Card send failed:', res?.msg, res?.code);
    } catch (err) {
      console.warn('[feishu-adapter] Card send error, falling back to post:', err instanceof Error ? err.message : err);
    }

    // Fallback to post
    return this.sendAsPost(chatId, text);
  }

  /**
   * Send text as a post message (msg_type: 'post') with md tag.
   * Used for simple text — renders bold, italic, inline code, links.
   */
  private async sendAsPost(chatId: string, text: string): Promise<SendResult> {
    const postContent = buildPostContent(text);

    try {
      const res = await this.restClient!.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'post',
          content: postContent,
        },
      });

      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Post send failed:', res?.msg, res?.code);
    } catch (err) {
      console.warn('[feishu-adapter] Post send error, falling back to text:', err instanceof Error ? err.message : err);
    }

    // Final fallback: plain text
    try {
      const res = await this.restClient!.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'text',
          content: JSON.stringify({ text }),
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      return { ok: false, error: res?.msg || 'Send failed' };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : 'Send failed' };
    }
  }

  // ── Permission card (with real action buttons) ─────────────

  /**
   * Send a permission card with real Feishu card action buttons.
   * Button clicks trigger card.action.trigger events handled by handleCardAction().
   * Falls back to text-based /perm commands if button card fails.
   */
  private async sendPermissionCard(
    chatId: string,
    text: string,
    inlineButtons: import('../types.js').InlineButton[][],
  ): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Feishu client not initialized' };
    }

    // Convert HTML text from permission-broker to Feishu markdown.
    const mdText = this.htmlToCardMarkdown(text);

    // Extract permissionRequestId from the first button's callback data
    const firstBtn = inlineButtons.flat()[0];
    const permId = firstBtn?.callbackData?.startsWith('perm:')
      ? firstBtn.callbackData.split(':').slice(2).join(':')
      : '';

    if (permId) {
      // Use real card action buttons
      const cardJson = buildPermissionButtonCard(mdText, permId, chatId);

      try {
        const res = await this.restClient.im.message.create({
          params: { receive_id_type: 'chat_id' },
          data: {
            receive_id: chatId,
            msg_type: 'interactive',
            content: cardJson,
          },
        });
        if (res?.data?.message_id) {
          return { ok: true, messageId: res.data.message_id };
        }
        console.warn('[feishu-adapter] Permission button card send failed:', JSON.stringify({ code: (res as any)?.code, msg: res?.msg }));
      } catch (err) {
        console.warn('[feishu-adapter] Permission button card error, falling back to text:', err instanceof Error ? err.message : err);
      }
    }

    // Fallback: text-based permission commands (same as before, for backward compat)
    const permCommands = inlineButtons.flat().map((btn) => {
      if (btn.callbackData.startsWith('perm:')) {
        const parts = btn.callbackData.split(':');
        const action = parts[1];
        const id = parts.slice(2).join(':');
        return `\`/perm ${action} ${id}\``;
      }
      return btn.text;
    });

    const cardContent = [
      mdText,
      '',
      '---',
      '**Reply:**',
      '`1` - Allow once',
      '`2` - Allow session',
      '`3` - Deny',
      '',
      'Or use full commands:',
      ...permCommands,
    ].join('\n');

    const cardJson = JSON.stringify({
      schema: '2.0',
      config: { wide_screen_mode: true },
      header: {
        template: 'orange',
        title: { tag: 'plain_text', content: '🔐 Permission Required' },
      },
      body: {
        elements: [
          { tag: 'markdown', content: cardContent },
        ],
      },
    });

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'interactive',
          content: cardJson,
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Fallback card also failed:', res?.msg);
    } catch (err) {
      console.warn('[feishu-adapter] Fallback card error, sending plain text:', err instanceof Error ? err.message : err);
    }

    // Last resort: plain text message (works even without card permissions)
    const plainText = [
      mdText,
      '',
      '---',
      'Reply: 1 = Allow once | 2 = Allow session | 3 = Deny',
      '',
      ...permCommands,
    ].join('\n');

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'text',
          content: JSON.stringify({ text: plainText }),
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      return { ok: false, error: res?.msg || 'Send failed' };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : 'Send failed' };
    }
  }

  /**
   * Send a generic interactive card with action buttons (e.g. model selection).
   * Converts inlineButton labels/callbackData pairs into Feishu card action buttons.
   */
  /** Convert HTML text to Feishu card markdown. Shared by both card builders. */
  private htmlToCardMarkdown(html: string): string {
    return html
      .replace(/<b>(.*?)<\/b>/gi, '**$1**')
      .replace(/<code>(.*?)<\/code>/gi, '`$1`')
      .replace(/<pre>([\s\S]*?)<\/pre>/gi, '```\n$1\n```')
      .replace(/<[^>]+>/g, '')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&amp;/g, '&')
      .replace(/&quot;/g, '"');
  }

  private async sendButtonCard(
    chatId: string,
    text: string,
    inlineButtons: import('../types.js').InlineButton[][],
  ): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Feishu client not initialized' };
    }

    // Convert HTML text to Feishu markdown
    const mdText = this.htmlToCardMarkdown(text);

    // Map inline buttons to card button rows
    const buttonRows = inlineButtons.map((row) =>
      row.map((btn) => ({ label: btn.text, callbackData: btn.callbackData })),
    );

    const cardJson = buildButtonCard('Select', mdText, buttonRows);

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'interactive',
          content: cardJson,
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Button card send failed:', JSON.stringify({ code: (res as any)?.code, msg: res?.msg }));
    } catch (err) {
      console.warn('[feishu-adapter] Button card error:', err instanceof Error ? err.message : err);
    }

    // Fallback: plain text with model names
    const fallbackText = [
      mdText,
      '',
      '---',
      ...inlineButtons.flat().map((btn) => `\`${btn.text}\``),
    ].join('\n');

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'text',
          content: JSON.stringify({ text: fallbackText }),
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      return { ok: false, error: res?.msg || 'Send failed' };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : 'Send failed' };
    }
  }

  // ── Config & Auth ───────────────────────────────────────────

  validateConfig(): string | null {
    const enabled = getBridgeContext().store.getSetting('bridge_feishu_enabled');
    if (enabled !== 'true') return 'bridge_feishu_enabled is not true';

    const appId = getBridgeContext().store.getSetting('bridge_feishu_app_id');
    if (!appId) return 'bridge_feishu_app_id not configured';

    const appSecret = getBridgeContext().store.getSetting('bridge_feishu_app_secret');
    if (!appSecret) return 'bridge_feishu_app_secret not configured';

    return null;
  }

  isAuthorized(userId: string, chatId: string): boolean {
    const allowedUsers = getBridgeContext().store.getSetting('bridge_feishu_allowed_users') || '';
    if (!allowedUsers) {
      // No restriction configured — allow all
      return true;
    }

    const allowed = allowedUsers
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    if (allowed.length === 0) return true;

    return allowed.includes(userId) || allowed.includes(chatId);
  }

  // ── Incoming event handler ──────────────────────────────────

  private async handleIncomingEvent(data: FeishuMessageEventData): Promise<void> {
    try {
      await this.processIncomingEvent(data);
    } catch (err) {
      console.error(
        '[feishu-adapter] Unhandled error in event handler:',
        err instanceof Error ? err.stack || err.message : err,
      );
    }
  }

  private async processIncomingEvent(data: FeishuMessageEventData): Promise<void> {
    const msg = data.message;
    const sender = data.sender;

    // [P1] Filter out bot messages to prevent self-triggering loops
    if (sender.sender_type === 'bot') return;

    // Dedup by message_id
    if (this.seenMessageIds.has(msg.message_id)) return;
    this.addToDedup(msg.message_id);

    const chatId = msg.chat_id;
    // [P2] Complete sender ID fallback chain: open_id > user_id > union_id
    const userId = sender.sender_id?.open_id
      || sender.sender_id?.user_id
      || sender.sender_id?.union_id
      || '';
    const isGroup = msg.chat_type === 'group';

    // Authorization check
    if (!this.isAuthorized(userId, chatId)) {
      console.warn('[feishu-adapter] Unauthorized message from userId:', userId, 'chatId:', chatId);
      return;
    }

    // Group chat policy
    if (isGroup) {
      const policy = getBridgeContext().store.getSetting('bridge_feishu_group_policy') || 'open';

      if (policy === 'disabled') {
        console.log('[feishu-adapter] Group message ignored (policy=disabled), chatId:', chatId);
        return;
      }

      if (policy === 'allowlist') {
        const allowedGroups = (getBridgeContext().store.getSetting('bridge_feishu_group_allow_from') || '')
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);
        if (!allowedGroups.includes(chatId)) {
          console.log('[feishu-adapter] Group message ignored (not in allowlist), chatId:', chatId);
          return;
        }
      }

      // Require @mention check
      const requireMention = getBridgeContext().store.getSetting('bridge_feishu_require_mention') !== 'false';
      if (requireMention && !this.isBotMentioned(msg.mentions)) {
        console.log('[feishu-adapter] Group message ignored (bot not @mentioned), chatId:', chatId, 'msgId:', msg.message_id);
        try {
          getBridgeContext().store.insertAuditLog({
            channelType: 'feishu',
            chatId,
            direction: 'inbound',
            messageId: msg.message_id,
            summary: '[FILTERED] Group message dropped: bot not @mentioned (require_mention=true)',
          });
        } catch { /* best effort */ }
        return;
      }
    }

    // Track last message ID per chat for typing indicator
    this.lastIncomingMessageId.set(chatId, msg.message_id);

    // Extract content based on message type
    const messageType = msg.message_type;
    let text = '';
    const attachments: FileAttachment[] = [];

    if (messageType === 'text') {
      text = this.parseTextContent(msg.content);
    } else if (messageType === 'image') {
      // [P1] Download image with failure fallback
      console.log('[feishu-adapter] Image message received, content:', msg.content);
      const fileKey = this.extractFileKey(msg.content);
      console.log('[feishu-adapter] Extracted fileKey:', fileKey);
      if (fileKey) {
        const attachment = await this.downloadResource(msg.message_id, fileKey, 'image');
        if (attachment) {
          attachments.push(attachment);
        } else {
          text = '[image download failed]';
          try {
            getBridgeContext().store.insertAuditLog({
              channelType: 'feishu',
              chatId,
              direction: 'inbound',
              messageId: msg.message_id,
              summary: `[ERROR] Image download failed for key: ${fileKey}`,
            });
          } catch { /* best effort */ }
        }
      }
    } else if (messageType === 'file' || messageType === 'audio' || messageType === 'video' || messageType === 'media') {
      // [P2] Support file/audio/video/media downloads
      const fileKey = this.extractFileKey(msg.content);
      if (fileKey) {
        const resourceType = messageType === 'audio' || messageType === 'video' || messageType === 'media'
          ? messageType
          : 'file';
        const attachment = await this.downloadResource(msg.message_id, fileKey, resourceType);
        if (attachment) {
          attachments.push(attachment);
        } else {
          text = `[${messageType} download failed]`;
          try {
            getBridgeContext().store.insertAuditLog({
              channelType: 'feishu',
              chatId,
              direction: 'inbound',
              messageId: msg.message_id,
              summary: `[ERROR] ${messageType} download failed for key: ${fileKey}`,
            });
          } catch { /* best effort */ }
        }
      }
    } else if (messageType === 'post') {
      // [P2] Extract text and image keys from rich text (post) messages
      const { extractedText, imageKeys } = this.parsePostContent(msg.content);
      text = extractedText;
      for (const key of imageKeys) {
        const attachment = await this.downloadResource(msg.message_id, key, 'image');
        if (attachment) {
          attachments.push(attachment);
        }
        // Don't add fallback text for individual post images — the text already carries context
      }
    } else {
      // Unsupported type — log and skip
      console.log(`[feishu-adapter] Unsupported message type: ${messageType}, msgId: ${msg.message_id}`);
      return;
    }

    // Strip @mention markers from text
    text = this.stripMentionMarkers(text);

    if (!text.trim() && attachments.length === 0) return;

    const timestamp = parseInt(msg.create_time, 10) || Date.now();
    const address = {
      channelType: 'feishu' as const,
      chatId,
      userId,
    };

    // [P1] Check for /perm text command (permission approval fallback)
    const trimmedText = text.trim();
    if (trimmedText.startsWith('/perm ')) {
      const permParts = trimmedText.split(/\s+/);
      // /perm <action> <permId>
      if (permParts.length >= 3) {
        const action = permParts[1]; // allow / allow_session / deny
        const permId = permParts.slice(2).join(' ');
        const callbackData = `perm:${action}:${permId}`;

        const inbound: InboundMessage = {
          messageId: msg.message_id,
          address,
          text: trimmedText,
          timestamp,
          callbackData,
        };
        this.enqueue(inbound);
        return;
      }
    }

    const inbound: InboundMessage = {
      messageId: msg.message_id,
      address,
      text: text.trim(),
      timestamp,
      attachments: attachments.length > 0 ? attachments : undefined,
    };

    // Audit log
    try {
      const summary = attachments.length > 0
        ? `[${attachments.length} attachment(s)] ${text.slice(0, 150)}`
        : text.slice(0, 200);
      getBridgeContext().store.insertAuditLog({
        channelType: 'feishu',
        chatId,
        direction: 'inbound',
        messageId: msg.message_id,
        summary,
      });
    } catch { /* best effort */ }

    this.enqueue(inbound);
  }

  // ── Content parsing ─────────────────────────────────────────

  private parseTextContent(content: string): string {
    try {
      const parsed = JSON.parse(content);
      return parsed.text || '';
    } catch {
      return content;
    }
  }

  /**
   * Extract file key from message content JSON.
   * Handles multiple key names: image_key, file_key, imageKey, fileKey.
   */
  private extractFileKey(content: string): string | null {
    try {
      const parsed = JSON.parse(content);
      return parsed.image_key || parsed.file_key || parsed.imageKey || parsed.fileKey || null;
    } catch {
      return null;
    }
  }

  /**
   * Parse rich text (post) content.
   * Extracts plain text from text elements and image keys from img elements.
   */
  private parsePostContent(content: string): { extractedText: string; imageKeys: string[] } {
    const imageKeys: string[] = [];
    const textParts: string[] = [];

    try {
      const parsed = JSON.parse(content);
      // Post content structure: { title, content: [[{tag, text/image_key}]] }
      const title = parsed.title;
      if (title) textParts.push(title);

      const paragraphs = parsed.content;
      if (Array.isArray(paragraphs)) {
        for (const paragraph of paragraphs) {
          if (!Array.isArray(paragraph)) continue;
          for (const element of paragraph) {
            if (element.tag === 'text' && element.text) {
              textParts.push(element.text);
            } else if (element.tag === 'a' && element.text) {
              textParts.push(element.text);
            } else if (element.tag === 'at' && element.user_id) {
              // Mention in post — handled by isBotMentioned for group policy
            } else if (element.tag === 'img') {
              const key = element.image_key || element.file_key || element.imageKey;
              if (key) imageKeys.push(key);
            }
          }
          textParts.push('\n');
        }
      }
    } catch {
      // Failed to parse post content
    }

    return { extractedText: textParts.join('').trim(), imageKeys };
  }

  // ── Bot identity ────────────────────────────────────────────

  /**
   * Resolve bot identity via the Feishu REST API /bot/v3/info/.
   * Collects all available bot IDs for comprehensive mention matching.
   */
  private async resolveBotIdentity(
    appId: string,
    appSecret: string,
    domain: lark.Domain,
  ): Promise<void> {
    try {
      const baseUrl = domain === lark.Domain.Lark
        ? 'https://open.larksuite.com'
        : 'https://open.feishu.cn';

      const tokenRes = await fetch(`${baseUrl}/open-apis/auth/v3/tenant_access_token/internal`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ app_id: appId, app_secret: appSecret }),
        signal: AbortSignal.timeout(10_000),
      });
      const tokenData: any = await tokenRes.json();
      if (!tokenData.tenant_access_token) {
        console.warn('[feishu-adapter] Failed to get tenant access token');
        return;
      }

      const botRes = await fetch(`${baseUrl}/open-apis/bot/v3/info/`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${tokenData.tenant_access_token}` },
        signal: AbortSignal.timeout(10_000),
      });
      const botData: any = await botRes.json();
      if (botData?.bot?.open_id) {
        this.botOpenId = botData.bot.open_id;
        this.botIds.add(botData.bot.open_id);
      }
      // Also record app_id-based IDs if available
      if (botData?.bot?.bot_id) {
        this.botIds.add(botData.bot.bot_id);
      }
      if (!this.botOpenId) {
        console.warn('[feishu-adapter] Could not resolve bot open_id');
      }
    } catch (err) {
      console.warn(
        '[feishu-adapter] Failed to resolve bot identity:',
        err instanceof Error ? err.message : err,
      );
    }
  }

  // ── @Mention detection ──────────────────────────────────────

  /**
   * [P2] Check if bot is mentioned — matches against open_id, user_id, union_id.
   */
  private isBotMentioned(
    mentions?: FeishuMessageEventData['message']['mentions'],
  ): boolean {
    if (!mentions || this.botIds.size === 0) return false;
    return mentions.some((m) => {
      const ids = [m.id.open_id, m.id.user_id, m.id.union_id].filter(Boolean) as string[];
      return ids.some((id) => this.botIds.has(id));
    });
  }

  private stripMentionMarkers(text: string): string {
    // Feishu uses @_user_N placeholders for mentions
    return text.replace(/@_user_\d+/g, '').trim();
  }

  // ── Resource download ───────────────────────────────────────

  /**
   * Download a message resource (image/file/audio/video) via SDK.
   * Returns null on failure (caller decides fallback behavior).
   */
  private async downloadResource(
    messageId: string,
    fileKey: string,
    resourceType: string,
  ): Promise<FileAttachment | null> {
    if (!this.restClient) return null;

    try {
      console.log(`[feishu-adapter] Downloading resource: type=${resourceType}, key=${fileKey}, msgId=${messageId}`);

      const res = await this.restClient.im.messageResource.get({
        path: {
          message_id: messageId,
          file_key: fileKey,
        },
        params: {
          type: resourceType === 'image' ? 'image' : 'file',
        },
      });

      if (!res) {
        console.warn('[feishu-adapter] messageResource.get returned null/undefined');
        return null;
      }

      // SDK returns { writeFile, getReadableStream, headers }
      // Try stream approach first, fall back to writeFile + read if stream fails
      let buffer: Buffer;

      try {
        const readable = res.getReadableStream();
        const chunks: Buffer[] = [];
        let totalSize = 0;

        for await (const chunk of readable) {
          const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          totalSize += buf.length;
          if (totalSize > MAX_FILE_SIZE) {
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE} bytes), key: ${fileKey}`);
            return null;
          }
          chunks.push(buf);
        }
        buffer = Buffer.concat(chunks);
      } catch (streamErr) {
        // Stream approach failed — fall back to writeFile + read
        console.warn('[feishu-adapter] Stream read failed, falling back to writeFile:', streamErr instanceof Error ? streamErr.message : streamErr);

        const fs = await import('fs');
        const os = await import('os');
        const path = await import('path');
        const tmpPath = path.join(os.tmpdir(), `feishu-dl-${crypto.randomUUID()}`);
        try {
          await res.writeFile(tmpPath);
          buffer = fs.readFileSync(tmpPath);
          if (buffer.length > MAX_FILE_SIZE) {
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE} bytes), key: ${fileKey}`);
            return null;
          }
        } finally {
          try { fs.unlinkSync(tmpPath); } catch { /* ignore cleanup errors */ }
        }
      }

      if (!buffer || buffer.length === 0) {
        console.warn('[feishu-adapter] Downloaded resource is empty, key:', fileKey);
        return null;
      }

      const base64 = buffer.toString('base64');
      const id = crypto.randomUUID();
      const mimeType = MIME_BY_TYPE[resourceType] || 'application/octet-stream';
      const ext = resourceType === 'image' ? 'png'
        : resourceType === 'audio' ? 'ogg'
        : resourceType === 'video' ? 'mp4'
        : 'bin';

      console.log(`[feishu-adapter] Resource downloaded: ${buffer.length} bytes, key=${fileKey}`);

      return {
        id,
        name: `${fileKey}.${ext}`,
        type: mimeType,
        size: buffer.length,
        data: base64,
      };
    } catch (err) {
      console.error(
        `[feishu-adapter] Resource download failed (type=${resourceType}, key=${fileKey}):`,
        err instanceof Error ? err.stack || err.message : err,
      );
      return null;
    }
  }

  // ── Utilities ───────────────────────────────────────────────

  private addToDedup(messageId: string): void {
    this.seenMessageIds.set(messageId, true);

    // LRU eviction: remove oldest entries when exceeding limit
    if (this.seenMessageIds.size > DEDUP_MAX) {
      const excess = this.seenMessageIds.size - DEDUP_MAX;
      let removed = 0;
      for (const key of this.seenMessageIds.keys()) {
        if (removed >= excess) break;
        this.seenMessageIds.delete(key);
        removed++;
      }
    }
  }
}

// Self-register so bridge-manager can create FeishuAdapter via the registry.
registerAdapterFactory('feishu', () => new FeishuAdapter());
