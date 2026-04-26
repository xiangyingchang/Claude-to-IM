/**
 * LLM Provider — wraps the Claude Code SDK (claude-agent-sdk) for the bridge.
 *
 * Creates SDK sessions and converts their events into the SSE format
 * expected by the conversation engine.
 */

import { unstable_v2_createSession, unstable_v2_resumeSession } from '@anthropic-ai/claude-agent-sdk';
import type { PermissionResult } from '@anthropic-ai/claude-agent-sdk';
import type {
  LLMProvider,
  StreamChatParams,
  PermissionResolution,
} from '../src/lib/bridge/host.js';

/** Per-session state for coordinating canUseTool with the bridge. */
interface PendingPermission {
  resolve: (result: PermissionResult) => void;
  reject: (err: Error) => void;
  toolUseId: string;
}

export class ClaudeLLMProvider implements LLMProvider {
  /** Map of permissionRequestId → pending permission resolvers. */
  private pendingPermissions = new Map<string, PendingPermission>();

  /**
   * Resolve a pending permission request (called by PermissionGateway).
   * Maps the bridge's PermissionResolution to the SDK's PermissionResult.
   */
  resolvePendingPermission(
    permissionRequestId: string,
    resolution: PermissionResolution,
  ): boolean {
    const pending = this.pendingPermissions.get(permissionRequestId);
    if (!pending) return false;

    if (resolution.behavior === 'allow') {
      pending.resolve({
        behavior: 'allow',
        toolUseID: pending.toolUseId,
        updatedPermissions: resolution.updatedPermissions as any,
      });
    } else {
      pending.resolve({
        behavior: 'deny',
        toolUseID: pending.toolUseId,
        message: resolution.message || 'Denied via IM bridge',
      });
    }
    this.pendingPermissions.delete(permissionRequestId);
    return true;
  }

  streamChat(params: StreamChatParams): ReadableStream<string> {
    const provider = this;

    return new ReadableStream<string>({
      async start(controller) {
        const abortController = params.abortController || new AbortController();

        // Determine model
        const model = params.model || 'claude-sonnet-4-20250514';

        // Resolve conversation history: replay previous messages
        const history = params.conversationHistory || [];

        // Path to the system claude binary (SDK defaults to bundled binary
        // which may fail to extract; system binary is more reliable)
        const claudePath = process.env.CLAUDE_CODE_PATH
          || process.env.CLAUDE_PATH
          || '/Users/haoshifasheng/.local/bin/claude';

        // Create or resume SDK session
        const sessionOptions = {
          model,
          pathToClaudeCodeExecutable: claudePath,
          permissionMode: mapPermissionMode(params.permissionMode),
          canUseTool: makeCanUseTool(provider, params, controller, abortController.signal),
          cwd: process.cwd(),
          settingSources: ['project', 'user'],
        };

        const session = params.sdkSessionId
          ? resumeSessionWithFallback(params.sdkSessionId, sessionOptions)
          : unstable_v2_createSession(sessionOptions);

        try {
          // If there's conversation history, replay each message as user input
          // to provide context for the LLM
          if (history.length > 0) {
            for (const msg of history) {
              await session.send(msg.content);
            }
          }

          // Send the current prompt
          await session.send(params.prompt);

          // Notify runtime status
          params.onRuntimeStatusChange?.('running');

          // session.sessionId is only available after the first stream message,
          // so we extract it from the stream events instead.
          let sessionIdEmitted = false;

          // Process the stream
          for await (const msg of session.stream()) {
            if (abortController.signal.aborted) break;

            switch (msg.type) {
              case 'assistant': {
                // Full assistant message with content blocks
                const content = msg.message.content;
                for (const block of content) {
                  if (block.type === 'text' && block.text) {
                    controller.enqueue(
                      `data: ${JSON.stringify({ type: 'text', data: block.text })}\n`,
                    );
                  } else if (block.type === 'tool_use') {
                    controller.enqueue(
                      `data: ${JSON.stringify({
                        type: 'tool_use',
                        data: JSON.stringify({ id: block.id, name: block.name, input: block.input }),
                      })}\n`,
                    );
                  }
                }
                break;
              }

              case 'stream_event': {
                const event = msg.event;
                if (event.type === 'content_block_delta' && event.delta?.type === 'text_delta') {
                  controller.enqueue(
                    `data: ${JSON.stringify({ type: 'text', data: event.delta.text })}\n`,
                  );
                } else if (event.type === 'content_block_start' && event.content_block?.type === 'tool_use') {
                  const block = event.content_block;
                  controller.enqueue(
                    `data: ${JSON.stringify({
                      type: 'tool_use',
                      data: JSON.stringify({ id: block.id, name: block.name, input: block.input }),
                    })}\n`,
                  );
                } else if (event.type === 'content_block_start' && event.content_block?.type === 'tool_result') {
                  const block = event.content_block;
                  const content = typeof block.content === 'string' ? block.content : JSON.stringify(block.content);
                  controller.enqueue(
                    `data: ${JSON.stringify({
                      type: 'tool_result',
                      data: JSON.stringify({
                        tool_use_id: block.tool_use_id,
                        content,
                        is_error: block.is_error || false,
                      }),
                    })}\n`,
                  );
                }
                break;
              }

              case 'result': {
                params.onRuntimeStatusChange?.('idle');
                controller.enqueue(
                  `data: ${JSON.stringify({
                    type: 'result',
                    data: JSON.stringify({
                      is_error: msg.is_error || false,
                      usage: msg.usage || null,
                      session_id: msg.session_id,
                      result: 'subtype' in msg ? (msg as any).result : undefined,
                    }),
                  })}\n`,
                );
                // Emit done event
                controller.enqueue(
                  `data: ${JSON.stringify({ type: 'done', data: '' })}\n`,
                );
                break;
              }

              case 'system': {
                // System messages (status, init, etc.) from the SDK
                // The first 'init' message carries the session_id
                if (!sessionIdEmitted || msg.subtype === 'status') {
                  if (msg.session_id) {
                    controller.enqueue(
                      `data: ${JSON.stringify({
                        type: 'status',
                        data: JSON.stringify({
                          session_id: msg.session_id,
                          status: msg.subtype === 'init' ? 'initialized' : (msg as any).status || 'running',
                        }),
                      })}\n`,
                    );
                    sessionIdEmitted = true;
                  }
                }
                if (msg.subtype === 'status' && !sessionIdEmitted) {
                  params.onRuntimeStatusChange?.(msg.status || 'running');
                }
                break;
              }
            }
          }
        } catch (err) {
          if (!abortController.signal.aborted) {
            const errMsg = err instanceof Error ? err.message : String(err);
            controller.enqueue(
              `data: ${JSON.stringify({ type: 'error', data: errMsg })}\n`,
            );
          }
        } finally {
          session.close();
          controller.close();
        }
      },

      cancel() {
        // The ReadableStream was cancelled (e.g., by abort), no cleanup needed
        // as the session will be closed when the start function's try/finally runs.
      },
    });
  }
}

/**
 * Try to resume an SDK session; fall back to creating a new one.
 */
function resumeSessionWithFallback(
  sdkSessionId: string,
  options: Parameters<typeof unstable_v2_createSession>[0],
) {
  try {
    return unstable_v2_resumeSession(sdkSessionId, options);
  } catch {
    return unstable_v2_createSession(options);
  }
}

/**
 * Map bridge permission mode to SDK permission mode.
 */
function mapPermissionMode(mode?: string): 'default' | 'acceptEdits' | 'plan' | 'dontAsk' {
  switch (mode) {
    case 'plan': return 'plan';
    case 'acceptEdits': return 'acceptEdits';
    case 'default': return 'default';
    default: return 'default';
  }
}

/**
 * Create a canUseTool callback for the SDK session.
 */
function makeCanUseTool(
  provider: ClaudeLLMProvider,
  params: StreamChatParams,
  controller: ReadableStreamDefaultController<string>,
  signal: AbortSignal,
) {
  return async (
    toolName: string,
    toolInput: Record<string, unknown>,
    options: {
      signal: AbortSignal;
      suggestions?: unknown[];
      toolUseID: string;
      blockedPath?: string;
      decisionReason?: string;
    },
  ): Promise<PermissionResult> => {
    const permId = options.toolUseID;

    // Emit permission_request SSE event
    controller.enqueue(
      `data: ${JSON.stringify({
        type: 'permission_request',
        data: JSON.stringify({
          permissionRequestId: permId,
          toolName,
          toolInput,
          suggestions: options.suggestions,
        }),
      })}\n`,
    );

    // Wait for the bridge to resolve this permission
    return new Promise<PermissionResult>((resolve, reject) => {
      provider['pendingPermissions'].set(permId, {
        resolve,
        reject,
        toolUseId: permId,
      });

      // Handle abort signal: if the stream is aborted, reject the permission
      if (signal.aborted) {
        provider['pendingPermissions'].delete(permId);
        reject(new DOMException('Aborted', 'AbortError'));
        return;
      }

      const onAbort = () => {
        provider['pendingPermissions'].delete(permId);
        reject(new DOMException('Aborted', 'AbortError'));
      };
      signal.addEventListener('abort', onAbort, { once: true });

      // Clean up listener if permission is resolved before abort
      const cleanup = provider['resolvePendingPermission'].bind(provider);
      const origResolve = provider['pendingPermissions'].get(permId)?.resolve;
      if (origResolve) {
        const wrappedResolve = (result: PermissionResult) => {
          signal.removeEventListener('abort', onAbort);
          origResolve(result);
        };
        provider['pendingPermissions'].set(permId, {
          resolve: wrappedResolve,
          reject: (err: Error) => {
            signal.removeEventListener('abort', onAbort);
            reject(err);
          },
          toolUseId: permId,
        });
      }
    });
  };
}
