/**
 * Permission Gateway — bridges IM permission approvals back to the LLM provider.
 *
 * Delegates resolution to the ClaudeLLMProvider since it owns the
 * canUseTool callback coordination.
 */

import type { PermissionGateway, PermissionResolution } from '../src/lib/bridge/host.js';
import type { ClaudeLLMProvider } from './llm-provider.js';

export function createPermissionGateway(llmProvider: ClaudeLLMProvider): PermissionGateway {
  return {
    resolvePendingPermission(
      permissionRequestId: string,
      resolution: PermissionResolution,
    ): boolean {
      // Delegate to the LLM provider which owns the canUseTool coordination
      return llmProvider.resolvePendingPermission(permissionRequestId, resolution);
    },
  };
}
