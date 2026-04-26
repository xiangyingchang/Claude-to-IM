/**
 * JSON file-based BridgeStore implementation.
 *
 * Persists all data to a single JSON file. Suitable for single-user
 * deployments and development. For production, replace with a proper
 * database-backed implementation.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import type {
  BridgeStore,
  BridgeSession,
  BridgeMessage,
  BridgeApiProvider,
  AuditLogInput,
  PermissionLinkInput,
  PermissionLinkRecord,
  OutboundRefInput,
  UpsertChannelBindingInput,
} from '../src/lib/bridge/host.js';
import type { ChannelBinding, ChannelType } from '../src/lib/bridge/types.js';

interface StoreData {
  settings: Record<string, string>;
  sessions: Record<string, BridgeSessionData>;
  messages: Record<string, BridgeMessage[]>;
  channelBindings: Record<string, ChannelBinding>;
  auditLogs: unknown[];
  dedupKeys: Record<string, number>;
  outboundRefs: unknown[];
  permissionLinks: Record<string, PermissionLinkRecord>;
  channelOffsets: Record<string, string>;
  providers: Record<string, BridgeApiProvider>;
  defaultProviderId: string | null;
  runtimeStatuses: Record<string, string>;
  /** Next IDs for auto-increment */
  nextBindingId: number;
  nextSessionId: number;
}

interface BridgeSessionData extends BridgeSession {
  system_prompt?: string;
  provider_id?: string;
  sdk_session_id?: string;
  model: string;
  todos?: unknown;
  locked?: boolean;
  lockId?: string;
  lockOwner?: string;
  lockExpiresAt?: number;
  autoIncrementId?: number;
}

export class JsonStore implements BridgeStore {
  private filePath: string;
  private data: StoreData;
  private saveTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(filePath: string) {
    this.filePath = filePath;
    this.data = this.load();
  }

  private load(): StoreData {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      if (fs.existsSync(this.filePath)) {
        const raw = fs.readFileSync(this.filePath, 'utf-8');
        return JSON.parse(raw);
      }
    } catch (err) {
      console.warn('[json-store] Failed to load store, starting fresh:', err instanceof Error ? err.message : err);
    }
    return {
      settings: {},
      sessions: {},
      messages: {},
      channelBindings: {},
      auditLogs: [],
      dedupKeys: {},
      outboundRefs: [],
      permissionLinks: {},
      channelOffsets: {},
      providers: {},
      defaultProviderId: null,
      runtimeStatuses: {},
      nextBindingId: 1,
      nextSessionId: 1,
    };
  }

  private save(): void {
    // Debounce saves
    if (this.saveTimer) clearTimeout(this.saveTimer);
    this.saveTimer = setTimeout(() => {
      try {
        const dir = path.dirname(this.filePath);
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }
        const tmp = this.filePath + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(this.data, null, 2), 'utf-8');
        fs.renameSync(tmp, this.filePath);
      } catch (err) {
        console.error('[json-store] Failed to save store:', err instanceof Error ? err.message : err);
      }
    }, 200);
  }

  // ── Settings ──

  getSetting(key: string): string | null {
    return this.data.settings[key] ?? null;
  }

  setSetting(key: string, value: string): void {
    this.data.settings[key] = value;
    this.save();
  }

  // ── Channel bindings ──

  getChannelBinding(channelType: string, chatId: string): ChannelBinding | null {
    const key = `${channelType}:${chatId}`;
    return this.data.channelBindings[key] ?? null;
  }

  upsertChannelBinding(data: UpsertChannelBindingInput): ChannelBinding {
    const key = `${data.channelType}:${data.chatId}`;
    const existing = this.data.channelBindings[key];
    const now = new Date().toISOString();
    if (existing) {
      existing.codepilotSessionId = data.codepilotSessionId;
      if (data.sdkSessionId !== undefined) existing.sdkSessionId = data.sdkSessionId;
      if (data.workingDirectory) existing.workingDirectory = data.workingDirectory;
      if (data.model) existing.model = data.model;
      if (data.mode) existing.mode = data.mode as ChannelBinding['mode'];
      existing.updatedAt = now;
      this.save();
      return existing;
    }
    const binding: ChannelBinding = {
      id: `binding-${this.data.nextBindingId++}`,
      channelType: data.channelType,
      chatId: data.chatId,
      codepilotSessionId: data.codepilotSessionId,
      sdkSessionId: data.sdkSessionId || '',
      workingDirectory: data.workingDirectory || process.env.HOME || '',
      model: data.model || '',
      mode: (data.mode as ChannelBinding['mode']) || 'code',
      active: true,
      createdAt: now,
      updatedAt: now,
    };
    this.data.channelBindings[key] = binding;
    this.save();
    return binding;
  }

  updateChannelBinding(id: string, updates: Partial<ChannelBinding>): void {
    const binding = Object.values(this.data.channelBindings).find(b => b.id === id);
    if (!binding) return;
    Object.assign(binding, updates, { updatedAt: new Date().toISOString() });
    this.save();
  }

  listChannelBindings(channelType?: ChannelType): ChannelBinding[] {
    const all = Object.values(this.data.channelBindings);
    if (channelType) return all.filter(b => b.channelType === channelType);
    return all;
  }

  // ── Sessions ──

  getSession(id: string): BridgeSession | null {
    const s = this.data.sessions[id];
    if (!s) return null;
    return {
      id: s.id,
      working_directory: s.working_directory,
      model: s.model,
      system_prompt: s.system_prompt,
      provider_id: s.provider_id,
    };
  }

  createSession(
    name: string,
    model: string,
    systemPrompt?: string,
    cwd?: string,
    mode?: string,
  ): BridgeSession {
    const id = `session-${this.data.nextSessionId++}-${Date.now()}`;
    const session: BridgeSessionData = {
      id,
      working_directory: cwd || process.env.HOME || '',
      model: model || 'claude-sonnet-4-20250514',
      system_prompt: systemPrompt,
      provider_id: this.data.defaultProviderId || undefined,
      sdk_session_id: '',
    };
    this.data.sessions[id] = session;
    this.save();
    return {
      id: session.id,
      working_directory: session.working_directory,
      model: session.model,
      system_prompt: session.system_prompt,
      provider_id: session.provider_id,
    };
  }

  updateSessionProviderId(sessionId: string, providerId: string): void {
    const s = this.data.sessions[sessionId];
    if (s) {
      s.provider_id = providerId;
      this.save();
    }
  }

  // ── Messages ──

  addMessage(sessionId: string, role: string, content: string, usage?: string | null): void {
    if (!this.data.messages[sessionId]) {
      this.data.messages[sessionId] = [];
    }
    this.data.messages[sessionId].push({
      role,
      content: usage ? `${content}\n<!--usage:${usage}-->` : content,
    });
    // Keep only last 100 messages
    if (this.data.messages[sessionId].length > 100) {
      this.data.messages[sessionId] = this.data.messages[sessionId].slice(-100);
    }
    this.save();
  }

  getMessages(sessionId: string, opts?: { limit?: number }): { messages: BridgeMessage[] } {
    const all = this.data.messages[sessionId] || [];
    const limit = opts?.limit || 50;
    const messages = all.slice(-limit).map(m => ({
      role: m.role,
      content: m.content,
    }));
    return { messages };
  }

  // ── Session locking ──

  acquireSessionLock(sessionId: string, lockId: string, owner: string, ttlSecs: number): boolean {
    const s = this.data.sessions[sessionId];
    if (!s) return false;

    if (s.locked && s.lockExpiresAt && s.lockExpiresAt > Date.now()) {
      return false; // Already locked by someone else
    }

    s.locked = true;
    s.lockId = lockId;
    s.lockOwner = owner;
    s.lockExpiresAt = Date.now() + ttlSecs * 1000;
    this.save();
    return true;
  }

  renewSessionLock(sessionId: string, lockId: string, ttlSecs: number): void {
    const s = this.data.sessions[sessionId];
    if (!s || !s.locked) return;
    if (s.lockId !== lockId) return; // Not our lock
    s.lockExpiresAt = Date.now() + ttlSecs * 1000;
    this.save();
  }

  releaseSessionLock(sessionId: string, lockId: string): void {
    const s = this.data.sessions[sessionId];
    if (!s || !s.locked) return;
    if (s.lockId !== lockId) return; // Not our lock
    s.locked = false;
    s.lockId = undefined;
    s.lockOwner = undefined;
    s.lockExpiresAt = undefined;
    this.save();
  }

  setSessionRuntimeStatus(sessionId: string, status: string): void {
    this.data.runtimeStatuses[sessionId] = status;
  }

  // ── SDK session ──

  updateSdkSessionId(sessionId: string, sdkSessionId: string): void {
    const s = this.data.sessions[sessionId];
    if (s) {
      s.sdk_session_id = sdkSessionId;
      this.save();
    }
  }

  updateSessionModel(sessionId: string, model: string): void {
    const s = this.data.sessions[sessionId];
    if (s) {
      s.model = model;
      this.save();
    }
  }

  syncSdkTasks(sessionId: string, todos: unknown): void {
    const s = this.data.sessions[sessionId];
    if (s) {
      s.todos = todos;
      this.save();
    }
  }

  // ── Provider ──

  getProvider(id: string): BridgeApiProvider | undefined {
    return this.data.providers[id];
  }

  getDefaultProviderId(): string | null {
    return this.data.defaultProviderId;
  }

  setDefaultProviderId(id: string): void {
    this.data.defaultProviderId = id;
    this.save();
  }

  addProvider(provider: BridgeApiProvider): void {
    this.data.providers[provider.id] = provider;
    this.save();
  }

  // ── Audit & dedup ──

  insertAuditLog(entry: AuditLogInput): void {
    this.data.auditLogs.push({
      ...entry,
      id: crypto.randomUUID(),
      createdAt: new Date().toISOString(),
    });
    // Keep last 1000 audit logs
    if (this.data.auditLogs.length > 1000) {
      this.data.auditLogs = this.data.auditLogs.slice(-1000);
    }
    this.save();
  }

  checkDedup(key: string): boolean {
    const ts = this.data.dedupKeys[key];
    if (!ts) return false;
    // Keys expire after 1 hour
    return (Date.now() - ts) < 3600_000;
  }

  insertDedup(key: string): void {
    this.data.dedupKeys[key] = Date.now();
    this.save();
  }

  cleanupExpiredDedup(): void {
    const now = Date.now();
    for (const [key, ts] of Object.entries(this.data.dedupKeys)) {
      if (now - ts > 3600_000) {
        delete this.data.dedupKeys[key];
      }
    }
    this.save();
  }

  insertOutboundRef(ref: OutboundRefInput): void {
    this.data.outboundRefs.push({
      ...ref,
      createdAt: new Date().toISOString(),
    });
    this.save();
  }

  // ── Permission links ──

  insertPermissionLink(link: PermissionLinkInput): void {
    this.data.permissionLinks[link.permissionRequestId] = {
      permissionRequestId: link.permissionRequestId,
      chatId: link.chatId,
      messageId: link.messageId,
      resolved: false,
      suggestions: link.suggestions,
    };
    this.save();
  }

  getPermissionLink(permissionRequestId: string): PermissionLinkRecord | null {
    return this.data.permissionLinks[permissionRequestId] ?? null;
  }

  markPermissionLinkResolved(permissionRequestId: string): boolean {
    const link = this.data.permissionLinks[permissionRequestId];
    if (!link || link.resolved) return false;
    link.resolved = true;
    this.save();
    return true;
  }

  listPendingPermissionLinksByChat(chatId: string): PermissionLinkRecord[] {
    return Object.values(this.data.permissionLinks).filter(
      l => l.chatId === chatId && !l.resolved,
    );
  }

  // ── Channel offsets ──

  getChannelOffset(key: string): string {
    return this.data.channelOffsets[key] || '0';
  }

  setChannelOffset(key: string, offset: string): void {
    this.data.channelOffsets[key] = offset;
    this.save();
  }
}
