/**
 * Claude-to-IM Bridge Host Application
 *
 * Wires together the bridge library with host implementations:
 * - JsonStore for persistence
 * - ClaudeLLMProvider wrapping the Claude Code SDK
 * - PermissionGateway bridging IM approvals
 *
 * Usage:
 *   1. Set environment variables (see .env.example)
 *   2. Run: npx tsx host/index.ts
 */

import 'dotenv/config';
import path from 'path';
import os from 'os';
import { initBridgeContext } from '../src/lib/bridge/context.js';
import * as bridgeManager from '../src/lib/bridge/bridge-manager.js';
// Side-effect: registers all adapter factories
import '../src/lib/bridge/adapters/index.js';
import '../src/lib/bridge/permission-broker.js';

import { JsonStore } from './json-store.js';
import { ClaudeLLMProvider } from './llm-provider.js';
import { createPermissionGateway } from './permissions.js';

// ── Configuration ──

interface BridgeConfig {
  /** File path for the JSON store */
  storePath: string;
  /** Default working directory for sessions */
  defaultWorkDir: string;
  /** Default model */
  defaultModel: string;
  /** Auto-start bridge on load */
  autoStart: boolean;
}

function loadConfig(): BridgeConfig {
  return {
    storePath: process.env.CLAUDE_TO_IM_STORE_PATH || path.join(os.homedir(), '.claude-to-im', 'store.json'),
    defaultWorkDir: process.env.CLAUDE_TO_IM_WORK_DIR || process.env.HOME || os.homedir(),
    defaultModel: process.env.CLAUDE_TO_IM_MODEL || 'claude-sonnet-4-20250514',
    autoStart: process.env.CLAUDE_TO_IM_AUTO_START !== 'false',
  };
}

function loadSettings(store: JsonStore): void {
  // Master switch
  const set = (key: string, envVar: string) => {
    const val = process.env[envVar];
    if (val) store.setSetting(key, val);
  };

  set('remote_bridge_enabled', 'CLAUDE_TO_IM_ENABLED');
  set('bridge_auto_start', 'CLAUDE_TO_IM_AUTO_START');

  // Telegram
  set('bridge_telegram_enabled', 'TELEGRAM_ENABLED');
  set('telegram_bot_token', 'TELEGRAM_BOT_TOKEN');
  set('telegram_bridge_allowed_users', 'TELEGRAM_ALLOWED_USERS');

  // Feishu
  set('bridge_feishu_enabled', 'FEISHU_ENABLED');
  set('bridge_feishu_app_id', 'FEISHU_APP_ID');
  set('bridge_feishu_app_secret', 'FEISHU_APP_SECRET');
  set('bridge_feishu_domain', 'FEISHU_DOMAIN');
  set('bridge_feishu_allowed_users', 'FEISHU_ALLOWED_USERS');

  // Defaults
  set('bridge_default_work_dir', 'CLAUDE_TO_IM_WORK_DIR');
  set('bridge_default_model', 'CLAUDE_TO_IM_MODEL');
}

// ── Main ──

async function main() {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   Claude-to-IM Bridge Host               ║');
  console.log('╚══════════════════════════════════════════╝');

  const config = loadConfig();

  // Switch to configured work directory so Claude Code SDK subprocesses
  // read the correct CLAUDE.md (e.g. workspace-sansan, not c2im)
  try {
    process.chdir(config.defaultWorkDir);
    console.log(`   Work dir: ${config.defaultWorkDir}`);
  } catch (err) {
    console.error(`   ⚠️  Cannot chdir to ${config.defaultWorkDir}:`, err instanceof Error ? err.message : err);
  }

  // Initialize store
  const store = new JsonStore(config.storePath);
  store.setSetting('bridge_default_work_dir', config.defaultWorkDir);
  store.setSetting('bridge_default_model', config.defaultModel);
  store.setSetting('remote_bridge_enabled', 'true');

  // Load settings from environment variables
  loadSettings(store);

  // Create LLM provider
  const llm = new ClaudeLLMProvider();

  // Create permission gateway
  const permissions = createPermissionGateway(llm);

  // Lifecycle hooks
  const lifecycle = {
    onBridgeStart() {
      console.log('[host] Bridge started');
    },
    onBridgeStop() {
      console.log('[host] Bridge stopped');
    },
  };

  // Initialize bridge context
  initBridgeContext({ store, llm, permissions, lifecycle });

  // Check configuration
  const bridgeEnabled = store.getSetting('remote_bridge_enabled');
  const telegramToken = store.getSetting('telegram_bot_token');
  const feishuAppId = store.getSetting('bridge_feishu_app_id');
  const feishuAppSecret = store.getSetting('bridge_feishu_app_secret');

  console.log('\n📋 Configuration:');
  console.log(`   Bridge enabled: ${bridgeEnabled}`);
  console.log(`   Store path: ${config.storePath}`);
  console.log(`   Default model: ${config.defaultModel}`);
  console.log(`   Telegram: ${telegramToken ? '✅ configured' : '⏳ not configured'}`);
  console.log(`   Feishu: ${feishuAppId && feishuAppSecret ? '✅ configured' : '⏳ not configured'}`);

  if (bridgeEnabled !== 'true') {
    console.log('\n❌ Bridge not enabled. Set CLAUDE_TO_IM_ENABLED=true');
    process.exit(1);
  }

  if (!telegramToken && (!feishuAppId || !feishuAppSecret)) {
    console.log('\n⚠️  No adapters configured. Configure at least Telegram or Feishu.');
    console.log('   See .env.example for configuration options.');
  }

  // Start the bridge
  console.log('\n🚀 Starting bridge...');
  try {
    await bridgeManager.start();
    const status = bridgeManager.getStatus();
    console.log(`\n✅ Bridge is running with ${status.adapters.length} adapter(s):`);
    for (const adapter of status.adapters) {
      console.log(`   - ${adapter.channelType}: ${adapter.running ? '✅ running' : '❌ stopped'}`);
    }
  } catch (err) {
    console.error('\n❌ Failed to start bridge:', err instanceof Error ? err.message : err);
    process.exit(1);
  }

  // Keep process alive
  console.log('\n📡 Listening for messages... (Ctrl+C to stop)\n');

  // Handle graceful shutdown
  const shutdown = async () => {
    console.log('\n\n🛑 Shutting down...');
    await bridgeManager.stop();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // Keep process alive indefinitely
  await new Promise(() => {});
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
