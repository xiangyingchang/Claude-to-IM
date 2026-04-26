/**
 * Patch the Claude Agent SDK to forward settingSources through
 * the session constructor (yQ class). Without this, CLAUDE.md
 * is never loaded in SDK session mode.
 */
import { readFileSync, writeFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const sdkPath = resolve(__dirname, '..', 'node_modules', '@anthropic-ai', 'claude-agent-sdk', 'sdk.mjs');

try {
  let content = readFileSync(sdkPath, 'utf-8');
  const oldPattern = 'resume:Q.resume,settingSources:[],allowedTools:Q.allowedTools';
  const newPattern = 'resume:Q.resume,settingSources:Q.settingSources??[],allowedTools:Q.allowedTools';

  if (content.includes(newPattern)) {
    console.log('[patch-sdk] Already patched, skipping.');
    process.exit(0);
  }

  if (!content.includes(oldPattern)) {
    console.error('[patch-sdk] ERROR: Pattern not found in SDK. Version mismatch?');
    process.exit(1);
  }

  content = content.replace(oldPattern, newPattern);
  writeFileSync(sdkPath, content, 'utf-8');
  console.log('[patch-sdk] SDK patched successfully.');
} catch (err) {
  console.error('[patch-sdk] Failed:', err.message);
  process.exit(1);
}
