/**
 * Tool display formatting — Hermes-style emoji + verb + preview.
 *
 * Mirrors Hermes agent/display.py (_TOOL_VERBS + registry emojis + build_tool_preview)
 * so IM progress messages look identical to Hermes:
 *
 *   📖 Reading feishu-adapter.ts L218-247
 *   💻 Running git status
 *   ✏️ Editing file.ts
 *   🔍 Searching for pattern
 *
 * Shared by conversation-engine (preview extraction) and platform adapters
 * (bubble rendering). Kept dependency-free to avoid adapter↔engine cycles.
 */

/** Claude Code tool → Hermes-style emoji + verb display mapping. */
const TOOL_DISPLAY: Record<string, { emoji: string; verb: string }> = {
  Read: { emoji: '📖', verb: 'Reading' },
  Write: { emoji: '✏️', verb: 'Writing' },
  Edit: { emoji: '✏️', verb: 'Editing' },
  MultiEdit: { emoji: '✏️', verb: 'Editing' },
  Bash: { emoji: '💻', verb: 'Running' },
  Run: { emoji: '💻', verb: 'Running' },
  Grep: { emoji: '🔍', verb: 'Searching for' },
  Glob: { emoji: '📂', verb: 'Listing files' },
  WebSearch: { emoji: '🔍', verb: 'Searching the web for' },
  WebFetch: { emoji: '📖', verb: 'Reading' },
  Task: { emoji: '🤖', verb: 'Delegating' },
  TodoWrite: { emoji: '📋', verb: 'Updating tasks' },
  KillShell: { emoji: '🛑', verb: 'Stopping shell' },
  LS: { emoji: '📂', verb: 'Listing' },
  View: { emoji: '👁️', verb: 'Viewing' },
  ApplyPatch: { emoji: '✏️', verb: 'Editing' },
};

/** Lowercase fallback so case-insensitive tool names still match. */
export function lookupToolDisplay(name: string): { emoji: string; verb: string } {
  if (!name) return { emoji: '⚡', verb: 'Using' };
  const exact = TOOL_DISPLAY[name];
  if (exact) return exact;
  // Case-insensitive match (e.g. 'bash' vs 'Bash')
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(TOOL_DISPLAY)) {
    if (k.toLowerCase() === lower) return v;
  }
  // Codex-style names like codex_shell / write_file
  if (lower.includes('shell') || lower.includes('terminal') || lower === 'bash') {
    return TOOL_DISPLAY.Bash;
  }
  if (lower.includes('read') || lower.includes('view')) return TOOL_DISPLAY.Read;
  if (lower.includes('edit') || lower.includes('write') || lower.includes('patch')) return TOOL_DISPLAY.Edit;
  if (lower.includes('search') || lower.includes('grep')) return TOOL_DISPLAY.Grep;
  if (lower.includes('web')) return TOOL_DISPLAY.WebSearch;
  return { emoji: '⚡', verb: 'Using' };
}

/**
 * Build a Hermes-style preview for a tool call.
 * Returns e.g. "feishu-adapter.ts L218-247" (Read), "git status" (Bash),
 * "src/lib/types.ts" (Edit/Write), "pattern" (Grep), "query" (WebSearch).
 */
export function buildToolPreview(name: string, input: unknown): string {
  try {
    if (input == null) return '';
    const lower = name.toLowerCase();

    // Codex can emit the same tool twice: content_block_start carries `{}`
    // while the later assistant block carries the real arguments. It can also
    // serialize arguments as a JSON string. Normalize that shape before
    // looking for file paths or commands.
    let normalizedInput: unknown = input;
    if (typeof normalizedInput === 'string') {
      const raw = normalizedInput.trim();
      if ((raw.startsWith('{') && raw.endsWith('}')) || (raw.startsWith('[') && raw.endsWith(']'))) {
        try { normalizedInput = JSON.parse(raw); } catch { /* keep raw text */ }
      }
    }

    // ApplyPatch-style tools often expose only a unified patch string. Extract
    // the affected file names instead of showing an empty pencil line or the
    // entire patch body.
    if (isPatchTool(lower)) {
      const patchText = typeof normalizedInput === 'string'
        ? normalizedInput
        : getString(normalizedInput, ['patch', 'diff', 'unified_diff', 'changes']);
      const patchFiles = extractPatchFiles(patchText);
      if (patchFiles) return patchFiles;
    }

    if (typeof normalizedInput === 'string') {
      // Sanitize: strip backticks (would break Feishu inline-code fences)
      // and collapse newlines to keep the bubble single-line.
      return sanitizePreview(normalizedInput);
    }
    const obj = unwrapInput(normalizedInput);
    if (!obj) {
      return sanitizePreview(String(normalizedInput));
    }

    // Read: "file.ts L218-247" (basename + line range from offset/limit)
    if (lower === 'read' || lower === 'view') {
      const p = extractPath(obj);
      const base = p.split('/').pop() || p;
      const offset = typeof obj.offset === 'number' ? obj.offset : 0;
      const limit = typeof obj.limit === 'number' ? obj.limit : 0;
      let lineLabel = '';
      if (offset > 0) {
        lineLabel = limit > 1 ? ` L${offset}-${offset + limit - 1}` : ` L${offset}`;
      }
      return `${base}${lineLabel}`.slice(0, 120);
    }

    // Bash / shell: command first line (codex "Run" tool included)
    if (lower === 'bash' || lower === 'shell' || lower === 'terminal' || lower === 'run' || lower.includes('shell')) {
      const cmd = (obj.command ?? obj.cmd ?? obj.script ?? obj.bash ?? '') as string;
      const oneLine = String(cmd).replace(/\n/g, ' ').trim();
      return oneLine.slice(0, 120);
    }

    // Edit/Write: file path
    if (isEditTool(lower)) {
      const p = extractPath(obj);
      if (p) return p.split('/').pop() || p.slice(0, 120);
      const fileCount = Array.isArray(obj.files) ? obj.files.length : 0;
      if (fileCount > 0) return `${fileCount} files`;
      const content = getString(obj, ['content', 'new_string', 'replacement', 'text']);
      if (content) return `${content.length} chars`;
    }

    // Grep: pattern
    if (lower === 'grep') {
      const pat = obj.pattern ?? obj.query ?? '';
      if (pat) return String(pat).slice(0, 120);
    }

    // WebSearch: query
    if (lower === 'websearch' || lower === 'web_search') {
      const q = obj.query ?? '';
      if (q) return String(q).slice(0, 120);
    }

    // WebFetch: url
    if (lower === 'webfetch' || lower === 'web_fetch' || lower === 'web_extract') {
      const u = obj.url ?? '';
      if (u) return String(u).slice(0, 120);
    }

    // Glob: pattern
    if (lower === 'glob') {
      const pat = obj.pattern ?? obj.glob ?? '';
      if (pat) return String(pat).slice(0, 120);
    }

    // Task: goal
    if (lower === 'task' || lower === 'delegate_task') {
      const g = obj.goal ?? obj.description ?? obj.task ?? '';
      if (g) return String(g).slice(0, 120);
    }

    // Generic: pick the most informative scalar fields
    const picks = ['command', 'file_path', 'filePath', 'path', 'target_file', 'targetFile', 'url', 'query', 'pattern', 'prompt', 'code', 'text', 'goal', 'name'];
    for (const k of picks) {
      const v = obj[k];
      if (v != null && typeof v === 'string' && v.trim()) {
        return sanitizePreview(String(v));
      }
    }
    return '';
  } catch {
    return '';
  }
}

function sanitizePreview(value: string): string {
  return value.replace(/`/g, "'").replace(/\n/g, ' ').trim().slice(0, 120);
}

function unwrapInput(input: unknown): Record<string, unknown> | null {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const obj = input as Record<string, unknown>;
  for (const key of ['input', 'arguments', 'args']) {
    const nested = obj[key];
    if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
      return nested as Record<string, unknown>;
    }
  }
  return obj;
}

function getString(input: unknown, keys: string[]): string {
  const obj = unwrapInput(input);
  if (!obj) return '';
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === 'string' && value.trim()) return value;
  }
  return '';
}

function extractPath(obj: Record<string, unknown>): string {
  const keys = ['file_path', 'filePath', 'path', 'file', 'filename', 'target_file', 'targetFile'];
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === 'string' && value.trim()) return value.replace(/\\/g, '/');
  }
  if (Array.isArray(obj.files)) {
    const paths = obj.files
      .map((file) => unwrapInput(file))
      .map((file) => file ? extractPath(file) : '')
      .filter(Boolean);
    if (paths.length > 0) return paths.join(', ');
  }
  return '';
}

function isEditTool(lowerName: string): boolean {
  return lowerName === 'edit'
    || lowerName === 'write'
    || lowerName === 'file_write'
    || lowerName === 'multi_edit'
    || lowerName.includes('edit')
    || lowerName.includes('write')
    || isPatchTool(lowerName);
}

function isPatchTool(lowerName: string): boolean {
  return lowerName === 'applypatch'
    || lowerName === 'apply_patch'
    || lowerName.includes('apply_patch')
    || lowerName.includes('patch');
}

function extractPatchFiles(patch: string): string {
  if (!patch) return '';
  const files: string[] = [];
  const add = (value: string) => {
    const normalized = value.trim().replace(/^a\//, '').replace(/^b\//, '');
    if (normalized && normalized !== '/dev/null' && !files.includes(normalized)) files.push(normalized);
  };

  for (const match of patch.matchAll(/^\s*\*\*\*\s+(?:Update|Add|Delete) File:\s*(.+?)\s*$/gm)) {
    add(match[1]);
  }
  for (const match of patch.matchAll(/^\s*\+\+\+\s+(.+?)\s*$/gm)) {
    add(match[1].split('\t')[0]);
  }
  return files.map((file) => file.split('/').pop() || file).join(', ').slice(0, 120);
}

/**
 * Build a Hermes-style display line for a tool call:
 * "📖 Reading feishu-adapter.ts L218-247" / "💻 Running git status".
 */
export function buildToolDisplayLine(toolName: string, status: 'running' | 'complete' | 'error', toolInput: unknown): string {
  const { emoji, verb } = lookupToolDisplay(toolName);
  const icon = status === 'running' ? '🔄' : status === 'complete' ? '✅' : '❌';
  const preview = buildToolPreview(toolName, toolInput);
  const verbPhrase = preview ? `${verb} ${preview}` : verb;
  return `${icon} ${emoji} ${verbPhrase}`;
}
