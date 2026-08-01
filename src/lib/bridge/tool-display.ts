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
    if (typeof input === 'string') {
      // Sanitize: strip backticks (would break Feishu inline-code fences)
      // and collapse newlines to keep the bubble single-line.
      return input.replace(/`/g, "'").replace(/\n/g, ' ').trim().slice(0, 120);
    }
    const obj = input as Record<string, unknown>;
    if (typeof obj !== 'object' || Array.isArray(obj)) {
      return String(input).replace(/`/g, "'").replace(/\n/g, ' ').trim().slice(0, 120);
    }
    const lower = name.toLowerCase();

    // Read: "file.ts L218-247" (basename + line range from offset/limit)
    if (lower === 'read' || lower === 'view') {
      const p = String(obj.path ?? obj.file_path ?? obj.filePath ?? obj.file ?? '').replace(/\\/g, '/');
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
    if (lower === 'edit' || lower === 'write' || lower === 'file_write' || lower === 'multi_edit' || lower.includes('edit')) {
      const p = String(obj.file_path ?? obj.filePath ?? obj.path ?? obj.file ?? '').replace(/\\/g, '/');
      if (p) return p.split('/').pop() || p.slice(0, 120);
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
    const picks = ['command', 'file_path', 'filePath', 'path', 'url', 'query', 'pattern', 'prompt', 'code', 'text', 'goal', 'name'];
    for (const k of picks) {
      const v = obj[k];
      if (v != null && typeof v === 'string' && v.trim()) {
        return String(v).replace(/\n/g, ' ').slice(0, 120);
      }
    }
    return '';
  } catch {
    return '';
  }
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
