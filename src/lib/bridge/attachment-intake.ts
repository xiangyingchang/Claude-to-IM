// @ts-nocheck
// Synced from dist/lib/bridge/attachment-intake.js (runtime source of truth).
// Kept as .ts so a future tsc build does not drop the feature.
/**
 * Attachment intake — turn arbitrary IM attachments into something the model
 * can actually see.
 *
 * Historically the bridge only forwarded image/* attachments to the LLM
 * (llm-provider.buildPrompt). A .zip, .txt, .pdf or .mp4 was downloaded and
 * saved to disk but never mentioned in the prompt, so the model replied
 * "there is no image in this message". This module closes that gap:
 *
 *   - images (png/jpeg/gif/webp) -> kept as FileAttachment -> multi-modal blocks
 *   - .zip -> list entries, inline text files, extract inner images as
 *     FileAttachments, note videos/other binaries by name
 *   - text-ish files (.txt/.md/.csv/.json/.log/...) -> inline their content
 *   - everything else -> describe it (name/type/size) and say media cannot be viewed
 *
 * Zip entries are read via Python's zipfile (python3, stdlib) rather than the
 * `unzip` CLI. Reason: WeChat chat-record zips use GBK entry names; `unzip -Z1`
 * mangles them and `unzip -p <zip> <mangled-name>` then fails with
 * "filename not matched". Python's zipfile reads entries by ZipInfo object,
 * which is encoding-independent. Entries are never extracted to disk.
 *
 * Returns a `summary` string prepended to the user prompt, plus an augmented
 * `files` array (original images + images extracted from zips).
 */
import { execFileSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';

const MAX_ZIP_ENTRIES = 60;
const MAX_TOTAL_UNCOMPRESSED = 40 * 1024 * 1024; // 40 MB across all entries
const MAX_TEXT_FILE = 200 * 1024;                 // 200 KB per text file
const MAX_IMAGE_FILE = 8 * 1024 * 1024;          // 8 MB per image
const MAX_ZIP_IMAGES = 20;
const MAX_INLINE_TEXT_CHARS = 12000;

const IMAGE_MIMES = new Set(['image/png', 'image/jpeg', 'image/jpg', 'image/gif', 'image/webp']);
const TEXT_EXTS = new Set(['txt', 'md', 'markdown', 'csv', 'json', 'log', 'yaml', 'yml', 'xml', 'html', 'htm', 'ini', 'conf', 'py', 'js', 'ts', 'sh', 'sql']);

// Python helper: emit JSON array of {name, ext, size, data} (data = base64).
// Reading by ZipInfo avoids all filename-encoding pitfalls.
const PY_ZIP_READ = `
import sys, json, zipfile, base64
MAX_ENTRIES = ${MAX_ZIP_ENTRIES}
MAX_TOTAL   = ${MAX_TOTAL_UNCOMPRESSED}
MAX_ONE     = ${MAX_IMAGE_FILE}
out = []
total = 0
try:
    with zipfile.ZipFile(sys.argv[1]) as z:
        for info in z.infolist()[:MAX_ENTRIES]:
            name = info.filename
            ext = name.rsplit('.', 1)[-1].lower() if '.' in name else ''
            if info.is_dir():
                out.append({'name': name, 'ext': ext, 'size': 0, 'dir': True, 'data': ''})
                continue
            if total + info.file_size > MAX_TOTAL or info.file_size > MAX_TOTAL:
                out.append({'name': name, 'ext': ext, 'size': info.file_size, 'truncated': True, 'data': ''})
                break
            try:
                raw = z.read(info)
            except Exception as e:
                out.append({'name': name, 'ext': ext, 'size': info.file_size, 'error': str(e), 'data': ''})
                continue
            total += len(raw)
            out.append({'name': name, 'ext': ext, 'size': len(raw), 'data': base64.b64encode(raw[:MAX_ONE]).decode()})
except Exception as e:
    print(json.dumps([{'fatal': str(e)}]))
    sys.exit(0)
print(json.dumps(out, ensure_ascii=False))
`;

function extOf(name) {
    const m = String(name || '').toLowerCase().match(/\.([a-z0-9]+)$/);
    return m ? m[1] : '';
}
function mimeFor(name) {
    const ext = extOf(name);
    const map = {
        jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif', webp: 'image/webp',
        mp4: 'video/mp4', mov: 'video/quicktime', pdf: 'application/pdf', txt: 'text/plain',
        zip: 'application/zip', silk: 'audio/silk',
    };
    return map[ext] || 'application/octet-stream';
}
function isTextExt(e) { return TEXT_EXTS.has(e); }
function isImageName(name) { return IMAGE_MIMES.has(mimeFor(name)); }
function humanSize(n) {
    if (n < 1024) return `${n}B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)}KB`;
    return `${(n / 1024 / 1024).toFixed(1)}MB`;
}

function readZip(zipPath) {
    const out = execFileSync('python3', ['-c', PY_ZIP_READ, zipPath], {
        encoding: 'utf-8', maxBuffer: MAX_TOTAL_UNCOMPRESSED * 2, timeout: 60_000,
    });
    return JSON.parse(out.trim());
}

/**
 * @param {Array<{id:string,name:string,type:string,size:number,data:string}>} files
 * @param {string} workDir
 * @returns {{ files: typeof files, summary: string }}
 */
export function intakeAttachments(files, workDir) {
    const outFiles = [];
    const lines = [];
    let zipImageCount = 0;

    for (const f of files || []) {
        const name = f.name || 'file';
        const type = f.type || mimeFor(name);
        const size = Number(f.size) || 0;
        const buffer = Buffer.from(f.data || '', 'base64');

        // ── plain image: keep as-is for buildPrompt ──
        if (IMAGE_MIMES.has(type) || isImageName(name)) {
            outFiles.push(f);
            lines.push(`- 图片 ${name} (${humanSize(size)}) 已附在消息里，请直接看图。`);
            continue;
        }

        // ── zip: introspect entries via Python zipfile ──
        if (type === 'application/zip' || extOf(name) === 'zip') {
            const zipPath = f.filePath || writeTempZip(buffer, workDir, name);
            if (!zipPath) { lines.push(`- 压缩包 ${name} (${humanSize(size)}) 无法读取。`); continue; }
            let entries;
            try { entries = readZip(zipPath); }
            catch (err) {
                lines.push(`- 压缩包 ${name} (${humanSize(size)}) 无法解析：${err instanceof Error ? err.message : String(err)}`);
                continue;
            }
            if (entries.length === 1 && entries[0].fatal) {
                lines.push(`- 压缩包 ${name} (${humanSize(size)}) 无法解压：${entries[0].fatal}`);
                continue;
            }
            lines.push(`- 压缩包 ${name} (${humanSize(size)})，内含 ${entries.length} 个文件：`);
            const mediaNotes = [];
            for (const e of entries) {
                const eName = printable(e.name);
                const eExt = (e.ext || extOf(e.name)).toLowerCase();
                if (e.fatal) { mediaNotes.push(`  · 解析中断：${e.fatal}`); break; }
                if (e.dir) continue;
                if (e.truncated) { mediaNotes.push(`  · ${eName} …（内容总量超限，已截断）`); break; }
                if (e.error) { mediaNotes.push(`  · ${eName}（读取失败：${e.error}）`); continue; }
                let data = Buffer.alloc(0);
                try { data = Buffer.from(e.data || '', 'base64'); } catch { /* ignore */ }

                if (isImageName(e.name) && zipImageCount < MAX_ZIP_IMAGES && data.length <= MAX_IMAGE_FILE) {
                    zipImageCount++;
                    outFiles.push({
                        id: `zipimg_${zipImageCount}`, name: eName, type: mimeFor(e.name),
                        size: data.length, data: data.toString('base64'),
                    });
                    lines.push(`  · ${eName} [图片 ${humanSize(data.length)}] 已附在消息里，请直接看图。`);
                } else if (isTextExt(eExt) && data.length <= MAX_TEXT_FILE) {
                    const content = decodeText(data);
                    lines.push(`  · ${eName} [文本 ${humanSize(data.length)}]，全文如下：`);
                    lines.push('    ```'); lines.push(indent(truncate(content))); lines.push('    ```');
                } else {
                    mediaNotes.push(`  · ${eName} [${eExt || '文件'} ${humanSize(e.size || data.length)}]（二进制/媒体，无法直接查看内容）`);
                }
            }
            lines.push(...mediaNotes);
            continue;
        }

        // ── standalone text file: inline content ──
        if (isTextExt(extOf(name)) && buffer.length <= MAX_TEXT_FILE) {
            const content = decodeText(buffer);
            lines.push(`- 文件 ${name} (${humanSize(size)})，全文如下：`);
            lines.push('  ```'); lines.push(indent(truncate(content))); lines.push('  ```');
            continue;
        }

        // ── pdf / video / audio / unknown: describe, don't pretend we can view ──
        const kind = extOf(name) || type;
        lines.push(`- 文件 ${name} (${kind}, ${humanSize(size)}) 已收到并保存。这是${/video|audio|mp4|mov|silk/i.test(type + name) ? '音视频' : '二进制'}文件，模型无法直接查看其画面/内容；如需处理请说明，或改用可直接阅读的格式（文字、图片）重发。`);
    }

    const summary = lines.length
        ? `[用户上传的附件]\n${lines.join('\n')}\n[附件说明结束]\n\n`
        : '';
    return { files: outFiles, summary };
}

function writeTempZip(buffer, workDir, name) {
    try {
        const dir = workDir || os.tmpdir();
        const safe = path.basename(String(name)).replace(/[^a-zA-Z0-9._-]/g, '_');
        const p = path.join(dir, `.cti-zip-${Date.now()}-${safe}`);
        fs.writeFileSync(p, buffer);
        return p;
    } catch { return undefined; }
}
function printable(s) {
    // ZipInfo.filename for GBK entries is often mojibake; keep it as-is rather
    // than risk double-conversion. Content (inlined text / image bytes) is
    // correct regardless of the display name.
    return String(s || '').replace(/\r?\n/g, ' ');
}
function decodeText(buf) {
    const utf8 = buf.toString('utf8');
    if (!utf8.includes('\uFFFD')) return utf8;
    return buf.toString('latin1');
}
function indent(s) { return s.split('\n').map(l => `    ${l}`).join('\n'); }
function truncate(s) {
    if (s.length <= MAX_INLINE_TEXT_CHARS) return s;
    return `${s.slice(0, MAX_INLINE_TEXT_CHARS)}\n…（全文共 ${s.length} 字符，已截断前 ${MAX_INLINE_TEXT_CHARS} 字符）`;
}
