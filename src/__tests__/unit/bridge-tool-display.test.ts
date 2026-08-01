import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildToolPreview } from '../../lib/bridge/tool-display.js';

describe('tool display previews', () => {
  it('shows the file for Edit and Write inputs', () => {
    assert.equal(
      buildToolPreview('Edit', { file_path: '/tmp/project/src/app.ts', old_string: 'a', new_string: 'b' }),
      'app.ts',
    );
    assert.equal(
      buildToolPreview('Write', JSON.stringify({ filePath: '/tmp/project/README.md', content: '# title' })),
      'README.md',
    );
  });

  it('extracts affected files from ApplyPatch payloads', () => {
    const patch = {
      patch: '*** Begin Patch\n*** Update File: src/app.ts\n@@\n-old\n+new\n*** Add File: src/new.ts\n+new file\n*** End Patch',
    };
    assert.equal(buildToolPreview('ApplyPatch', patch), 'app.ts, new.ts');
  });

  it('handles a raw patch string without exposing the whole diff', () => {
    assert.equal(
      buildToolPreview('apply_patch', '*** Begin Patch\n*** Delete File: lib/old.ts\n*** End Patch'),
      'old.ts',
    );
  });
});
