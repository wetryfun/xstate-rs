// System-level differential fixtures: runs fixtures/system/*.json through
// XState's pure system API and records the full system snapshot and the
// external effects after initialization and after every step, in
// <name>.expected.json. crates/xstate-rt/tests/system_differential.rs
// replays them on the Rust System. See system-runner.mjs for the format.
//
//   node generate-system.mjs [name]     (STACK=1 prints XState's stack for error records)
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runSystemFixture } from './system-runner.mjs';

const dir = join(dirname(fileURLToPath(import.meta.url)), '../../fixtures/system');

const only = process.argv[2];
for (const file of readdirSync(dir).sort()) {
  if (!file.endsWith('.json') || file.endsWith('.expected.json')) continue;
  const name = file.slice(0, -5);
  if (only && name !== only) continue;
  const fixture = JSON.parse(readFileSync(join(dir, file), 'utf8'));
  const records = runSystemFixture(fixture, { stack: !!process.env.STACK });
  writeFileSync(join(dir, `${name}.expected.json`), JSON.stringify(records, null, 2) + '\n');
  console.log(`${name}: ${records.length} records${records.some((r) => r.error) ? ' (with errors)' : ''}`);
}
