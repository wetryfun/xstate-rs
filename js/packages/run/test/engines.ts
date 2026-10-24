/**
 * The engines the tests can use: upstream XState always; the native addon
 * and the WASI component when they are built (else those tests skip).
 */
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import type { Engine } from '@xmachines/run';
import { nativeEngine, type NativeAddon } from '@xmachines/run/native';
import { componentImports, wasiEngine, type ComponentActors } from '@xmachines/run/wasi';
import { xstateEngine } from '@xmachines/run/xstate';

const root = (path: string) => fileURLToPath(new URL(`../../../../${path}`, import.meta.url));

export const addonPath = process.env.XSTATE_NODE_ADDON ?? root('target/release/xstate_node.node');
export const componentPath = process.env.XSTATE_WASI_COMPONENT ?? root('target/wasi/wasm32-wasip2/release/xstate_wasi.wasm');

export const nativeSkip = existsSync(addonPath) ? false : `no native addon at ${addonPath} (crates/xstate-node/build.sh)`;
export const wasiSkip = existsSync(componentPath) ? false : `no WASI component at ${componentPath}`;

export function loadAddon(): NativeAddon {
  return createRequire(import.meta.url)(addonPath) as NativeAddon;
}

let actors: Promise<ComponentActors> | undefined;
/** Transpiles the component with jco (once) and instantiates it. */
export function loadComponent(): Promise<ComponentActors> {
  actors ??= (async () => {
    const dir = mkdtempSync(join(tmpdir(), 'xstate-jco-'));
    const jco = fileURLToPath(new URL('../../../node_modules/.bin/jco', import.meta.url));
    execFileSync(jco, ['transpile', componentPath, '-o', dir, '--instantiation', 'async', '--name', 'xstate', '--quiet']);
    const { instantiate } = await import(pathToFileURL(join(dir, 'xstate.js')).href);
    const shim = await import('@bytecodealliance/preview2-shim');
    const runtime = await instantiate(
      (path: string) => WebAssembly.compile(readFileSync(join(dir, path))),
      componentImports(shim as never),
    );
    return runtime.actors as ComponentActors;
  })();
  return actors;
}

/** Every available engine, by name. */
export async function engines(): Promise<Record<string, Engine>> {
  const out: Record<string, Engine> = { xstate: xstateEngine() };
  if (!nativeSkip) out.native = nativeEngine({ addon: loadAddon() });
  if (!wasiSkip) out.wasi = wasiEngine({ actors: await loadComponent() });
  return out;
}
