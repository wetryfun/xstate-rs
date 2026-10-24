/**
 * isomorphic-git uses a global `Buffer`, which browsers do not have: install
 * the `buffer` package's one when there is none. Imported first by
 * `@xmachines/git/browser`, so it runs before isomorphic-git.
 */
import { Buffer } from 'buffer';

const g = globalThis as { Buffer?: unknown };
if (g.Buffer === undefined) g.Buffer = Buffer;
