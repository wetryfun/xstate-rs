/**
 * `@xmachines/git/browser`: everything `@xmachines/git` exports, with the
 * global `Buffer` isomorphic-git needs installed first. Use it in browsers,
 * with an fs such as `@isomorphic-git/lightning-fs` (IndexedDB) and
 * isomorphic-git's `http/web` client for `push` and `pull`.
 */
import './buffer-global.ts';

export * from './index.ts';
