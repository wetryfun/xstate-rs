//! A WebAssembly bridge that lets XState's own JavaScript runtime use the
//! `xstate-rt` core for the structural statechart algorithm.
//!
//! The ABI is plain integers, so no binding generator is needed:
//!
//! - JS registers a machine's structure with `xs_tree_new` (see
//!   [`decode_tree`] for the encoding) and gets a handle back.
//! - JS calls `xs_select`, `xs_select_eventless`, `xs_microstep`,
//!   `xs_initial_microstep` or `xs_run_to_completion` with that handle.
//! - The core calls back into JS through the `xs` import module, one import
//!   per [`Host`] hook. Arrays go to JS as pointers into wasm memory; JS
//!   returns arrays and strings by writing them into the exchange buffers
//!   (`xs_u32buf`, `xs_bytebuf`) before returning.
//! - Every import returns `-1` when the JS hook threw. The core then unwinds
//!   with an error and the export returns `-1`; the JS side rethrows the
//!   exception it stashed. Calls may re-enter (a guard can run another
//!   actor), which is safe because no state is kept between calls except
//!   the immutable trees, and buffers are read right after each import.

#![allow(clippy::missing_safety_doc)]

use xstate_rt::core::{
    self, DomainHint, Host, Kind, MicrostepResult, Nid, RegionOutput, Resolved, Resolver, TRef, Tid, Tree, TreeNode,
    TreeTransition,
};

const BUF_LEN: usize = 1 << 16;
static mut U32BUF: [u32; BUF_LEN] = [0; BUF_LEN];
static mut BYTEBUF: [u8; BUF_LEN] = [0; BUF_LEN];

fn u32buf() -> &'static [u32; BUF_LEN] {
    // Only read right after an import wrote it; never held across calls.
    unsafe { &*std::ptr::addr_of!(U32BUF) }
}

fn bytebuf() -> &'static [u8; BUF_LEN] {
    unsafe { &*std::ptr::addr_of!(BYTEBUF) }
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "xs")]
extern "C" {
    fn config() -> i32;
    fn history(h: u32) -> i32;
    fn static_initial(n: u32) -> i32;
    fn begin_selection() -> i32;
    fn eval_candidate(t: u32, node: u32, eventless: u32) -> i32;
    fn resolve(resolver: u32, tref: u32) -> i32;
    fn begin_microstep() -> i32;
    fn exit_state(n: u32) -> i32;
    fn transition_content(ptr: *const u32, len: u32) -> i32;
    fn begin_entry(active: *const u32, active_len: u32, entered: *const u32, entered_len: u32) -> i32;
    fn enter_state(n: u32, default_entry: u32, defaults: *const u32, defaults_len: u32) -> i32;
    fn done_compound(parent: u32, final_child: u32) -> i32;
    fn done_parallel(parallel: u32, regions: *const u32, len: u32) -> i32;
    fn machine_done(completion: u32) -> i32;
    fn end_entry() -> i32;
    fn is_done() -> i32;
    fn exit_on_done(ptr: *const u32, len: u32) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn apply_microstep(
        has_result: u32,
        filtered: *const u32,
        filtered_len: u32,
        nodes: *const u32,
        nodes_len: u32,
        structure_changed: u32,
        history: *const u32,
        history_len: u32,
        enabled: *const u32,
        enabled_len: u32,
    ) -> i32;
    fn is_active() -> i32;
    fn check_iteration(n: u32) -> i32;
    fn snapshot_token() -> i32;
    fn microstep_token() -> i32;
    fn drain_raised() -> i32;
    fn queue_is_empty() -> i32;
    fn dequeue() -> i32;
}

// Native builds (used by `cargo test` and clippy) have no JS to call.
#[cfg(not(target_arch = "wasm32"))]
mod native_stubs {
    macro_rules! stub {
        ($($name:ident($($arg:ident: $ty:ty),*);)*) => {
            $(pub unsafe extern "C" fn $name($(_: $ty),*) -> i32 { unreachable!("xstate-wasm imports only exist on wasm32") })*
        };
    }
    stub! {
        config(); history(h: u32); static_initial(n: u32); begin_selection();
        eval_candidate(t: u32, node: u32, eventless: u32); resolve(resolver: u32, tref: u32);
        begin_microstep(); exit_state(n: u32); transition_content(ptr: *const u32, len: u32);
        begin_entry(a: *const u32, b: u32, c: *const u32, d: u32);
        enter_state(n: u32, d: u32, p: *const u32, l: u32); done_compound(p: u32, f: u32);
        done_parallel(p: u32, r: *const u32, l: u32); machine_done(c: u32); end_entry(); is_done();
        exit_on_done(p: *const u32, l: u32);
        apply_microstep(a: u32, b: *const u32, c: u32, d: *const u32, e: u32, f: u32, g: *const u32, h: u32, i: *const u32, j: u32);
        is_active(); check_iteration(n: u32); snapshot_token(); microstep_token(); drain_raised();
        queue_is_empty(); dequeue();
    }
}
#[cfg(not(target_arch = "wasm32"))]
use native_stubs::*;

// ---- TRef encoding -------------------------------------------------------------

const TREF_INITIAL_MICROSTEP: u32 = u32::MAX;
const TREF_INITIAL: u32 = 0x8000_0000;
const TREF_HISTORY_DEFAULT: u32 = 0x4000_0000;
const TREF_MASK: u32 = 0x3fff_ffff;

pub fn encode_tref(t: TRef) -> u32 {
    match t {
        TRef::T(t) => t,
        TRef::Initial(n) => TREF_INITIAL | n,
        TRef::HistoryDefault(n) => TREF_HISTORY_DEFAULT | n,
        TRef::InitialMicrostep => TREF_INITIAL_MICROSTEP,
    }
}

pub fn decode_tref(x: u32) -> TRef {
    if x == TREF_INITIAL_MICROSTEP {
        TRef::InitialMicrostep
    } else if x & TREF_INITIAL != 0 {
        TRef::Initial(x & TREF_MASK)
    } else if x & TREF_HISTORY_DEFAULT != 0 {
        TRef::HistoryDefault(x & TREF_MASK)
    } else {
        TRef::T(x)
    }
}

// ---- the JS host ----------------------------------------------------------------

struct JsHost;

fn ck(r: i32) -> Result<i32, ()> {
    if r == -1 {
        Err(())
    } else {
        Ok(r)
    }
}

fn read_u32s(n: i32) -> Vec<u32> {
    u32buf()[..n as usize].to_vec()
}

impl Host for JsHost {
    type Error = ();

    fn config(&self) -> Vec<Nid> {
        read_u32s(unsafe { config() })
    }

    fn history(&self, h: Nid) -> Option<Vec<Nid>> {
        let n = unsafe { history(h) };
        (n >= 0).then(|| read_u32s(n))
    }

    fn static_initial(&self, node: Nid) -> Vec<Nid> {
        read_u32s(unsafe { static_initial(node) })
    }

    fn begin_selection(&mut self) -> Result<(), ()> {
        ck(unsafe { begin_selection() }).map(drop)
    }

    fn eval_candidate(&mut self, t: Tid, node: Nid, eventless: bool) -> Result<bool, ()> {
        Ok(ck(unsafe { eval_candidate(t, node, eventless as u32) })? == 1)
    }

    fn resolve(&mut self, resolver: Resolver, t: TRef) -> Result<Resolved, ()> {
        let r = match resolver {
            Resolver::EventlessSelection => 0,
            Resolver::Conflicts => 1,
            Resolver::Current => 2,
        };
        ck(unsafe { resolve(r, encode_tref(t)) })?;
        let buf = u32buf();
        let (has_targets, reenter, n) = (buf[0] == 1, buf[1] == 1, buf[2] as usize);
        Ok(Resolved { targets: has_targets.then(|| buf[3..3 + n].to_vec()), reenter })
    }

    fn begin_microstep(&mut self) -> Result<(), ()> {
        ck(unsafe { begin_microstep() }).map(drop)
    }

    fn exit_state(&mut self, node: Nid) -> Result<(), ()> {
        ck(unsafe { exit_state(node) }).map(drop)
    }

    fn transition_content(&mut self, filtered: &[TRef]) -> Result<(), ()> {
        let encoded: Vec<u32> = filtered.iter().map(|&t| encode_tref(t)).collect();
        ck(unsafe { transition_content(encoded.as_ptr(), encoded.len() as u32) }).map(drop)
    }

    fn begin_entry(&mut self, active: &[Nid], entered: &[(TRef, Vec<Nid>)]) -> Result<(), ()> {
        let mut encoded = Vec::new();
        for (t, nodes) in entered {
            encoded.push(encode_tref(*t));
            encoded.push(nodes.len() as u32);
            encoded.extend_from_slice(nodes);
        }
        ck(unsafe { begin_entry(active.as_ptr(), active.len() as u32, encoded.as_ptr(), encoded.len() as u32) })
            .map(drop)
    }

    fn enter_state(&mut self, node: Nid, default_entry: bool, history_defaults: &[Nid]) -> Result<(), ()> {
        ck(unsafe { enter_state(node, default_entry as u32, history_defaults.as_ptr(), history_defaults.len() as u32) })
            .map(drop)
    }

    fn done_compound(&mut self, parent: Nid, final_child: Nid) -> Result<(), ()> {
        ck(unsafe { done_compound(parent, final_child) }).map(drop)
    }

    fn done_parallel(&mut self, parallel: Nid, regions: &[RegionOutput]) -> Result<(), ()> {
        // [kind, region, child + 1] per region; kind 0 final, 1 parallel, 2 compound.
        let mut encoded = Vec::with_capacity(regions.len() * 3);
        for r in regions {
            match *r {
                RegionOutput::Final(n) => encoded.extend_from_slice(&[0, n, 0]),
                RegionOutput::Parallel(n) => encoded.extend_from_slice(&[1, n, 0]),
                RegionOutput::Compound(n, child) => encoded.extend_from_slice(&[2, n, child.map_or(0, |c| c + 1)]),
            }
        }
        ck(unsafe { done_parallel(parallel, encoded.as_ptr(), encoded.len() as u32) }).map(drop)
    }

    fn machine_done(&mut self, completion: Nid) -> Result<(), ()> {
        ck(unsafe { machine_done(completion) }).map(drop)
    }

    fn end_entry(&mut self) -> Result<(), ()> {
        ck(unsafe { end_entry() }).map(drop)
    }

    fn is_done(&self) -> bool {
        unsafe { is_done() == 1 }
    }

    fn exit_on_done(&mut self, nodes_desc: &[Nid]) -> Result<(), ()> {
        ck(unsafe { exit_on_done(nodes_desc.as_ptr(), nodes_desc.len() as u32) }).map(drop)
    }

    fn apply_microstep(&mut self, result: Option<MicrostepResult>, enabled: &[TRef]) -> Result<(), ()> {
        let enabled: Vec<u32> = enabled.iter().map(|&t| encode_tref(t)).collect();
        match result {
            None => ck(unsafe {
                apply_microstep(
                    0,
                    [].as_ptr(),
                    0,
                    [].as_ptr(),
                    0,
                    0,
                    [].as_ptr(),
                    0,
                    enabled.as_ptr(),
                    enabled.len() as u32,
                )
            })
            .map(drop),
            Some(r) => {
                let filtered: Vec<u32> = r.filtered.iter().map(|&t| encode_tref(t)).collect();
                let mut history = Vec::new();
                for (h, nodes) in &r.history_updates {
                    history.push(*h);
                    history.push(nodes.len() as u32);
                    history.extend_from_slice(nodes);
                }
                ck(unsafe {
                    apply_microstep(
                        1,
                        filtered.as_ptr(),
                        filtered.len() as u32,
                        r.nodes.as_ptr(),
                        r.nodes.len() as u32,
                        r.structure_changed as u32,
                        history.as_ptr(),
                        history.len() as u32,
                        enabled.as_ptr(),
                        enabled.len() as u32,
                    )
                })
                .map(drop)
            }
        }
    }

    fn is_active(&self) -> bool {
        unsafe { is_active() == 1 }
    }

    fn check_iteration(&mut self, iteration: usize) -> Result<(), ()> {
        ck(unsafe { check_iteration(iteration as u32) }).map(drop)
    }

    fn snapshot_token(&self) -> u64 {
        unsafe { snapshot_token() as u32 as u64 }
    }

    fn microstep_token(&self) -> u64 {
        unsafe { microstep_token() as u32 as u64 }
    }

    fn drain_raised(&mut self) -> Result<(), ()> {
        ck(unsafe { drain_raised() }).map(drop)
    }

    fn queue_is_empty(&self) -> bool {
        unsafe { queue_is_empty() == 1 }
    }

    fn dequeue(&mut self) -> Result<Option<String>, ()> {
        match unsafe { dequeue() } {
            -1 => Err(()),
            -2 => Ok(None),
            n => Ok(Some(String::from_utf8_lossy(&bytebuf()[..n as usize]).into_owned())),
        }
    }
}

// ---- tree registration -----------------------------------------------------------------

/// Decodes a machine structure.
///
/// `words`:
/// `[node_count, transition_count,`
/// per node: `parent + 1, kind, deep, n_children, children…, n_history, history…,
/// n_on, (descriptor string index, n_transitions, transitions…)…, n_always, always…,`
/// per transition: `source, domain_hint]`.
/// Kinds: 0 atomic, 1 compound, 2 parallel, 3 final, 4 history, 5 choice.
/// Domain hints: 0 none, 1 internal, 2 external.
/// `strings` is `[count, (offset, len)…]` over `bytes`.
pub fn decode_tree(words: &[u32], strings: &[u32], bytes: &[u8]) -> Option<Tree> {
    let mut it = words.iter().copied();
    let mut next = || it.next();
    let str_count = *strings.first()? as usize;
    let string = |i: u32| -> Option<String> {
        let i = i as usize;
        if i >= str_count {
            return None;
        }
        let (off, len) = (strings[1 + 2 * i] as usize, strings[2 + 2 * i] as usize);
        Some(String::from_utf8_lossy(bytes.get(off..off + len)?).into_owned())
    };
    let node_count = next()? as usize;
    let transition_count = next()? as usize;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let parent = next()?;
        let kind = match (next()?, next()?) {
            (0, _) => Kind::Atomic,
            (1, _) => Kind::Compound,
            (2, _) => Kind::Parallel,
            (3, _) => Kind::Final,
            (4, deep) => Kind::History { deep: deep == 1 },
            (5, _) => Kind::Choice,
            _ => return None,
        };
        let mut node = TreeNode::new(parent.checked_sub(1), kind);
        let n = next()?;
        node.children = (0..n).map(|_| next()).collect::<Option<_>>()?;
        let n = next()?;
        node.history_children = (0..n).map(|_| next()).collect::<Option<_>>()?;
        let n_on = next()?;
        for _ in 0..n_on {
            let descriptor = string(next()?)?;
            let n = next()?;
            let ts: Vec<Tid> = (0..n).map(|_| next()).collect::<Option<_>>()?;
            node.on.push((descriptor, ts));
        }
        let n = next()?;
        node.always = (0..n).map(|_| next()).collect::<Option<_>>()?;
        nodes.push(node);
    }
    let mut transitions = Vec::with_capacity(transition_count);
    for _ in 0..transition_count {
        let source = next()?;
        let domain = match next()? {
            1 => DomainHint::Internal,
            2 => DomainHint::External,
            _ => DomainHint::None,
        };
        transitions.push(TreeTransition { source, domain });
    }
    Some(Tree::new(nodes, transitions))
}

// ---- exports --------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn xs_u32buf() -> *mut u32 {
    std::ptr::addr_of_mut!(U32BUF) as *mut u32
}

#[no_mangle]
pub extern "C" fn xs_bytebuf() -> *mut u8 {
    std::ptr::addr_of_mut!(BYTEBUF) as *mut u8
}

#[no_mangle]
pub extern "C" fn xs_buf_len() -> u32 {
    BUF_LEN as u32
}

#[no_mangle]
pub extern "C" fn xs_alloc(len: u32) -> *mut u8 {
    let mut v = Vec::<u8>::with_capacity(len as usize);
    let p = v.as_mut_ptr();
    std::mem::forget(v);
    p
}

#[no_mangle]
pub unsafe extern "C" fn xs_free(ptr: *mut u8, len: u32) {
    drop(Vec::from_raw_parts(ptr, 0, len as usize));
}

/// Registers a machine structure; returns a handle, or 0 if it is malformed.
#[no_mangle]
pub unsafe extern "C" fn xs_tree_new(
    words: *const u32,
    words_len: u32,
    strings: *const u32,
    strings_len: u32,
    bytes: *const u8,
    bytes_len: u32,
) -> u32 {
    let words = std::slice::from_raw_parts(words, words_len as usize);
    let strings = std::slice::from_raw_parts(strings, strings_len as usize);
    let bytes = std::slice::from_raw_parts(bytes, bytes_len as usize);
    match decode_tree(words, strings, bytes) {
        Some(tree) => Box::into_raw(Box::new(tree)) as u32,
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn xs_tree_free(handle: u32) {
    drop(Box::from_raw(handle as *mut Tree));
}

unsafe fn tree<'a>(handle: u32) -> &'a Tree {
    &*(handle as *const Tree)
}

fn write_out(values: impl IntoIterator<Item = u32>) -> i32 {
    let buf = unsafe { &mut *std::ptr::addr_of_mut!(U32BUF) };
    let mut n = 0;
    for v in values {
        buf[n] = v;
        n += 1;
    }
    n as i32
}

/// `getTransitionData`: writes the selected transition ids to the u32
/// buffer and returns their count, or -1 if a hook threw. The event type is
/// read from the byte buffer.
#[no_mangle]
pub unsafe extern "C" fn xs_select(handle: u32, event_type_len: u32) -> i32 {
    let event_type = String::from_utf8_lossy(&bytebuf()[..event_type_len as usize]).into_owned();
    match core::select_transitions(tree(handle), &mut JsHost, &event_type) {
        Ok(ts) => write_out(ts),
        Err(()) => -1,
    }
}

/// `selectEventlessTransitions`: writes the selected transitions (encoded
/// TRefs) and returns their count, or -1.
#[no_mangle]
pub unsafe extern "C" fn xs_select_eventless(handle: u32) -> i32 {
    match core::select_eventless(tree(handle), &mut JsHost) {
        Ok(ts) => write_out(ts.into_iter().map(encode_tref)),
        Err(()) => -1,
    }
}

/// `microstep` over encoded TRefs at `ptr`. Returns 0, or -1.
#[no_mangle]
pub unsafe extern "C" fn xs_microstep(handle: u32, ptr: *const u32, len: u32) -> i32 {
    let transitions: Vec<TRef> =
        std::slice::from_raw_parts(ptr, len as usize).iter().map(|&x| decode_tref(x)).collect();
    match core::microstep(tree(handle), &mut JsHost, &transitions, None) {
        Ok(()) => 0,
        Err(()) => -1,
    }
}

/// `initialMicrostep`. Returns 0, or -1.
#[no_mangle]
pub unsafe extern "C" fn xs_initial_microstep(handle: u32) -> i32 {
    match core::initial_microstep(tree(handle), &mut JsHost) {
        Ok(()) => 0,
        Err(()) => -1,
    }
}

/// The initial microstep's synthetic transition: writes its targets and
/// returns their count.
#[no_mangle]
pub unsafe extern "C" fn xs_initial_targets(handle: u32) -> i32 {
    let plan = core::initial_plan(tree(handle), &JsHost);
    write_out(plan.targets)
}

/// The loop at the end of `macrostep`. Returns 0, or -1.
#[no_mangle]
pub unsafe extern "C" fn xs_run_to_completion(handle: u32) -> i32 {
    match core::run_to_completion(tree(handle), &mut JsHost) {
        Ok(()) => 0,
        Err(()) => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tref_round_trip() {
        for t in [TRef::T(5), TRef::Initial(3), TRef::HistoryDefault(7), TRef::InitialMicrostep] {
            assert_eq!(decode_tref(encode_tref(t)), t);
        }
    }

    #[test]
    fn decodes_a_tree() {
        // root (compound) with children a (atomic, on "go" -> t0) and b (final).
        let words = [3, 1, 0, 1, 0, 2, 1, 2, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 1, 0];
        let strings = [1, 0, 2];
        let tree = decode_tree(&words, &strings, b"go").unwrap();
        assert_eq!(tree.nodes.len(), 3);
        assert_eq!(tree.node(1).on, vec![("go".to_string(), vec![0])]);
        assert_eq!(tree.kind(2), Kind::Final);
        assert!(tree.is_descendant(2, Some(0)));
    }
}
