//! Memory per idle actor: starts N actors in one partition (one event each,
//! then a flush, as the service would) and reports the heap bytes and
//! allocations still live per actor.
//!
//!   cargo run --release -p xstate-kafka --example actor_memory [-- 100000]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use xstate_kafka::{EngineConfig, Partition};
use xstate_rt::{Machine, Registry};

struct Counting;

static BYTES: AtomicIsize = AtomicIsize::new(0);
static BLOCKS: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        BLOCKS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        BLOCKS.fetch_sub(1, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        BYTES.fetch_add(new_size as isize - layout.size() as isize, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn machine(timer: bool) -> Arc<Machine<Value, Value>> {
    let mut open = json!({
        "on": {
            "add": { "context": { "items": 1 }, "actions": [{ "type": "@xstate.emit", "event": { "type": "added" } }] },
            "pay": { "target": "paying" }
        }
    });
    if timer {
        open["after"] = json!({ "86400000": { "target": "expired" } });
    }
    let definition = json!({
        "id": "order",
        "initial": "open",
        "context": { "items": 0, "customer": "c-1", "tags": ["web", "eu"] },
        "states": {
            "open": open,
            "paying": { "on": { "ok": { "target": "paid" }, "add": { "target": "open" } } },
            "paid": { "type": "final" },
            "expired": { "type": "final" }
        }
    });
    Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap())
}

fn measure(name: &str, timer: bool, n: usize) {
    let machine = machine(timer);
    let mut partition = Partition::new(machine, EngineConfig::default());
    let keys: Vec<String> = (0..n).map(|i| format!("order-{i:08}")).collect();
    let (bytes, blocks) = (BYTES.load(Ordering::Relaxed), BLOCKS.load(Ordering::Relaxed));
    for key in &keys {
        partition.handle(Some(key), br#"{"type":"add"}"#, 1);
    }
    drop(partition.flush());
    let bytes = BYTES.load(Ordering::Relaxed) - bytes;
    let blocks = BLOCKS.load(Ordering::Relaxed) - blocks;
    println!(
        "{name}: {:.0} bytes in {:.1} allocations per actor ({n} actors)",
        bytes as f64 / n as f64,
        blocks as f64 / n as f64
    );
    drop(partition);
}

fn main() {
    let n = std::env::args().nth(1).map_or(10_000, |a| a.parse().expect("a number of actors"));
    measure("idle actor", false, n);
    measure("idle actor with an `after` timer", true, n);
}
