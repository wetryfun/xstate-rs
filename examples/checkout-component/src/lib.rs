//! The checkout machine (`examples/checkout`) as a typed WASI component.
//! Its guards, actions, delay and invoked actor are implemented here, in
//! Rust compiled into the component, against the trait `xstate-codegen`
//! generates; the host only drives actors and completes the card charge.
//!
//!   cargo build -p checkout-component --target wasm32-wasip2 --release
//!
//! `cargo test -p xstate-wasi-host` builds it and runs it in Wasmtime.

use std::sync::Arc;

use xstate_rt::step::async_logic;
use xstate_rt::{Ev, ExprArgs, GuardArgs, Logic, Machine};

pub mod checkout {
    xstate_rt::include_machine!("checkout");
}

use checkout::{CheckoutImpl, Context, ContextLinesItem, Event, NotifyParams, WithinLimitParams};
use xstate_rt::__codegen::serde_json;

/// The shop's side of the checkout. Actions write a line to stderr (the
/// host's to capture), the way a component without other imports reports.
pub struct Shop;

impl CheckoutImpl for Shop {
    fn guard_within_limit(&self, args: &GuardArgs<'_, Context, Event>, params: &WithinLimitParams) -> bool {
        matches!(args.event, Ev::User(Event::Checkout { total }) if *total <= params.max)
    }

    fn guard_retryable(&self, args: &GuardArgs<'_, Context, Event>) -> bool {
        args.event.field("error").and_then(|e| e.get("retryable")?.as_bool()).unwrap_or(false)
    }

    fn action_notify(&self, params: &NotifyParams) {
        eprintln!("notify {:?} {}", params.channel, params.template);
    }

    fn action_audit(&self) {
        eprintln!("audit");
    }

    fn expr_add_line(&self, args: &ExprArgs<'_, Context, Event>) -> serde_json::Value {
        // The cart's lines with the added one (the transition's `context.lines`).
        let mut lines = args.context.lines.clone();
        if let Ev::User(Event::CartAdd { line }) = args.event {
            lines.push(ContextLinesItem { sku: line.sku.clone(), quantity: line.quantity });
        }
        serde_json::to_value(lines).expect("lines serialize")
    }

    fn delay_backoff(&self, context: &Context, _event: &Ev<Event>) -> u64 {
        // Longer while more lines wait.
        250 + 50 * context.lines.len() as u64
    }

    fn actor_charge_card(&self) -> Arc<dyn Logic> {
        // The charge is the host's: it completes the pending `@xstate.effect`.
        Arc::new(async_logic(Some("chargeCard".into()), None).expect("valid logic"))
    }
}

pub struct Component;

impl xstate_wasi_guest::MachineComponent for Component {
    type Context = Context;
    type Event = Event;
    const DEFINITION: &'static str = checkout::DEFINITION;
    const SCHEMAS: &'static str = checkout::SCHEMAS;

    fn machine() -> Result<Machine<Context, Event>, String> {
        checkout::machine(Arc::new(Shop)).map_err(|e| e.to_string())
    }
}

xstate_wasi_guest::export_machine!(Component);
