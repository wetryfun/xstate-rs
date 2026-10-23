//! A checkout machine authored in TypeScript (`machines/checkout.ts`),
//! extracted to `machines/checkout.xstate.json`, and compiled into typed
//! Rust by `xstate-codegen` in `build.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use xstate_rt::step::async_logic;
use xstate_rt::{Ev, ExprArgs, GuardArgs, Logic};

pub mod checkout {
    xstate_rt::include_machine!("checkout");
}

/// A machine that exercises the schema-to-Rust type mapping.
pub mod types {
    xstate_rt::include_machine!("types");
}

use checkout::{CheckoutImpl, Context, ContextLinesItem, Event, NotifyParams, WithinLimitParams};
use xstate_rt::__codegen::serde_json;

/// The shop's side of the checkout: notifications, auditing and payment.
pub struct Shop {
    pub notifications: Mutex<Vec<NotifyParams>>,
    pub audited: AtomicUsize,
    pub backoff_ms: u64,
}

impl Default for Shop {
    fn default() -> Self {
        Self { notifications: Mutex::new(Vec::new()), audited: AtomicUsize::new(0), backoff_ms: 250 }
    }
}

impl CheckoutImpl for Shop {
    fn guard_within_limit(&self, args: &GuardArgs<'_, Context, Event>, params: &WithinLimitParams) -> bool {
        matches!(args.event, Ev::User(Event::Checkout { total }) if *total <= params.max)
    }

    fn guard_retryable(&self, args: &GuardArgs<'_, Context, Event>) -> bool {
        args.event.field("error").and_then(|e| e.get("retryable")?.as_bool()).unwrap_or(false)
    }

    fn action_notify(&self, params: &NotifyParams) {
        self.notifications.lock().unwrap().push(params.clone());
    }

    fn action_audit(&self) {
        self.audited.fetch_add(1, Ordering::Relaxed);
    }

    fn expr_add_line(&self, args: &ExprArgs<'_, Context, Event>) -> serde_json::Value {
        // The cart's lines with the added one (the transition's `context.lines`).
        let mut lines = args.context.lines.clone();
        if let Ev::User(Event::CartAdd { line }) = args.event {
            lines.push(ContextLinesItem { sku: line.sku.clone(), quantity: line.quantity });
        }
        serde_json::to_value(lines).expect("lines serialize")
    }

    fn delay_backoff(&self, _context: &Context, _event: &Ev<Event>) -> u64 {
        self.backoff_ms
    }

    fn actor_charge_card(&self) -> Arc<dyn Logic> {
        // The charge runs outside the machine: the host resolves the pending
        // `@xstate.effect` with `xstate.async.resolve` / `xstate.async.reject`.
        Arc::new(async_logic(Some("chargeCard".into()), None).expect("valid logic"))
    }
}
