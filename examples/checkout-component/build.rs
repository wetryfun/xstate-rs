fn main() {
    // The checkout example's bundle, generated into this crate.
    if let Err(e) = xstate_codegen::Builder::new().machine("../checkout/machines/checkout.xstate.json").build() {
        panic!("{e}");
    }
}
