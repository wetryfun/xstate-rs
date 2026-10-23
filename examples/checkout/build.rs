fn main() {
    let built = xstate_codegen::Builder::new()
        .machine("machines/checkout.xstate.json")
        .machine("machines/types.xstate.json")
        .build();
    if let Err(e) = built {
        panic!("{e}");
    }
}
