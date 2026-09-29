use std::{cell::RefCell, cell::UnsafeCell, rc::Rc};

use catscope_rust_bot::export;
use catscope_rust_bot::{event_loop::run, exports::wasi::cli::run::Guest, message::Parser};
// wit_bindgen's generated `export!` macro body references its own inner
// per-interface "cabi" macro via `self::exports::wasi::cli::run::...` --
// that only resolves if the `exports` module itself (not just the
// `Guest` trait inside it) is also in scope at this crate's own root.
use catscope_rust_bot::exports;

pub use catscope_rust_bot::*;

pub mod brain;

struct Component;

impl Guest for Component {
    fn run() -> Result<(), ()> {
        let args = std::env::args();
        let mut l_arg = Vec::new();
        for x in args {
            l_arg.push(x);
        }
        // Set the event loop object here -- SKILL.md step 4: swap for
        // `brain::<your_strategy>::YourStrategyHook::new(...)`.
        let b = brain::StrategyV1Hook::new(Rc::new(UnsafeCell::new(Parser::default())));
        let sampler = Rc::new(RefCell::new(b));

        let r = run(sampler, l_arg);
        if let Err(e) = r {
            panic!("program exited with error: {e}")
        }
        Ok(())
    }
}

// Must be called exactly once across the whole final link for the
// "catscopevalidator" world, which is why catscope-rust-bot's own
// Component/export!(Component) is feature-gated off for this crate.
export!(Component);
