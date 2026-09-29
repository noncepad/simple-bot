use std::{cell::RefCell, cell::UnsafeCell, rc::Rc};

use catscope_rust_bot::{event_loop::run, exports::wasi::cli::run::Guest, message::Parser};
use catscope_rust_bot::export;
// wit_bindgen's generated `export!` macro body references its own inner
// per-interface "cabi" macro via `self::exports::wasi::cli::run::...` --
// that only resolves if the `exports` module itself (not just the
// `Guest` trait inside it) is also in scope at this crate's own root.
use catscope_rust_bot::exports;

// Re-exported (glob, not enumerated -- catscope-rust-bot's crate root
// has 20+ build-time `*_config` modules alone) under the same names
// catscope-rust-bot itself uses at its own crate root, so brain modules
// copied verbatim from there (see brain::testperpv1, and copy that same
// pattern for a new brain) resolve their own unmodified
// `crate::err::...`/`crate::event::...`/`crate::router_config::...`/etc.
// paths and `log_debug!`/`log_info!`/`log_warn!`/`log_error!` macro
// calls without any text rewriting. This crate's own local `brain`
// module (declared below) shadows catscope-rust-bot's own same-named
// `brain` module without conflict -- an explicit item declaration always
// wins over a glob import.
pub use catscope_rust_bot::*;

pub mod brain;

struct Component;

impl Guest for Component {
    /// This is the entry point for the bot. Swap `testperpv1::TestPerpV1Hook`
    /// for your own brain (copy the same mod.rs/configuration.rs/message.rs/
    /// state.rs shape) once you're ready to write real trading logic here.
    fn run() -> Result<(), ()> {
        let args = std::env::args();
        let mut l_arg = Vec::new();
        for x in args {
            l_arg.push(x);
        }
        let b = brain::testperpv1::TestPerpV1Hook::new(Rc::new(UnsafeCell::new(Parser::default())));
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
