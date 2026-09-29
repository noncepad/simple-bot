//! Trading strategies live here, one directory per strategy, each with
//! the same four-file shape as `testperpv1` (copied from catscope-rust-bot
//! as an example/template, not a real strategy): `mod.rs` (Hook struct +
//! EventHandler impl), `configuration.rs`, `message.rs`, `state.rs` (the
//! actual decision logic). See /SKILL.md for the full step-by-step on
//! adding a new one -- register it below, then wire it up in
//! `crate::lib::run()`.

pub mod testperpv1;
// pub mod your_strategy; // see /SKILL.md step 2
