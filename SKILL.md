# Implementing a trading strategy in simple-bot

Instructions for an agent (human or AI) asked to add or modify a trading
strategy in this repo. Read this whole file before touching code — the
short version is "copy `src/brain/testperpv1/`, rename things, rewrite
`evaluate_inner`," but the details below matter for getting a strategy
that actually compiles and runs inside the real WASM bot runtime.

## What this repo is

`simple-bot` compiles to a WASM component that runs inside the Catscope
Bot runtime (catscope.io). It depends on
[`catscope-rust-bot`](https://github.com/noncepad/catscope-rust-bot) —
either a local path dependency (`../catscope-rust-bot`, for iterating on
both repos together) or a `git`+`tag` dependency (for a self-contained
build) — see `Cargo.toml`'s `[dependencies]` block for which is currently
active and why.

`catscope-rust-bot` provides everything reusable: the event loop, wallet/
transaction batching, the account-subscription graph, per-DEX state
(`trader::dex::*` — Orca, Raydium, Solend, Kamino, Phoenix, etc.), the
router/pricing graph, and message (de)serialization to/from the Go-side
brain. `simple-bot` supplies exactly one thing catscope-rust-bot's own
build deliberately omits when used this way: the WIT component entry
point (`Component`/`export!` in `src/lib.rs`) and the actual strategy
logic under `src/brain/`.

Everything from catscope-rust-bot is reachable at this crate's own root
via a glob re-export in `src/lib.rs` (`pub use catscope_rust_bot::*;`).
This means code copied verbatim from catscope-rust-bot — including its
own internal `crate::event::...`, `crate::graph::...`,
`log_info!`/`log_warn!`/`log_error!` macro calls, etc. — keeps compiling
unmodified once it's physically inside `src/brain/` here. **Do not
rewrite `crate::` paths in copied files.** If something doesn't resolve,
the fix is almost always relaxing a visibility modifier in
catscope-rust-bot (`pub(crate)` → `pub`) or adding a dependency to
`Cargo.toml`, not editing the copied file's import paths.

## The brain module pattern

Every strategy lives in its own directory under `src/brain/` with four
files, following `src/brain/testperpv1/`'s shape exactly:

- **`mod.rs`** — the `Hook` struct (holds `Rc<UnsafeCell<...>>` handles to
  parser, wallet, configuration, state, graph, poller) and the
  `EventHandler` trait impl that dispatches runtime events into it. This
  file is mostly boilerplate — see "Wiring" below for the one line that
  usually needs a rename.
- **`configuration.rs`** — static/slow-changing config: which mints,
  which wallet, anything resolved once at startup or on the `Wallet`
  message arriving. Kept free of any WIT host-import calls in `Default`
  (those calls abort outside the real WASM guest runtime — see the
  `set()`/`Default` split in `testperpv1::configuration::Configuration`).
- **`message.rs`** — the wire protocol: `CustomMessageInbound` (messages
  the Go-side brain sends down, e.g. wallet key, target allocation, a
  live APY update) and `CustomMessageOutbound` (anything sent back up —
  often just an empty placeholder, most strategies don't need one yet).
  Each variant has a corresponding Go-side encoder in
  `optimizer/brain/<strategy>/message.go` — the wire format (key byte +
  fixed-size value) must match on both sides, see the doc comments on
  `testperpv1::message::CustomMessageInbound`'s variants for the exact
  byte layout convention.
- **`state.rs`** — the actual strategy. A `State` struct (persistent
  data: open positions, cooldown timers, epoch tracking) and a
  `StateHelper<'a>` (borrows everything `State` needs access to for one
  event — graph, wallet, configuration, message queue — bundled fresh on
  every dispatch by `Hook::helper()` in `mod.rs`). This is almost always
  the only file with real trading-strategy content; the other three are
  wiring.

### Event flow (how `StateHelper` gets invoked)

`mod.rs`'s `on_event` dispatches every host-delivered event to a
`StateHelper` method, then always calls `helper.evaluate()` afterward:

```text
validator → Event::LowLatency  → helper.low_latency(llap)      (processed accounts, ~400ms)
validator → Event::Commit      → commit.process(&mut helper)   (rooted accounts, ~12s; via CommitHook)
validator → Event::Transaction → helper.mid_on_tx(list)         (drained tx notifications)
validator → Event::SlotStatus  → helper.on_slot_status(slot, status)
Go brain  → stdin               → helper.on_message(action)     (inbound wire messages)
                                 → helper.evaluate()             (ALWAYS runs last, every event)
```

`evaluate()` (and whatever private `evaluate_inner()` it delegates to) is
the reactive decision loop — it runs after *every* event, so it must be
cheap when there's nothing to do and must itself gate any real
time-based logic (e.g. "only check every N seconds/hours") using
wall-clock time (`SystemTime::now()` / `Instant`), not event cadence.
This is where you decide whether to open, hold, or close a position.

The `EventHandler` trait itself (implemented once per `Hook` in
`mod.rs`, do not change its shape) is:

```rust
pub trait EventHandler {
    fn on_load(&mut self, poller: EventPoller, args: &[String]) -> Result<(), CatscopeGuestError>;
    fn on_unload(&mut self) -> Result<(), CatscopeGuestError>;
    fn on_event(&mut self, event: Event) -> Result<(), CatscopeGuestError>;
    fn flush(&mut self) -> Result<(), CatscopeGuestError>;
}
```

## Step-by-step: adding a new strategy

1. **Copy the template.**
   ```
   cp -r src/brain/testperpv1 src/brain/<your_strategy>
   ```
2. **Register the module** in `src/brain/mod.rs`:
   ```rust
   pub mod your_strategy;
   ```
3. **Rename the public type** in `<your_strategy>/mod.rs`: rename
   `TestPerpV1Hook` (struct, `impl` blocks, `new()`) to something
   strategy-specific, e.g. `YourStrategyHook`. Everything else in that
   file (the `EventHandler` impl, `helper()`, field shapes) stays as-is
   unless your strategy needs extra fields on `Hook` itself (rare —
   almost everything belongs on `State`/`StateHelper` instead).
4. **Wire it up** in `src/lib.rs` — this is the one line that actually
   selects which strategy runs:
   ```rust
   let b = brain::your_strategy::YourStrategyHook::new(Rc::new(UnsafeCell::new(Parser::default())));
   ```
5. **Write the actual strategy** in `<your_strategy>/state.rs`:
   - Strip out what you don't need from the copied `State`/`StateHelper`
     (testperpv1 carries Solend/Kamino/Phoenix basis-trade legs, LST-loop
     projection logging, and a real-tx smoke-test state machine —
     `TestPhase` — none of which your strategy necessarily needs).
   - Put your entry/exit decision logic in `evaluate_inner()` (or
     whatever you rename it to) — this runs after every event, see
     "Event flow" above.
   - Reuse `trader::dex::*` (Orca/Raydium/Solend/Kamino/Phoenix/etc.),
     `trader::router`/`trader::pricegraph::TradeRouter`, and `Wallet` for
     actually reading prices/positions and sending transactions — these
     are catscope-rust-bot's reusable primitives, don't reimplement them.
   - If your strategy needs config data baked in at compile time (pool
     lists, mint decimals, curated symbols, etc.), it's already available
     via the `*_config` modules (`kamino_config`, `router_pools_config`,
     `top_pools_config`, etc.) — see catscope-rust-bot's `src/lib.rs` for
     the full list, all reachable here via the glob re-export.
6. **Extend the wire protocol** in `<your_strategy>/message.rs` only if
   the Go-side brain needs to push new config/data down (new
   `CustomMessageInbound` variant) or your strategy needs to report
   something back up (new `CustomMessageOutbound` variant). Add the
   matching encoder/decoder on the Go side in
   `optimizer/brain/<your_strategy>/message.go` — the two must agree
   byte-for-byte.
7. **Adjust `configuration.rs`** only if your strategy needs different
   static config (different mints, different slippage default, etc.).

## Data build-time dependency (why `cargo build` might fail with "file not found")

`catscope-rust-bot`'s `build.rs` bakes several tables (pool lists, mint
decimals, lending reserves, etc.) into the binary at compile time, read
from `target/*.json` files (or, as a fallback, the same filenames at its
own repo root). Whether that data is present depends on which dependency
mode is active in this crate's `Cargo.toml`:

- **Path dependency** (`catscope-rust-bot = { path = "../catscope-rust-bot", ... }`):
  needs `../catscope-rust-bot/target/*.json` populated — either by a real
  `optimizer` `Prefetcher.Build` run (real market data) or by copying
  `../catscope-rust-bot/contrib/test-json/*.json` into
  `../catscope-rust-bot/target/` (sample data, fine for local iteration).
- **Git+tag dependency** (`catscope-rust-bot = { git = "...", tag = "...", ... }`):
  the tagged commit must already carry root-level `*.json` fixtures
  (`contrib/` is stripped before every publish — see catscope-rust-bot's
  `contrib/remote-git.sh`). If you bump to a newer tag and the build
  fails with a `build.rs` panic like `failed to open <table>.json
  (required)`, that tag doesn't have the fixtures; either add them (see
  catscope-rust-bot's commit that introduced the current minimal set) or
  pin back to a tag that has them.

Either way, most tables degrade gracefully to empty with just a
`cargo:warning=...` if missing — only a handful are hard-required
(currently: `router.json`, the four core pool tables, `kamino_reserve.json`/
`solend_reserve.json` with at least one real row each, and `mint_info.json`
for the curated symbol set). A missing *optional* table just means your
strategy sees empty data for that table, not a build failure.

## Build / test loop

```
cargo build --release          # builds for wasm32-wasip2 (see .cargo/config.toml)
cargo test --target x86_64-unknown-linux-gnu --lib   # unit tests, native target
                                # NOTE: always pass --lib here -- without it, cargo
                                # also tries to link a native cdylib for doctests,
                                # which fails on Linux (empty-exports version-script
                                # bug) since this crate exports no native symbols.
wasm-tools component wit target/wasm32-wasip2/release/simple_bot.wasm
                                # sanity-check the compiled output is a genuine,
                                # correctly-shaped wasi:cli/run component
```

Dead-code warnings on state/methods you haven't wired up yet are
expected and harmless until `evaluate_inner()` actually calls them.

## What NOT to do

- Don't edit `catscope-rust-bot` to add strategy-specific logic — it's a
  shared dependency; strategy code belongs entirely under `src/brain/`
  here. Only touch catscope-rust-bot for genuinely reusable primitives
  (a new DEX integration, a new router capability), and treat that as a
  separate, deliberate change.
- Don't rewrite `crate::`-prefixed import paths in files copied from
  catscope-rust-bot — see "The brain module pattern" above for why they
  already resolve correctly.
- Don't remove the `component-entrypoint` feature gate's
  `default-features = false` on the `catscope-rust-bot` dependency in
  `Cargo.toml` — without it, catscope-rust-bot's own WIT component entry
  point collides with this crate's.
