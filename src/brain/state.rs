use crate::trader::dex::kamino_xstocks_watcher;
use crate::util::pubkey_from_account_id;
use crate::{
    atl_config,
    brain::{
        Configuration,
        message::{CustomMessageInbound, CustomMessageOutbound},
    },
    catscope::witbot::shooter::{Header, Tokenaccountv1},
    drift_config,
    err::CatscopeGuestError,
    event::SlotStatus,
    graph::{AccountId, CommitHook, Graph, LowLatencyAccountUpdate, SubscriptionQueue},
    jet_config, kamino_config, log_error, log_info, log_warn, marginfi_config,
    message::{InboundMesasgeHandler, MessageAction, MessageSend},
    orca_config, phoenix_config, pumpfun_config, pumpswap_config, raydium_amm_config,
    raydium_clmm_config, raydium_cpmm_config, router_config, router_pools_config, sanctum_config,
    solend_config, symbol_mint_config, target_allocation_config, top_pools_config,
    tracked_accounts_config,
    trader::{
        dex::{DexState, solend, update::Updater as _},
        perp_router::PerpRouter,
        planner,
        pricegraph::TradeRouter,
        router,
    },
    trading_config,
    txview::TransactionList,
    util::{account_id_from_pubkey, rc_unlock, resolve_symbol_decimals, resolve_symbol_mint},
    wallet::{PriorityLevel, Wallet},
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{clock::Slot, signature::Keypair, signer::Signer};
use std::{
    cell::UnsafeCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

/// Both Phoenix and Velocity settle funding on a real hourly cadence --
/// verified from each protocol's own source this session (see
/// `trader::perp_router`'s module doc), not a bot-side choice.
const SECONDS_PER_EPOCH: i64 = 3600;

/// Builds the 3-tier build-time liquidity router from `router_pools_config`
/// (the `ROUTER_POOLS` snapshot embedded at compile time) -- copied
/// verbatim from `arbv1::state::build_liquidity_router` rather than
/// shared, matching this codebase's established convention of copying
/// small per-mode boilerplate instead of factoring it out. Only used to
/// seed `State::spot_router`'s node set once, in `StateHelper::on_load`
/// -- see `TradeRouter::from_router`'s doc comment for why that seeding
/// step is required at all.
fn build_liquidity_router() -> router::Router {
    let cfg = &router_config::ROUTER_CONFIG;
    let mut r = router::Router::new(cfg.token_count, cfg.lambda);
    for core_mint in cfg.core_mints {
        r.register_mint(account_id_from_pubkey(&Pubkey::new_from_array(core_mint)));
    }
    let mut pools = Vec::with_capacity(router_pools_config::ROUTER_POOLS.len());
    for p in router_pools_config::ROUTER_POOLS {
        let token_a = r.register_mint(account_id_from_pubkey(&Pubkey::new_from_array(p.mint_a)));
        let token_b = r.register_mint(account_id_from_pubkey(&Pubkey::new_from_array(p.mint_b)));
        pools.push(router::Pool {
            token_a,
            token_b,
            liquidity_usd: p.liquidity_usd,
            price_a_to_b: p.price_a_to_b,
        });
    }
    r.rebuild_partitions(&pools);
    r
}

#[derive(Debug)]
struct KeypairExtra {
    #[allow(dead_code)]
    rc_keypair: Rc<UnsafeCell<Keypair>>,
    account_id: AccountId,
}

/// One symbol's target allocation, join-keyed to its mint via
/// `resolve_symbol_mint` -- `account_id` is `None` until resolved.
/// Resolution can't happen at `State::default()` time: `resolve_symbol_mint`
/// bottoms out in `account_id_from_pubkey`, a WIT host import that only
/// works inside the real WASM guest runtime (see `util::resolve_symbol_mint`'s
/// own doc and its native-test caveat) -- `Default::default()` must stay
/// safe to construct in a native unit test, so resolution instead happens
/// in `on_message` at the same point `Configuration::set`'s own
/// `mint_sol`/`mint_usdc` resolution already does (the `Wallet` arm --
/// the first point in this file's message flow confirmed to be inside a
/// live WASM guest), plus per-entry in the `TargetAllocation` arm itself
/// for runtime updates.
#[derive(Debug, Clone, Copy)]
struct TargetAllocationEntry {
    account_id: Option<AccountId>,
    allocation_pct: f64,
}

#[derive(Debug)]
pub(crate) struct State {
    last_slot: Slot,
    slot_delta_since_start: Slot,
    /// Paces every large startup subscription burst (Raydium/Orca/
    /// lending-reserve pools, ~32,000 requests total) across many slots
    /// instead of firing them all in one blocking `bulk_subscribe` call
    /// -- see [`SubscriptionQueue`]'s own doc comment. Drained by
    /// [`MAX_SUBSCRIBES_PER_SLOT`] requests per slot from
    /// `CommitHook::finish`.
    subscription_queue: SubscriptionQueue,
    /// Wall-clock instant `CommitHook::start` fired for the slot
    /// currently being processed -- `finish()` measures the elapsed time
    /// against this and logs a warning if it exceeds
    /// `SLOT_TIMING_TARGET_MS`. Diagnostic added while investigating this
    /// session's real `stdio timeout` disconnects -- whether slow
    /// guest-side per-slot processing (not just log volume, already
    /// fixed) is a contributing factor. Covers only this commit's own
    /// `start()`-through-`finish()` span (every `on_account`/`on_token`
    /// call plus `finish()`'s own flush work) -- see
    /// `o_prev_commit_start_instant` for the *other* number, the gap
    /// between one slot's `start()` and the next.
    o_commit_start_instant: Option<std::time::Instant>,
    /// Wall-clock instant the *previous* `CommitHook::start` fired --
    /// `start()` measures the gap against this every time it's called,
    /// before overwriting it for next time. Unlike `o_commit_start_instant`,
    /// this captures everything that happens *between* commits too:
    /// `evaluate()`'s own work, other event types (`LowLatency`, `Stdin`,
    /// `SlotStatus`), and genuine idle time waiting for the next event --
    /// the fuller picture of end-to-end guest responsiveness, not just
    /// this commit's own processing.
    o_prev_commit_start_instant: Option<std::time::Instant>,
    /// Cumulative wall-clock time spent inside `low_latency()` since the
    /// previous `CommitHook::start()` call -- `start()` logs and resets
    /// this every time, alongside the gap-since-previous-start number, to
    /// isolate how much of that gap (if any) is actually low-latency
    /// account-update processing versus something else. Diagnostic added
    /// to test a specific hypothesis: that low-latency updates (much
    /// higher volume/frequency than rooted commits) are the real
    /// contributor to this session's `stdio timeout` disconnects.
    low_latency_elapsed_since_last_start: std::time::Duration,
    /// How many low-latency account/token updates were processed in the
    /// same window as `low_latency_elapsed_since_last_start` -- reported
    /// alongside it so a large elapsed time can be told apart from "a
    /// genuinely huge batch arrived" vs. "processing got slow".
    low_latency_count_since_last_start: u64,
    /// Cumulative wall-clock time spent inside `evaluate()` since the
    /// previous `CommitHook::start()` call -- same accumulate-and-reset
    /// shape as `low_latency_elapsed_since_last_start`, reported alongside
    /// it. `evaluate()` runs after *every* event (`Commit`, `LowLatency`,
    /// `Stdin`, `Transaction`, `SlotStatus` -- see `on_event`'s tail call),
    /// so this is the other real candidate (besides `low_latency()`) for
    /// where the gap-since-previous-start time is actually going. Added
    /// to test a specific hypothesis: a negative-cycle/route search inside
    /// `evaluate()`'s call graph growing superlinearly as more of the
    /// ~32,000-request subscription burst drains in and the live pool/
    /// router graph grows.
    evaluate_elapsed_since_last_start: std::time::Duration,
    /// How many `evaluate()` calls happened in the same window as
    /// `evaluate_elapsed_since_last_start` -- same role as
    /// `low_latency_count_since_last_start`.
    evaluate_count_since_last_start: u64,
    last_action_slot: Option<Slot>,
    o_rc_keypair: Option<KeypairExtra>,
    router: PerpRouter,
    /// The epoch currently being accumulated -- `None` until the first
    /// `evaluate()` call after `on_load`. Distinct from `PerpRouter`'s
    /// own internal pending buffers: this just tracks *when* to call
    /// `close_epoch`.
    pending_epoch_ts: Option<i64>,
    /// Spot-market execution hook -- see this module's doc comment and
    /// `StateHelper::execute_spot_leg`. `None` until `on_load`, same
    /// lifecycle as `o_phoenix`/`o_solend_position`.
    o_dex: Option<DexState>,
    /// Bellman-Ford spot price graph, fed incrementally by `low_latency`/
    /// `CommitHook::on_account` exactly like `arbv1::state`'s `router`
    /// field -- named `spot_router` here since `router` above is already
    /// taken by `PerpRouter`.
    spot_router: TradeRouter,
    /// Target portfolio allocation -- fraction of total portfolio value
    /// (0.0-1.0) to hold in each symbol, e.g. `0.30` for "target 30% of
    /// the portfolio in this symbol". The remainder is implicitly
    /// USD/stable (no explicit USD entry). Rebalancing toward this
    /// target is what realizes profit/loss. Seeded from
    /// `target_allocation_config::DEFAULT_TARGET_ALLOCATION`
    /// (build.rs-baked, itself read from the optimizer's own
    /// `prefetch.db` at compile time) and live-updated at runtime by
    /// `CustomMessageInbound::TargetAllocation` (see `on_message`
    /// below). Each entry is join-keyed to its mint via
    /// `resolve_symbol_mint` -- see `TargetAllocationEntry`'s doc for
    /// why that resolution is deferred, not done here at construction.
    /// Not yet consumed by any rebalance/selection logic -- see this
    /// module's plan doc for why that's a separate follow-up.
    target_allocation_pct: HashMap<String, TargetAllocationEntry>,
    /// Real annualized SOL-per-LST staking yield per liquid-staking-token
    /// symbol (e.g. `"jitoSOL" -> 0.073`), pushed periodically by
    /// `optimizer watch-lst-yield` via `CustomMessageInbound::LstApy` (see
    /// `on_message` below) -- this bot can't compute it itself (no
    /// persistent storage across restarts, see
    /// `leveraged_yield_farming_plan.md`'s "Phase 0"). Refreshed in place
    /// per symbol, not accumulated; absent until at least one real update
    /// has arrived. Read by `log_lst_loop_projection` for the
    /// Phase-1-style read-only leverage projection -- nothing in this bot
    /// opens a real leveraged position from this yet.
    lst_staking_apy: HashMap<String, f64>,
    /// Latest `Slot` `low_latency`'s fast, ~400ms processed-account stream
    /// has recorded for each account id -- lets
    /// [`StateHelper::is_newer_than_low_latency`] reject a rooted
    /// `Self::on_account` (~12s-late, but finalized) update when
    /// `low_latency` has already delivered the same or newer data for
    /// that account, mirroring `arbv1::state`'s identically-named
    /// field/pattern (see its own doc comment for the full reasoning).
    ///
    /// Added after a real, on-chain-confirmed bug this session: this
    /// mode's `on_account` had no such gate, so a late rooted duplicate
    /// re-delivering this wallet's *pre-withdraw* Solend obligation bytes
    /// (from the `[3/16]` deposit, moments before the `[4/16]` withdraw
    /// that superseded them) silently rolled `o_solend_position`'s cached
    /// obligation back to "still has a USDC deposit" -- and since nothing
    /// else ever touches that account again, the wrong state stuck
    /// around permanently, not just "briefly stale" as this file's old
    /// (now-corrected) doc comment on `Self::on_account` assumed. That
    /// fed `open_solend_borrow_leg`'s `has_usdc_collateral` check a false
    /// positive, which built a `refresh_obligation` call against a
    /// reserve list that no longer matched the real (by-then-empty)
    /// on-chain obligation, tripping Solend's own strict remaining-
    /// accounts check (`Custom(0xd) InvalidAccountInput`, "Too many
    /// obligation deposit or borrow reserves provided") on every retry,
    /// live-confirmed via `solana confirm -v` and a direct on-chain byte
    /// decode of the obligation account.
    m_account_slot: HashMap<AccountId, Slot>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            last_slot: 0,
            slot_delta_since_start: 0,
            subscription_queue: SubscriptionQueue::default(),
            o_commit_start_instant: None,
            o_prev_commit_start_instant: None,
            low_latency_elapsed_since_last_start: std::time::Duration::ZERO,
            low_latency_count_since_last_start: 0,
            evaluate_elapsed_since_last_start: std::time::Duration::ZERO,
            evaluate_count_since_last_start: 0,
            last_action_slot: None,
            o_rc_keypair: None,
            router: PerpRouter::default(),
            pending_epoch_ts: None,
            o_dex: None,
            spot_router: TradeRouter::default(),
            target_allocation_pct: target_allocation_config::DEFAULT_TARGET_ALLOCATION
                .iter()
                .map(|&(s, allocation_pct)| {
                    (
                        s.to_string(),
                        TargetAllocationEntry {
                            account_id: None,
                            allocation_pct,
                        },
                    )
                })
                .collect(),
            lst_staking_apy: HashMap::new(),
            m_account_slot: HashMap::default(),
        }
    }
}

impl State {
    fn wallet(&self) -> Option<AccountId> {
        let ke = self.o_rc_keypair.as_ref()?;
        Some(ke.account_id)
    }

    /// Resolves every not-yet-resolved `target_allocation_pct` entry's
    /// mint via `resolve_symbol_mint`. Safe to call at any call site
    /// confirmed to run inside the live WASM guest (see
    /// `TargetAllocationEntry`'s doc) -- currently only the `Wallet`
    /// arm, which fires once per bot lifetime, so a symbol with no
    /// curated mint (not in `SYMBOL_MINT_MAP`) logging on every call
    /// isn't a practical spam risk; re-check if a future call site
    /// invokes this on a tighter loop.
    fn resolve_target_allocation_mints(&mut self) {
        for (symbol, entry) in self.target_allocation_pct.iter_mut() {
            if entry.account_id.is_some() {
                continue;
            }
            match resolve_symbol_mint(symbol) {
                Some(account_id) => entry.account_id = Some(account_id),
                None => {
                    log_error!(
                        ": target allocation symbol {} has no curated mint -- cannot resolve to AccountId",
                        symbol,
                    );
                }
            }
        }
    }
}

pub(crate) struct StateHelper<'a> {
    pub(crate) graph: &'a mut Graph,
    pub(crate) nonce: &'a mut u32,
    pub(crate) o_commit_slot: Option<Slot>,
    pub(crate) state: &'a mut State,
    pub(crate) wallet: &'a mut Wallet,
    pub(crate) configuration: &'a mut Configuration,
    pub(crate) q_msg: &'a mut VecDeque<MessageSend<CustomMessageOutbound>>,
}

impl<'a> StateHelper<'a> {
    pub(crate) fn nonce_check(&mut self, other_nonce: u32) -> Result<(), CatscopeGuestError> {
        if *self.nonce != other_nonce {
            return Err(CatscopeGuestError::BadNonce(*self.nonce, other_nonce));
        }
        *self.nonce += 1;
        Ok(())
    }

    /// Every pubkey baked in by build.rs's generated tables, across every
    /// `[u8; 32]` field of every generated struct -- not just each
    /// table's own "root" pool/reserve/market pubkey, but secondary
    /// references too (vaults, oracle keys, lending markets, etc.).
    /// Gathered once at boot and batch-resolved to `AccountId`s via
    /// `PubkeyAccountIdCache::account_ids` (called from `on_load`,
    /// before `DexState::new()`/`PhoenixState::new_and_subscribe` run),
    /// so every one of those constructors' own individual
    /// `account_id_from_pubkey` calls become cache hits instead of a
    /// fresh host round-trip each -- see `account_ids`'s own doc comment
    /// for why this is still "one host call per miss" rather than a
    /// single round-trip (`pubkey-map-by-pubkey` never gained a batched
    /// parameter, unlike the accountid-to-pubkey direction).
    fn build_time_pubkeys() -> Vec<Pubkey> {
        let mut out = Vec::new();
        macro_rules! push {
            ($bytes:expr) => {
                out.push(Pubkey::new_from_array($bytes));
            };
        }

        for p in raydium_amm_config::RAYDIUM_AMM_POOLS {
            push!(p.pubkey);
            push!(p.market_bids);
            push!(p.market_asks);
            push!(p.market_event_queue);
            push!(p.market_coin_vault);
            push!(p.market_pc_vault);
            push!(p.market_vault_signer);
        }
        for p in raydium_clmm_config::RAYDIUM_CLMM_POOLS {
            push!(p.pubkey);
            push!(p.mint_0);
            push!(p.mint_1);
        }
        for p in raydium_cpmm_config::RAYDIUM_CPMM_POOLS {
            push!(p.pubkey);
            push!(p.mint_0);
            push!(p.mint_1);
        }
        for p in orca_config::ORCA_WHIRLPOOL_POOLS {
            push!(p.pubkey);
            push!(p.mint_a);
            push!(p.mint_b);
        }
        for p in kamino_config::KAMINO_RESERVES {
            push!(p.pubkey);
            push!(p.lending_market);
            push!(p.supply_vault);
            push!(p.fee_vault);
        }
        for p in sanctum_config::SANCTUM_LSTS {
            push!(p.mint);
            push!(p.sol_value_calculator);
            push!(p.pool_state);
        }
        for p in phoenix_config::PHOENIX_MARKETS {
            push!(p.market_account);
        }
        for p in drift_config::DRIFT_SPOT_MARKETS {
            push!(p.pubkey);
            push!(p.mint);
            push!(p.vault);
        }
        for p in marginfi_config::MARGINFI_BANKS {
            push!(p.pubkey);
            push!(p.group);
            push!(p.mint);
            push!(p.oracle_key);
        }
        for p in solend_config::SOLEND_RESERVES {
            push!(p.pubkey);
            push!(p.lending_market);
            push!(p.mint);
            push!(p.supply_vault);
        }
        for p in pumpfun_config::PUMPFUN_BONDING_CURVES {
            push!(p.mint);
        }
        for p in pumpswap_config::PUMPSWAP_POOLS {
            push!(p.pool);
            push!(p.base_mint);
            push!(p.quote_mint);
            push!(p.base_vault);
            push!(p.quote_vault);
        }
        for p in jet_config::JET_RESERVES {
            push!(p.pubkey);
            push!(p.market);
            push!(p.mint);
            push!(p.vault);
        }
        for p in top_pools_config::TOP_POOLS {
            push!(p.pubkey);
            push!(p.mint_a);
            push!(p.mint_b);
        }
        for p in symbol_mint_config::SYMBOL_MINT_MAP {
            push!(p.mint);
        }
        for p in router_pools_config::ROUTER_POOLS {
            push!(p.mint_a);
            push!(p.mint_b);
        }
        for m in router_config::ROUTER_CONFIG.core_mints {
            push!(m);
        }
        for bytes in tracked_accounts_config::TRACKED_TOKEN_ACCOUNTS {
            push!(*bytes);
        }
        for entry in atl_config::ADDRESS_LOOKUP_TABLES {
            let (table, addrs) = *entry;
            push!(table);
            for a in addrs {
                push!(*a);
            }
        }
        for (a, b) in trading_config::TRADING_PAIRS {
            push!(*a);
            push!(*b);
        }

        out
    }

    pub(crate) fn on_load(&mut self) {
        self.configuration.count += 1;
        assert_eq!(self.configuration.count, 1);
        // Batch-resolve every build-time-baked pubkey to an AccountId
        // before any of the constructors below run their own individual
        // lookups -- see `build_time_pubkeys`'s doc comment.
        let l_pk = Self::build_time_pubkeys();
        let n_pk = l_pk.len();
        let n_ids = crate::util::pubkey_account_id_cache()
            .account_ids(&l_pk)
            .len();
        log_warn!("batch-resolved {n_ids}/{n_pk} build-time pubkeys to account ids at startup");
        assert!(
            self.state
                .o_dex
                .replace(DexState::new().expect("dex state"))
                .is_none()
        );
        // Seed spot_router's node set from the build-time liquidity
        // router's classified mint universe -- see
        // TradeRouter::from_router's doc comment: live pool registration
        // (refresh_account_router/refresh_token_router) only *looks up*
        // nodes, it never creates them, so route_slippage_aware would
        // silently return None for every mint forever without this.
        // Mirrors arbv1::state::on_load exactly (same
        // build_liquidity_router helper below).
        self.state.spot_router = TradeRouter::from_router(&build_liquidity_router());
        log_info!(": bot has been successfully uploaded to validator");
    }

    pub(crate) fn on_slot_status(&mut self, slot: Slot, status: SlotStatus) {
        if status == SlotStatus::Dead {
            log_info!(": slot {slot}; status dead");
        }
    }

    /// Real, byte-for-byte identical to classic SPL Token's fixed-size
    /// prefix (mint 32B + owner 32B + amount u64 LE at offset 64..72) --
    /// Token-2022 keeps this layout even for accounts with extensions
    /// (extensions are appended after the base 165-byte struct, keyed by a
    /// discriminator at offset 165). See `State::o_tslax_ata`'s doc
    /// comment for why this bot reads the amount directly instead of going
    /// through `Wallet::token()`. Returns `None` if `body` is too short to
    /// contain the amount field at all (e.g. an account not yet
    /// initialized).
    fn parse_token_amount(body: &[u8]) -> Option<u64> {
        body.get(64..72)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    }

    /// Records `slot` as the latest slot `low_latency` has processed for
    /// `account_id` -- unconditional, never a gate (mirrors
    /// `arbv1::StateHelper::record_low_latency_slot`'s identical
    /// reasoning: a single slot can carry multiple updates for the same
    /// account, each strictly newer than the last even though
    /// `header.slot` doesn't change between them, so gating writes here
    /// would silently drop later-but-same-slot updates).
    fn record_low_latency_slot(&mut self, account_id: AccountId, slot: Slot) {
        self.state.m_account_slot.insert(account_id, slot);
    }

    /// Gate for the rooted `Self::on_account` stream's position-tracking
    /// observers only: true iff `slot` is strictly newer than whatever
    /// `record_low_latency_slot` has already recorded for `account_id`.
    /// See [`State::m_account_slot`]'s doc comment for the real,
    /// on-chain-confirmed bug this fixes (a late rooted duplicate rolling
    /// `o_solend_position`'s cached obligation back to a stale,
    /// already-superseded snapshot) -- mirrors
    /// `arbv1::StateHelper::is_newer_than_low_latency` exactly.
    fn is_newer_than_low_latency(&self, account_id: AccountId, slot: Slot) -> bool {
        match self.state.m_account_slot.get(&account_id) {
            Some(&last) => slot > last,
            None => true,
        }
    }

    pub(crate) fn low_latency(&mut self, mut llap: LowLatencyAccountUpdate) {
        let t0 = std::time::Instant::now();
        let mut count: u64 = 0;
        while let Some(ta) = llap.token() {
            count += 1;
            self.wallet.token_mut().on_token(ta, false);
            if let Some(dex) = self.state.o_dex.as_mut() {
                _ = dex.on_token(ta);
                dex.refresh_token_router(ta.id, &mut self.state.spot_router);
            }
        }
        let zero = [];
        while let Some(account) = llap.account() {
            count += 1;
            let d = account.body.unwrap_or(&zero);
            self.record_low_latency_slot(account.header.accountid, account.header.slot);
            self.wallet.on_account(account.header, d);
            if let Some(dex) = self.state.o_dex.as_mut() {
                dex.on_account(account.header, d);
                dex.refresh_account_router(account.header.accountid, &mut self.state.spot_router);
            }
        }
        // Accumulated (not logged here) -- see `CommitHook::start`'s doc
        // comment on `low_latency_elapsed_since_last_start` for why: this
        // fires far more often than a commit, so logging every call here
        // would reintroduce the exact log-volume problem already found to
        // contribute to real `stdio timeout` disconnects this session.
        self.state.low_latency_elapsed_since_last_start += t0.elapsed();
        self.state.low_latency_count_since_last_start += count;
    }

    pub(crate) fn mid_on_tx(&mut self, mut transaction_list: TransactionList) {
        // This mode never sends a transaction -- nothing to correlate,
        // just drain the iterator (matches ::State::mid_on_tx's
        // own identical-purpose loop, rather than assuming it's safe to
        // skip entirely).
        while transaction_list.transaction().is_some() {}
    }

    pub(crate) fn current_epoch_ts() -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_secs() as i64;
        (now / SECONDS_PER_EPOCH) * SECONDS_PER_EPOCH
    }

    pub(crate) fn log_latest_layer(&self) {
        let Some(layer) = self.state.router.latest_layer() else {
            return;
        };
        if layer.edges.is_empty() {
            log_warn!(
                ": epoch {} closed @ slot {} -- no funding spread edges (no symbol had data from both venues, or rates were equal)",
                layer.epoch_ts,
                layer.slot,
            );
            return;
        }
        for edge in &layer.edges {
            log_warn!(
                ": epoch {} @ slot {}: {} long={:?} short={:?} spread_annualized={:.3}%",
                layer.epoch_ts,
                layer.slot,
                edge.asset,
                edge.from_venue,
                edge.to_venue,
                edge.spread_annualized_pct,
            );
        }
    }

    /// Diagnostic-only spot SOL/USD price probe, both directions --
    /// mirrors `arbv1::state::evaluate`'s periodic "trade router check"
    /// (same `route_slippage_aware` + `reverify_route_with_exact_quotes`
    /// pattern, same CLMM-quote safety check and pool-cooldown-on-
    /// rejection), except run both SOL->USDC and USDC->SOL so the two
    /// implied prices can be compared against each other. Read-only:
    /// never touches `execute_spot_leg`/`self.wallet`, so it can never
    /// build or send anything -- purely confirms `spot_router` is being
    /// fed live data and can find a route in *this* process.
    fn log_spot_price_probe(&mut self) {
        let (mint_sol, mint_usdc) = (self.configuration.mint_sol, self.configuration.mint_usdc);
        if mint_sol == 0 || mint_usdc == 0 {
            // Configuration::set() hasn't run yet -- no wallet keypair
            // received, so the mint AccountIds aren't resolved.
            return;
        }
        let Some(dex) = self.state.o_dex.as_ref() else {
            return;
        };
        self.state
            .spot_router
            .set_current_slot(self.state.last_slot);
        // Independent ground truth for both directions' router-implied
        // prices below -- same `pyth_sol_usd_price` cross-check
        // `arbv1::state::evaluate`'s own "trade router check" diagnostic
        // uses, not a new lookup path.
        let o_pyth = dex.pyth_sol_usd_price();
        let last_slot = self.state.last_slot;
        let log_pyth_delta = |direction: &str, router_price_usd: f64| {
            let Some(pyth_price) = o_pyth.as_ref() else {
                return;
            };
            let delta_pct =
                (router_price_usd - pyth_price.price_usd) / pyth_price.price_usd * 100.0;
            log_warn!(
                ": spot price probe @ slot {}: {} router SOL/USD={:.4} pyth SOL/USD={:.4} (conf={:.4}) delta={:+.2}%",
                last_slot,
                direction,
                router_price_usd,
                pyth_price.price_usd,
                pyth_price.confidence_usd,
                delta_pct,
            );
        };
        // Per-hop breakdown -- only for multi-hop routes (a 1-hop route's
        // top-level "amount_in -> amount_out" line above already says
        // everything). Added to debug a real observed case: a 4-hop
        // USDC->SOL route passed reverify_route_with_exact_quotes (every
        // hop individually re-quoted fine) yet the end-to-end price was
        // ~18x too low vs Pyth -- this surfaces which specific hop's
        // amount_in/amount_out ratio is the culprit.
        let log_route_hops = |direction: &str, route: &crate::trader::pricegraph::Route| {
            if route.hops.len() <= 1 {
                return;
            }
            for (i, hop) in route.hops.iter().enumerate() {
                // Temporary: resolve the pool's real pubkey for offline
                // RPC verification of the Raydium CLMM tick-array fix --
                // AccountId is a runtime-assigned host mapping
                // (shooter::pubkey_map_by_id), unrecoverable outside the
                // live WASM guest, so this is the only way to get it.
                let pool_pubkey = crate::util::pubkey_from_account_id(&hop.pool_id);
                log_warn!(
                    ": spot price probe @ slot {}: {} hop {}: dex={:?} pool={} ({:?}) {} -> {} amount_in={} amount_out={}",
                    last_slot,
                    direction,
                    i,
                    hop.dex,
                    hop.pool_id,
                    pool_pubkey,
                    hop.input_mint,
                    hop.output_mint,
                    hop.amount_in,
                    hop.amount_out,
                );
            }
        };

        const MAX_HOPS: usize = 4;
        const SOL_DECIMALS: i32 = 9;
        const USDC_DECIMALS: i32 = 6;
        // Diagnostic-only USDC probe size for the reverse direction --
        // no established constant for this direction elsewhere in the
        // codebase (arbv1's own probe only ever quotes SOL->USDC), so
        // this is a round $10 pick, comparable in spirit to arbv1's
        // build-time-configurable SOL-side probe.
        const USDC_PROBE_RAW: u64 = 10_000_000;
        let sol_probe_raw = crate::diagnostic_config::TRADE_ROUTER_PROBE_LAMPORTS;

        match self.state.spot_router.route_slippage_aware(
            mint_sol,
            mint_usdc,
            sol_probe_raw,
            MAX_HOPS,
        ) {
            Some(route) => {
                match planner::reverify_route_with_exact_quotes(
                    &route,
                    sol_probe_raw,
                    &self.state.spot_router,
                    dex,
                ) {
                    Ok(route) => {
                        let sol_in = sol_probe_raw as f64 / 10f64.powi(SOL_DECIMALS);
                        let usdc_out = route.amount_out() as f64 / 10f64.powi(USDC_DECIMALS);
                        if sol_in > 0.0 {
                            let router_price_usd = usdc_out / sol_in;
                            log_warn!(
                                ": spot price probe @ slot {}: SOL->USDC {:.9} SOL -> {:.6} USDC (price={:.4} USD/SOL, {} hop{})",
                                self.state.last_slot,
                                sol_in,
                                usdc_out,
                                router_price_usd,
                                route.n_hops(),
                                if route.n_hops() == 1 { "" } else { "s" },
                            );
                            log_pyth_delta("SOL->USDC", router_price_usd);
                            log_route_hops("SOL->USDC", &route);
                        }
                    }
                    Err(failure) => {
                        if failure.coolable {
                            self.state
                                .spot_router
                                .mark_pool_cooldown(failure.pool_id, planner::POOL_COOLDOWN_SLOTS);
                        }
                        log_warn!(
                            ": spot price probe @ slot {}: SOL->USDC rejected -- exact quote invalidated pool {} (cooling down {} slots: {})",
                            self.state.last_slot,
                            failure.pool_id,
                            planner::POOL_COOLDOWN_SLOTS,
                            failure.coolable,
                        );
                    }
                }
            }
            None => {
                log_warn!(
                    ": spot price probe @ slot {}: no SOL->USDC route found",
                    self.state.last_slot,
                );
            }
        }

        match self.state.spot_router.route_slippage_aware(
            mint_usdc,
            mint_sol,
            USDC_PROBE_RAW,
            MAX_HOPS,
        ) {
            Some(route) => {
                match planner::reverify_route_with_exact_quotes(
                    &route,
                    USDC_PROBE_RAW,
                    &self.state.spot_router,
                    dex,
                ) {
                    Ok(route) => {
                        let usdc_in = USDC_PROBE_RAW as f64 / 10f64.powi(USDC_DECIMALS);
                        let sol_out = route.amount_out() as f64 / 10f64.powi(SOL_DECIMALS);
                        if sol_out > 0.0 {
                            let router_price_usd = usdc_in / sol_out;
                            log_warn!(
                                ": spot price probe @ slot {}: USDC->SOL {:.6} USDC -> {:.9} SOL (price={:.4} USD/SOL, {} hop{})",
                                self.state.last_slot,
                                usdc_in,
                                sol_out,
                                router_price_usd,
                                route.n_hops(),
                                if route.n_hops() == 1 { "" } else { "s" },
                            );
                            log_pyth_delta("USDC->SOL", router_price_usd);
                            log_route_hops("USDC->SOL", &route);
                        }
                    }
                    Err(failure) => {
                        if failure.coolable {
                            self.state
                                .spot_router
                                .mark_pool_cooldown(failure.pool_id, planner::POOL_COOLDOWN_SLOTS);
                        }
                        log_warn!(
                            ": spot price probe @ slot {}: USDC->SOL rejected -- exact quote invalidated pool {} (cooling down {} slots: {})",
                            self.state.last_slot,
                            failure.pool_id,
                            planner::POOL_COOLDOWN_SLOTS,
                            failure.coolable,
                        );
                    }
                }
            }
            None => {
                log_warn!(
                    ": spot price probe @ slot {}: no USDC->SOL route found",
                    self.state.last_slot,
                );
            }
        }
    }

    /// One-time bootstrap for this bot's own Solend obligation:
    /// `create_obligation_account` + `init_obligation`, batched into a
    /// single transaction -- same reasoning as
    /// [`Self::bootstrap_phoenix_trader`]. No deposit here -- collateral
    /// sizing/asset choice is direction-specific (deposit-hedge deposits
    /// the underlying, borrow-hedge deposits USDC), decided at open time
    /// in `open_deposit_hedge_leg`/`open_borrow_hedge_leg`, not bootstrap
    /// time. `lending_market` is read off the USDC reserve (any tracked
    /// reserve's `lending_market` field works -- they all share Solend's
    /// one main pool -- USDC is just guaranteed to be tracked). Gated by
    /// `!solend_position.registered()`, queued instead of opening a leg,
    /// deferred to next epoch once confirmed via a real `on_account`
    /// update -- identical precedent to `bootstrap_phoenix_trader`.
    fn bootstrap_solend_obligation(&mut self) {
        let Some(owner) = self.state.wallet() else {
            return;
        };
        let Some(dex) = self.state.o_dex.as_ref() else {
            return;
        };
        let mint_usdc = self.configuration.mint_usdc;
        let Some((_, usdc_reserve)) = dex.solend().reserve_by_mint(mint_usdc) else {
            log_warn!(": bootstrap: Solend USDC reserve not observed yet");
            return;
        };
        let lending_market = usdc_reserve.lending_market;

        // `id=0`, named (not a bare literal) so this real, currently-live
        // Solend obligation address is easy to grep for -- must never
        // change, since a different id would derive a different, unfunded
        // account, silently orphaning any real position already open here.
        const SOLEND_OBLIGATION_ID: u8 = 0;
        log_warn!(": bootstrap: registering Solend obligation");
        if let Err(e) = solend::create_obligation_account(owner, SOLEND_OBLIGATION_ID, self.wallet)
        {
            log_error!(": bootstrap: solend create_obligation_account failed: {e}");
            return;
        }
        if let Err(e) =
            solend::init_obligation(owner, lending_market, SOLEND_OBLIGATION_ID, self.wallet)
        {
            log_error!(": bootstrap: solend init_obligation failed: {e}");
        }
    }

    /// Cheapest real borrow APY for `symbol` across every lending
    /// protocol with a tracked, priced reserve for it (percent units,
    /// matching [`decide_basis_trade`]'s expectation) -- `None` if
    /// neither Solend nor Kamino has one, or the ones that do haven't
    /// reported an account update yet. Used both for the borrow-hedge
    /// profitability signal (cheapest borrow = best chance of clearing
    /// the funding-collected bar) and, once borrow-hedge is chosen, to
    /// know which protocol to actually borrow from.
    fn best_borrow_apy(&self, symbol: &str) -> Option<(LendingProtocol, f64)> {
        let mint = resolve_symbol_mint(symbol)?;
        let dex = self.state.o_dex.as_ref()?;
        let solend_apy = dex
            .solend()
            .reserve_by_mint(mint)
            .map(|(_, r)| r.current_borrow_apy() * 100.0);
        let kamino_apy = dex
            .kamino()
            .reserve_by_mint(mint)
            .map(|(_, r)| r.current_borrow_apy() * 100.0);
        match (solend_apy, kamino_apy) {
            (Some(s), Some(k)) if k < s => Some((LendingProtocol::Kamino, k)),
            (Some(s), _) => Some((LendingProtocol::Solend, s)),
            (None, Some(k)) => Some((LendingProtocol::Kamino, k)),
            (None, None) => None,
        }
    }

    /// Highest real supply APY for `symbol` across every lending protocol
    /// with a tracked, priced reserve for it -- used once deposit-hedge
    /// is already chosen (depositing always helps regardless of
    /// protocol, so this isn't part of the open/close signal, only which
    /// protocol to actually deposit into).
    fn best_supply_apy(&self, symbol: &str) -> Option<(LendingProtocol, f64)> {
        let mint = resolve_symbol_mint(symbol)?;
        let dex = self.state.o_dex.as_ref()?;
        let solend_apy = dex
            .solend()
            .reserve_by_mint(mint)
            .map(|(_, r)| r.current_supply_apy() * 100.0);
        let kamino_apy = dex
            .kamino()
            .reserve_by_mint(mint)
            .map(|(_, r)| r.current_supply_apy() * 100.0);
        match (solend_apy, kamino_apy) {
            (Some(s), Some(k)) if k > s => Some((LendingProtocol::Kamino, k)),
            (Some(s), _) => Some((LendingProtocol::Solend, s)),
            (None, Some(k)) => Some((LendingProtocol::Kamino, k)),
            (None, None) => None,
        }
    }

    /// [`Self::best_borrow_apy`]/[`Self::best_supply_apy`]'s USDC-specific
    /// counterpart -- those two are keyed by a curated perp symbol
    /// (`resolve_symbol_mint`), but USDC isn't one of the 6 entries in
    /// `SYMBOL_MINT_MAP`, so idle-USDC deployment ([`Self::deploy_idle_usdc`])
    /// needs its own lookup using `self.configuration.mint_usdc` directly.
    fn best_usdc_supply_apy(&self) -> Option<(LendingProtocol, f64)> {
        let mint_usdc = self.configuration.mint_usdc;
        let dex = self.state.o_dex.as_ref()?;
        let solend_apy = dex
            .solend()
            .reserve_by_mint(mint_usdc)
            .map(|(_, r)| r.current_supply_apy() * 100.0);
        let kamino_apy = dex
            .kamino()
            .reserve_by_mint(mint_usdc)
            .map(|(_, r)| r.current_supply_apy() * 100.0);
        match (solend_apy, kamino_apy) {
            (Some(s), Some(k)) if k > s => Some((LendingProtocol::Kamino, k)),
            (Some(s), _) => Some((LendingProtocol::Solend, s)),
            (None, Some(k)) => Some((LendingProtocol::Kamino, k)),
            (None, None) => None,
        }
    }

    /// [`Self::best_usdc_supply_apy`]'s borrow-side counterpart -- the
    /// debt cost side of an LST-collateral leverage loop (deposit LST,
    /// borrow USDC), used only by [`Self::log_lst_loop_projection`]'s
    /// read-only projection. Same Solend/Kamino cheapest-wins selection
    /// as [`Self::best_borrow_apy`], just keyed by `mint_usdc` directly
    /// instead of a curated symbol.
    fn best_usdc_borrow_apy(&self) -> Option<(LendingProtocol, f64)> {
        let mint_usdc = self.configuration.mint_usdc;
        let dex = self.state.o_dex.as_ref()?;
        let solend_apy = dex
            .solend()
            .reserve_by_mint(mint_usdc)
            .map(|(_, r)| r.current_borrow_apy() * 100.0);
        let kamino_apy = dex
            .kamino()
            .reserve_by_mint(mint_usdc)
            .map(|(_, r)| r.current_borrow_apy() * 100.0);
        match (solend_apy, kamino_apy) {
            (Some(s), Some(k)) if k < s => Some((LendingProtocol::Kamino, k)),
            (Some(s), _) => Some((LendingProtocol::Solend, s)),
            (None, Some(k)) => Some((LendingProtocol::Kamino, k)),
            (None, None) => None,
        }
    }

    /// Phase 1 of `leveraged_yield_farming_plan.md`: read-only, no
    /// transactions sent. Logs what an LST-collateral/USDC-debt leverage
    /// loop would return at a few candidate leverage levels, using
    /// `net_apy(L) = L*lst_staking_apy - (L-1)*usdc_borrow_apy` (the
    /// plan's own formula -- LST *lending* supply APR is omitted, not
    /// approximated as zero: it's confirmed near-zero in the plan's
    /// live-checked numbers, and this bot doesn't currently track
    /// jitoSOL/bSOL/mSOL Kamino reserves at all, so there's nothing to
    /// read even if it mattered). No-ops for a symbol until a real
    /// `LstApy` update has arrived for it and a real USDC borrow rate is
    /// available -- never fabricates either input.
    fn log_lst_loop_projection(&self, symbol: &str, staking_apy_fraction: f64) {
        let Some((protocol, usdc_borrow_apy_pct)) = self.best_usdc_borrow_apy() else {
            return;
        };
        let staking_apy_pct = staking_apy_fraction * 100.0;
        let levels = [1.0, 2.0, 3.0];
        let projections: Vec<String> = levels
            .iter()
            .map(|&l| {
                let net_apy = l * staking_apy_pct - (l - 1.0) * usdc_borrow_apy_pct;
                format!("{l:.1}x={net_apy:.2}%")
            })
            .collect();
        log_warn!(
            "LST loop projection {symbol}: staking_apy={staking_apy_pct:.3}% usdc_borrow_apy={usdc_borrow_apy_pct:.3}% ({protocol:?}) -> {}",
            projections.join(" "),
        );
    }

    /// Real-transaction smoke test entry point -- see [`TestPhase`]'s doc
    /// comment for the full state machine this drives through. Replaces
    /// `::evaluate`'s Phoenix/epoch/basis-trade logic
    /// entirely: this mode never touches Phoenix or the funding-rate
    /// router, only Solend/Kamino/marginfi deposit+withdraw.
    pub(crate) fn evaluate(&mut self) {
        let t0 = std::time::Instant::now();
        self.evaluate_inner();
        // Accumulated (not logged here) -- same reasoning as
        // `low_latency()`'s own accumulate-don't-log comment: this fires
        // after every event, so logging every call would reintroduce the
        // log-volume problem already found to contribute to real
        // `stdio timeout` disconnects. `start()` logs and resets this.
        self.state.evaluate_elapsed_since_last_start += t0.elapsed();
        self.state.evaluate_count_since_last_start += 1;
    }

    fn evaluate_inner(&mut self) {
        self.wallet.set_priority_fee(PriorityLevel::Medium);
        // Idempotent/self-latching (2026-08-28): only actually queues the
        // real create-nonce transaction once (Wallet::ensure_bundler_nonce_created
        // no-ops on every call after the first, whether still unconfirmed
        // or already Ready) -- safe to call unconditionally every tick so
        // the durable-nonce account this wallet's Astralane landing path
        // (Wallet::send_bundler_pair, driven by set_priority_fee(High) --
        // see its own doc comment) needs is bootstrapped automatically at
        // wallet load time instead of requiring a manual trigger.
        if let Some(owner) = self.state.wallet() {
            self.wallet.ensure_bundler_nonce_created(owner);
        }
        if self.state.o_dex.is_none() {
            return;
        }

        // Drains whatever this evaluate() call (or `on_message`'s Wallet
        // arm) built onto self.wallet and actually sends it -- same tail
        // arbv1::state::evaluate/::evaluate both use.
        // Without this, queued instructions would sit on the wallet
        // forever and never reach the chain.
        for (sig, result) in self.wallet.drain_and_send() {
            match result {
                Ok(_) => {
                    log_warn!("sent transaction {sig}");
                }
                Err(e) => log_error!("failed to send transaction {sig}: {e}"),
            }
        }
    }

    /// Minimum slots to wait between an action and either retrying it
    /// (bootstrap/deposit) or advancing past it (withdraw) -- generous
    /// enough for a real transaction to land and confirm (real
    /// confirmations observed manually this session landed within a few
    /// seconds; this is deliberately several times that). `evaluate()`
    /// fires on every event, which can be many times per second, so this
    /// is what stops that from spamming duplicate transactions.
    const TEST_ACTION_COOLDOWN_SLOTS: Slot = 100;
    /// Deliberately small -- this only needs to prove the code path
    /// works, not move meaningful capital. Matches the conservative end
    /// of what was manually tested for real this session (5 USDC).
    const TEST_AMOUNT_USD: f64 = 1.0;
    /// How often (in slots) to report this wallet's most-referenced
    /// accounts back to the optimizer via `MessageSend::
    /// CommonAddressUpdate`, so `optimizer alt` can rank real Address
    /// Lookup Table candidates without an RPC history scan.
    const ACCOUNT_USAGE_REPORT_INTERVAL_SLOTS: Slot = 500;
    /// Caps each report comfortably under `MESSAGE_MAX_SIZE` (4096
    /// bytes): `28 + 100*36 = 3628`.
    const ACCOUNT_USAGE_REPORT_MAX_ENTRIES: usize = 100;

    /// `true` while still within the cooldown window of the current
    /// phase's last action -- see [`Self::TEST_ACTION_COOLDOWN_SLOTS`].
    fn test_cooldown_active(&self) -> bool {
        match self.state.last_action_slot {
            Some(last) => {
                self.state.last_slot.saturating_sub(last) < Self::TEST_ACTION_COOLDOWN_SLOTS
            }
            None => false,
        }
    }

    /// Records that the current phase just sent a real action, starting
    /// its cooldown.
    fn test_mark_action(&mut self) {
        self.state.last_action_slot = Some(self.state.last_slot);
    }

    /// Native lamports wrapped into wSOL and swapped to USDC per attempt
    /// -- deliberately small like [`Self::TEST_AMOUNT_USD`], just enough
    /// (at any plausible SOL price) to cover both protocols' $1 deposits
    /// plus routing slippage, while leaving the rest of the child
    /// wallet's SOL for transaction fees and obligation/collateral
    /// account rent across the whole remaining sequence.
    const TEST_WRAP_SOL_LAMPORTS: u64 = 50_000_000;
    /// Lamports the child wallet must keep unwrapped as a fee/rent
    /// buffer -- checked before wrapping so a low balance skips the
    /// swap (and logs why) instead of leaving the wallet unable to pay
    /// for its own transactions.
    const TEST_SOL_FEE_RESERVE_LAMPORTS: u64 = 10_000_000;

    fn solend_usdc_reserve_id(&self) -> Option<AccountId> {
        let mint_usdc = self.configuration.mint_usdc;
        let dex = self.state.o_dex.as_ref()?;
        dex.solend().reserve_by_mint(mint_usdc).map(|(id, _)| id)
    }

    fn kamino_usdc_reserve_id(&self) -> Option<AccountId> {
        let mint_usdc = self.configuration.mint_usdc;
        let dex = self.state.o_dex.as_ref()?;
        dex.kamino().reserve_by_mint(mint_usdc).map(|(id, _)| id)
    }

    fn marginfi_usdc_bank_id(&self) -> Option<AccountId> {
        let mint_usdc = self.configuration.mint_usdc;
        let dex = self.state.o_dex.as_ref()?;
        dex.marginfi().reserve_by_mint(mint_usdc).map(|(id, _)| id)
    }

    fn solend_sol_reserve_id(&self) -> Option<AccountId> {
        let mint_sol = self.configuration.mint_sol;
        let dex = self.state.o_dex.as_ref()?;
        dex.solend().reserve_by_mint(mint_sol).map(|(id, _)| id)
    }

    fn kamino_sol_reserve_id(&self) -> Option<AccountId> {
        let mint_sol = self.configuration.mint_sol;
        let dex = self.state.o_dex.as_ref()?;
        dex.kamino().reserve_by_mint(mint_sol).map(|(id, _)| id)
    }

    fn marginfi_sol_bank_id(&self) -> Option<AccountId> {
        let mint_sol = self.configuration.mint_sol;
        let dex = self.state.o_dex.as_ref()?;
        dex.marginfi().reserve_by_mint(mint_sol).map(|(id, _)| id)
    }

    /// Build and send a single spot swap leg (`mint_in` -> `mint_out`,
    /// `amount_in` raw units) through `TradeRouter`/`DexState`, onto the
    /// real `self.wallet` (not a scratch dry-run, unlike
    /// `arbv1::build_execution_plan`) -- built instructions are picked
    /// up by `evaluate()`'s `assemble()`/send loop above.
    ///
    /// This is a general-purpose hook, not a strategy of its own -- it
    /// was built so a future decision could be wired in without first
    /// re-deriving this plumbing, and `rebalance_portfolio` below is
    /// now that decision (portfolio-target rebalancing); it stays
    /// available for others too (collateral top-up, a cash-and-carry
    /// spot leg against a Phoenix/Velocity funding edge, etc.). Same
    /// untestable-outside-the-WASM-guest-runtime boundary
    /// as `build_execution_plan` (see that method's doc comment): every
    /// `Wallet`/`account_id_from_pubkey`-touching call here transitively
    /// hits a WIT host import, so this has no native `cargo test`
    /// coverage by design, matching the rest of this codebase's
    /// `Wallet`-touching code.
    pub(crate) fn execute_spot_leg(
        &mut self,
        mint_in: AccountId,
        mint_out: AccountId,
        amount_in: u64,
    ) -> Result<(), String> {
        let Some(owner) = self.state.wallet() else {
            return Err("no wallet keypair yet".to_string());
        };
        let Some(dex) = self.state.o_dex.as_ref() else {
            return Err("dex state not ready".to_string());
        };
        const MAX_HOPS: usize = 4;
        self.state
            .spot_router
            .set_current_slot(self.state.last_slot);
        let Some(route) = self
            .state
            .spot_router
            .route_slippage_aware(mint_in, mint_out, amount_in, MAX_HOPS)
        else {
            log_error!(
                "route diagnostics for {mint_in} -> {mint_out}:\n{}",
                self.state
                    .spot_router
                    .route_diagnostics(mint_in, mint_out, amount_in, MAX_HOPS)
            );
            return Err(format!(
                "no route found for {mint_in} -> {mint_out} amount_in={amount_in}"
            ));
        };
        let route = match planner::reverify_route_with_exact_quotes(
            &route,
            amount_in,
            &self.state.spot_router,
            dex,
        ) {
            Ok(route) => route,
            Err(failure) => {
                if failure.coolable {
                    self.state
                        .spot_router
                        .mark_pool_cooldown(failure.pool_id, planner::POOL_COOLDOWN_SLOTS);
                    return Err(format!(
                        "exact quote invalidated pool {} (cooling down {} slots)",
                        failure.pool_id,
                        planner::POOL_COOLDOWN_SLOTS,
                    ));
                }
                return Err(format!(
                    "pool {} isn't ready to quote yet (tick-array data still syncing) -- try again shortly",
                    failure.pool_id,
                ));
            }
        };

        log_warn!(
            ": spot leg @ slot {}: {} hop{} {} -> {} amount_in={}",
            self.state.last_slot,
            route.hops.len(),
            if route.hops.len() == 1 { "" } else { "s" },
            mint_in,
            mint_out,
            amount_in,
        );
        for (i, hop) in route.hops.iter().enumerate() {
            // `append_create_ata` (not `derive_ata`) -- a hop's
            // intermediate mint (unlike the route's overall input/output,
            // which are almost always mints the wallet already holds) may
            // never have been touched by this wallet before, so its ATA
            // may not exist yet. `CreateIdempotent` is a safe no-op when
            // it already does. Live-confirmed this session: a
            // marginfi borrow-hedge sell routed through an unfamiliar
            // intermediate mint and reverted on-chain with
            // `AccountNotInitialized` because only the address was
            // derived, never actually created.
            // Glue this hop's ATA-creation + swap instructions together --
            // see `Wallet::begin_atomic_group`'s doc comment for the real,
            // live-confirmed `AccountNotInitialized` bug this prevents
            // (assemble()'s size-based splitter previously could, and
            // did, send a hop's swap in a different, unordered
            // transaction than its own destination-ATA-creation
            // instruction).
            self.wallet.begin_atomic_group();
            let (Some(source_ata), Some(dest_ata)) = (
                self.wallet.append_create_ata(owner, hop.input_mint),
                self.wallet.append_create_ata(owner, hop.output_mint),
            ) else {
                self.wallet.end_atomic_group();
                return Err(format!(
                    "hop {i}: FAILED to derive token account(s) for owner={owner}"
                ));
            };
            let hop_result = dex.execute_hop(hop, owner, source_ata, dest_ata, self.wallet);
            self.wallet.end_atomic_group();
            match hop_result {
                Ok(()) => {
                    log_warn!(
                        "  hop {i}: OK dex={:?} pool={} {} -> {} amount_in={} amount_out={}",
                        hop.dex,
                        hop.pool_id,
                        hop.input_mint,
                        hop.output_mint,
                        hop.amount_in,
                        hop.amount_out,
                    );
                }
                Err(e) => {
                    return Err(format!(
                        "hop {i}: FAILED dex={:?} pool={} {} -> {}: {}",
                        hop.dex, hop.pool_id, hop.input_mint, hop.output_mint, e,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Current USDC balance, valued at $1 (no price oracle -- USDC is
    /// assumed pegged, same convention `build.rs` and every other USDC
    /// valuation in this file already use). `0.0` if there's no wallet
    /// keypair yet, matching the rest of this file's precondition
    /// style. Shared by `rebalance_portfolio` (values the whole
    /// portfolio) and `log_funding_graph_cycles`'s capital-feasibility
    /// selection (how much spare USDC is available to margin a funding
    /// cycle) -- same live `TokenDatabase` lookup, not two data
    /// sources that could disagree.
    fn current_usdc_value(&mut self) -> f64 {
        let Some(owner) = self.state.wallet() else {
            return 0.0;
        };
        const USDC_DECIMALS: i32 = 6;
        let mint_usdc = self.configuration.mint_usdc;
        let usdc_balance_raw: u64 = self
            .wallet
            .token_mut()
            .balance(&owner, &mint_usdc, true)
            .iter()
            .map(|(_, a)| *a)
            .sum();
        usdc_balance_raw as f64 / 10f64.powi(USDC_DECIMALS)
    }

    /// Full-portfolio rebalance toward `target_allocation_pct`: values
    /// current holdings (each tracked asset + implicit USDC) in USD via
    /// `spot_router.route_slippage_aware` (the same per-asset quoting
    /// `log_spot_price_probe` above already uses -- `TradeRouter` has
    /// no bulk "value my whole wallet" API), computes each asset's
    /// delta against `allocation_pct * total_value`
    /// (`plan_rebalance_legs`), then executes every sell before every
    /// buy via `execute_spot_leg` (sells free the USDC buys need to
    /// spend). Triggered by every `CustomMessageInbound::
    /// TargetAllocation` -- a *full* portfolio rebalance, not just the
    /// one symbol that changed, since the implicit USDC remainder is
    /// defined as `1.0` minus every tracked asset's `allocation_pct`:
    /// changing one asset's target always implicitly changes every
    /// other asset's effective target too. Buy sizing reserves
    /// `FUNDING_CYCLE_MIN_MARGIN_USD` off the top before spending
    /// anything on directional purchases -- see `plan_rebalance_legs`'s
    /// doc comment for why.
    pub(crate) fn rebalance_portfolio(&mut self) {
        let Some(owner) = self.state.wallet() else {
            log_warn!(": rebalance skipped -- no wallet keypair yet");
            return;
        };
        const MAX_HOPS: usize = 4;
        const USDC_DECIMALS: i32 = 6;
        let mint_usdc = self.configuration.mint_usdc;
        let usdc_value = self.current_usdc_value();

        self.state
            .spot_router
            .set_current_slot(self.state.last_slot);

        // Snapshot (symbol, account_id, target_pct) first -- avoids
        // holding an immutable borrow of self.state across the mutable
        // self.wallet/self.state.spot_router calls in the loop below.
        let entries: Vec<(String, AccountId, f64)> = self
            .state
            .target_allocation_pct
            .iter()
            .filter_map(|(symbol, entry)| {
                entry
                    .account_id
                    .map(|id| (symbol.clone(), id, entry.allocation_pct))
            })
            .collect();

        let mut holdings: Vec<(String, AssetHolding)> = Vec::new();
        let mut total_value = usdc_value;
        for (symbol, account_id, target_pct) in entries {
            // No SOL special-case: `Wallet::balance_sol` (native
            // lamports) is deliberately unused for trading-relevant
            // balance anywhere in this codebase -- pools only ever
            // trade *wrapped* SOL as an SPL mint, so `TokenDatabase::
            // balance` against the wSOL mint (which `account_id`
            // already resolves to) is the correct, uniform query for
            // every tracked asset including SOL. Matches
            // `planner::find_opportunity`'s own explicit doc comment on
            // this exact question. Un-wrapped native SOL sitting in the
            // wallet is real value this bot can't act on without a
            // wrap step this codebase doesn't have -- correctly
            // excluded, not a bug.
            let balance_raw: u64 = self
                .wallet
                .token_mut()
                .balance(&owner, &account_id, true)
                .iter()
                .map(|(_, a)| *a)
                .sum();
            // A zero balance needs no quote -- it's worth $0 regardless
            // of whether a route exists. A *nonzero* balance with no
            // route found is excluded from both the total and this
            // pass's trading (logged below), not silently treated as
            // $0 -- that would wrongly inflate its "needs buying"
            // signal.
            let value_usd = if balance_raw == 0 {
                0.0
            } else {
                match self.state.spot_router.route_slippage_aware(
                    account_id,
                    mint_usdc,
                    balance_raw,
                    MAX_HOPS,
                ) {
                    Some(route) => route.amount_out() as f64 / 10f64.powi(USDC_DECIMALS),
                    None => {
                        log_error!(
                            ": rebalance: no route to value {} ({}), skipping this pass",
                            symbol,
                            account_id,
                        );
                        continue;
                    }
                }
            };
            let decimals = resolve_symbol_decimals(&symbol).unwrap_or(0);
            log_warn!(
                ": rebalance holding {}: balance={:.9} (raw {}) value=${:.2} target_pct={:.4}",
                symbol,
                balance_raw as f64 / 10f64.powi(decimals as i32),
                balance_raw,
                value_usd,
                target_pct,
            );
            total_value += value_usd;
            holdings.push((
                symbol,
                AssetHolding {
                    account_id,
                    balance_raw,
                    value_usd,
                    target_pct,
                },
            ));
        }

        log_warn!(
            ": rebalance @ slot {}: total portfolio value=${:.2} (usdc=${:.2}, {} priced asset{})",
            self.state.last_slot,
            total_value,
            usdc_value,
            holdings.len(),
            if holdings.len() == 1 { "" } else { "s" },
        );

        let (sells, buys, scale) = plan_rebalance_legs(
            &holdings,
            total_value,
            usdc_value,
            FUNDING_CYCLE_MIN_MARGIN_USD,
        );
        if scale < 1.0 {
            log_warn!(
                ": rebalance: desired buys exceed available capital -- scaled to {:.4} \
                 (reserving ${:.2} for funding-arb margin)",
                scale,
                FUNDING_CYCLE_MIN_MARGIN_USD,
            );
        }
        for (symbol, account_id, amount_in) in sells {
            log_warn!(": rebalance SELL {} amount_in={}", symbol, amount_in);
            if let Err(e) = self.execute_spot_leg(account_id, mint_usdc, amount_in) {
                log_error!(": rebalance SELL {} failed: {}", symbol, e);
            }
        }
        for (symbol, account_id, amount_in) in buys {
            log_warn!(": rebalance BUY {} amount_in={}", symbol, amount_in);
            if let Err(e) = self.execute_spot_leg(mint_usdc, account_id, amount_in) {
                log_error!(": rebalance BUY {} failed: {}", symbol, e);
            }
        }
    }
}

/// Minimum `|delta_usd|` a rebalance leg must clear to be worth trading
/// -- a deliberately simple, tunable placeholder to avoid dust trades
/// from float noise or a few cents of drift, not derived from any
/// cost-of-trading analysis.
const REBALANCE_DUST_THRESHOLD_USD: f64 = 1.0;

/// One asset's valuation snapshot going into `plan_rebalance_legs` --
/// deliberately holds only plain data (no `Wallet`/`TradeRouter`
/// references), so the actual buy/sell-sizing math is natively
/// testable even though *gathering* these values (real wallet
/// balances, real route quotes) needs the live WASM guest runtime.
#[derive(Debug, Clone, Copy)]
struct AssetHolding {
    account_id: AccountId,
    balance_raw: u64,
    value_usd: f64,
    target_pct: f64,
}

/// Pure planning step (no host-import dependency, natively testable):
/// for each `(symbol, holding)` compares `holding.value_usd` to
/// `holding.target_pct * total_value`, skips deltas under
/// `REBALANCE_DUST_THRESHOLD_USD`, and returns sells and buys as two
/// separately ordered lists (both sorted by symbol for determinism --
/// `holdings` itself has no defined order). Sell amounts are sized as a
/// proportional fraction of the current raw balance, reusing this
/// pass's own valuation quote as an implied per-unit price -- an
/// approximation, not an exact-price computation;
/// `execute_spot_leg`'s own `reverify_route_with_exact_quotes` still
/// re-checks the real price before actually sending anything, so this
/// only affects the requested trade *size*, not the executed price.
///
/// Two passes: sells are sized and returned immediately (selling only
/// ever frees USDC, so nothing constrains it), while buys are collected
/// as desired USD amounts first, then capped in a second pass against
/// `usdc_value + sell_proceeds - capital_reserve_usd` -- what's
/// actually going to be on hand after this pass's sells settle, minus a
/// standing floor (`capital_reserve_usd`, e.g.
/// `FUNDING_CYCLE_MIN_MARGIN_USD` -- shared with
/// `select_capital_feasible_cycles` so the two systems don't both treat
/// the same dollar as theirs to spend) that's reserved off the top
/// regardless of which system runs first. If total desired buys exceed
/// what's available, every buy is scaled down by the same ratio --
/// proportional, not first-come-first-served, so capital scarcity
/// doesn't arbitrarily favor whichever symbol sorts first. The third
/// return value is that scale factor (`1.0` when every desired buy fit
/// without scaling), so a caller can log when a capital shortfall
/// actually bit.
fn plan_rebalance_legs(
    holdings: &[(String, AssetHolding)],
    total_value: f64,
    usdc_value: f64,
    capital_reserve_usd: f64,
) -> (
    Vec<(String, AccountId, u64)>,
    Vec<(String, AccountId, u64)>,
    f64,
) {
    const USDC_DECIMALS: i32 = 6;
    let mut sorted: Vec<&(String, AssetHolding)> = holdings.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut sells = Vec::new();
    let mut desired_buys: Vec<(String, AccountId, f64)> = Vec::new();
    let mut sell_proceeds_usd = 0.0;
    for (symbol, holding) in sorted {
        let target_usd = holding.target_pct * total_value;
        let delta_usd = target_usd - holding.value_usd;
        if delta_usd.abs() < REBALANCE_DUST_THRESHOLD_USD {
            continue;
        }
        if delta_usd > 0.0 {
            desired_buys.push((symbol.clone(), holding.account_id, delta_usd));
        } else if holding.value_usd > 0.0 {
            let sell_fraction = (delta_usd.abs() / holding.value_usd).min(1.0);
            let amount_in = (holding.balance_raw as f64 * sell_fraction).round() as u64;
            if amount_in > 0 {
                sells.push((symbol.clone(), holding.account_id, amount_in));
                sell_proceeds_usd += delta_usd.abs();
            }
        }
    }

    let available_for_buys = (usdc_value + sell_proceeds_usd - capital_reserve_usd).max(0.0);
    let total_desired_usd: f64 = desired_buys.iter().map(|(_, _, d)| *d).sum();
    let scale = if total_desired_usd > available_for_buys && total_desired_usd > 0.0 {
        available_for_buys / total_desired_usd
    } else {
        1.0
    };
    let mut buys = Vec::new();
    for (symbol, account_id, delta_usd) in desired_buys {
        let amount_in = ((delta_usd * scale) * 10f64.powi(USDC_DECIMALS)).round() as u64;
        if amount_in > 0 {
            buys.push((symbol, account_id, amount_in));
        }
    }
    (sells, buys, scale)
}

/// Assumed capital needed to margin one funding-arb cycle (both legs
/// combined) -- a deliberately simple, tunable placeholder, not derived
/// from real per-venue margin requirements (Phoenix/Velocity margin
/// ratios aren't modeled anywhere in this bot yet). Same
/// "flag the simplification, don't hide it" discipline as
/// `REBALANCE_DUST_THRESHOLD_USD`.
/// Assumed capital needed to margin one basis-trade cycle (both legs
/// combined) -- a deliberately simple, tunable placeholder, not derived
/// from real per-venue margin requirements. Same "flag the
/// simplification, don't hide it" discipline as
/// `REBALANCE_DUST_THRESHOLD_USD`.
const FUNDING_CYCLE_MIN_MARGIN_USD: f64 = 10.0;

/// Which side of the basis trade is profitable for a symbol right now.
/// See [`decide_basis_trade`]'s doc comment for the real economics of
/// each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BasisDirection {
    /// Funding positive (longs pay shorts on Phoenix): short the perp,
    /// hedge with a lending-protocol deposit of the underlying (no
    /// borrowing).
    DepositHedge,
    /// Funding negative (shorts pay longs): long the perp, hedge with a
    /// lending-protocol borrow of the underlying, sold for USDC
    /// (synthetic short).
    BorrowHedge,
}

/// Which lending protocol backs a basis-trade hedge leg -- Solend and
/// Kamino are the only two this bot uses (marginfi deferred, see this
/// module's doc comment: its cached prefetch data looked stale and a live
/// re-fetch needs infrastructure this environment doesn't have configured
/// yet).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LendingProtocol {
    Solend,
    Kamino,
}

/// Pure (no host-import dependency, natively testable): decides which
/// side of the Phoenix-perp-funding-vs-lending-rate basis trade is
/// profitable for a symbol, given this epoch's Phoenix funding rate and
/// the cheapest real borrow APY available across every lending protocol
/// with a reserve for this symbol (both **percent** units --
/// `SolendReserve`/`KaminoReserve::current_borrow_apy()` each return a
/// 0.0-1.0 fraction, multiply by 100 before calling this -- see
/// [`StateHelper::best_borrow_apy`]).
///
/// - `phoenix_funding_pct > 0.0` (longs pay shorts): short the perp,
///   deposit the underlying on whichever protocol pays the best supply
///   APY to stay delta-neutral -- this *adds* the deposit's yield on top
///   of the captured funding, so it's worth doing whenever funding is
///   positive at all, no threshold against any lending rate needed
///   (depositing never costs anything, only ever earns).
/// - `phoenix_funding_pct < 0.0` (shorts pay longs): long the perp,
///   borrow the underlying and sell it for a synthetic short hedge --
///   only profitable if the funding collected exceeds the real interest
///   paid to borrow, i.e. `-phoenix_funding_pct > borrow_apy_pct`.
/// - Otherwise (funding is exactly zero, or negative but not enough to
///   clear the borrow cost): `None`, no capturable edge.
fn decide_basis_trade(phoenix_funding_pct: f64, borrow_apy_pct: f64) -> Option<BasisDirection> {
    if phoenix_funding_pct > 0.0 {
        Some(BasisDirection::DepositHedge)
    } else if phoenix_funding_pct < 0.0 && -phoenix_funding_pct > borrow_apy_pct {
        Some(BasisDirection::BorrowHedge)
    } else {
        None
    }
}

impl<'a> InboundMesasgeHandler<Configuration, CustomMessageInbound, CustomMessageOutbound>
    for StateHelper<'a>
{
    fn on_message(&mut self, action: MessageAction<Configuration, CustomMessageInbound>) {
        match action {
            MessageAction::Ping(_) => {
                self.q_msg
                    .push_back(MessageSend::Pong(std::time::SystemTime::now()));
            }
            MessageAction::AdjustConfiguration(new_configuration) => {
                unsafe { std::ptr::copy_nonoverlapping(&new_configuration, self.configuration, 1) };
            }
            MessageAction::Shutdown => panic!("shutting down"),
            MessageAction::Custom(x) => match x {
                CustomMessageInbound::Blank => {}
                CustomMessageInbound::Wallet(rc_keypair) => {
                    let keypair = rc_unlock(&rc_keypair);
                    let pubkey = keypair.pubkey();
                    let account_id = account_id_from_pubkey(&pubkey);
                    log_warn!("got wallet keypair {} {}", pubkey, account_id);
                    self.wallet
                        .append_key(rc_keypair.clone(), self.graph)
                        .unwrap();
                    self.wallet.set_payer(account_id);
                    self.configuration.set(&rc_keypair);
                    self.state.o_rc_keypair.replace(KeypairExtra {
                        rc_keypair,
                        account_id,
                    });
                    // First point in this file's message flow confirmed
                    // to be inside the live WASM guest (mirrors
                    // Configuration::set's own mint_sol/mint_usdc
                    // resolution immediately above) -- safe to resolve
                    // the build-time-default target allocation entries'
                    // mints now.
                    self.state.resolve_target_allocation_mints();
                    // This wallet's own SPL token accounts (USDC, wSOL,
                    // every currently-tracked target-allocation mint) --
                    // batched into the same call for the same reason as
                    // the four venues above. Real, live-observed
                    // motivation: before this, nothing in this codebase
                    // ever subscribed to a wallet's own ATAs at all (only
                    // its native SOL account, via `append_key`) --
                    // confirmed live via `solana confirm -v`: a real swap
                    // succeeded on-chain (22+ USDC landed in the wallet's
                    // real ATA) while `current_usdc_value()` stayed at 0
                    // the entire time, since no `on_token`/low-latency
                    // update for that ATA was ever received. See
                    // `Wallet::l_ata_sub`'s own doc comment.
                    let mut hs_ata_mints: std::collections::HashSet<AccountId> =
                        std::collections::HashSet::new();
                    hs_ata_mints.insert(self.configuration.mint_usdc);
                    hs_ata_mints.insert(self.configuration.mint_sol);
                    for entry in self.state.target_allocation_pct.values() {
                        if let Some(mint) = entry.account_id {
                            hs_ata_mints.insert(mint);
                        }
                    }
                    // This wallet's own durable-nonce account (see
                    // `Wallet::send_bundler_pair`'s doc comment) --
                    // batched into the same subscribe_now call as
                    // everything else above, not a separate round-trip.
                    let nonce_reqs: Vec<_> = self
                        .wallet
                        .nonce_subscribe_request(account_id)
                        .into_iter()
                        .collect();

                    let nonce_len = nonce_reqs.len();
                    let mut all_requests = Vec::with_capacity(nonce_len);
                    all_requests.extend(nonce_reqs);

                    match SubscriptionQueue::subscribe_now(self.graph, all_requests) {
                        Ok(subs) => {
                            let mut it = subs.into_iter();
                            if let Some(sub) = (&mut it).take(nonce_len).next() {
                                self.wallet.keep_nonce_subscription(sub);
                            }
                        }
                        Err(e) => {
                            log_error!("failed to batch-subscribe wallet authority accounts: {e}");
                        }
                    }
                }
                CustomMessageInbound::TargetAllocation(symbol, allocation_pct) => {
                    let account_id = resolve_symbol_mint(&symbol);
                    if account_id.is_none() {
                        log_error!(
                            "target allocation symbol {} has no curated mint -- cannot resolve to AccountId",
                            symbol,
                        );
                    }
                    log_warn!(
                        "target allocation update: {}={} ({:?})",
                        symbol,
                        allocation_pct,
                        account_id
                    );
                    self.state.target_allocation_pct.insert(
                        symbol,
                        TargetAllocationEntry {
                            account_id,
                            allocation_pct,
                        },
                    );
                    self.rebalance_portfolio();
                }
                CustomMessageInbound::LstApy(symbol, staking_apy) => {
                    log_warn!(
                        "LST staking APY update: {}={:.3}%",
                        symbol,
                        staking_apy * 100.0,
                    );
                    self.state
                        .lst_staking_apy
                        .insert(symbol.clone(), staking_apy);
                    self.log_lst_loop_projection(&symbol, staking_apy);
                }
                CustomMessageInbound::TriggerTestAstralane => {
                    let Some(owner) = self.state.wallet() else {
                        log_warn!("TriggerTestAstralane ignored -- no wallet yet");
                        return;
                    };
                    match self.wallet.test_send_astralane_tip_batch(owner) {
                        Some(Ok(())) => {
                            log_warn!(
                                "TriggerTestAstralane -- tip + self-transfer sent via transactionprocessor::batch (bundler=astralane)"
                            );
                        }
                        Some(Err(e)) => {
                            log_error!("TriggerTestAstralane -- batch failed: {e:?}");
                        }
                        None => {
                            log_warn!(
                                "TriggerTestAstralane ignored -- wallet key not loaded yet, or something else is already queued this tick"
                            );
                        }
                    }
                }
                CustomMessageInbound::CommonBundlerTipUpdate(update) => {
                    self.wallet.apply_bundler_tip_update(self.graph, update);
                }
            },
        }
    }

    fn message_send(&mut self, message: MessageSend<CustomMessageOutbound>) {
        self.q_msg.push_back(message);
    }
}

/// Target for both slot-timing diagnostics below: `finish()`'s
/// start-to-finish span, and `start()`'s gap-since-previous-start.
/// Originally set to 200ms; corrected to 400ms after live validator-side
/// data (root-slot `total=` timings gathered this session) showed real
/// root-slot intervals cluster around 230-250ms median with normal
/// spikes into the 400s -- 200ms wasn't an achievable target given
/// Solana's own real block cadence, not a guest-side problem to chase.
const SLOT_TIMING_TARGET_MS: u128 = 400;

/// Cap on how many queued subscription requests `subscription_queue`
/// sends per slot (one bounded `bulk_subscribe` call in `finish()`) --
/// keeps each slot's own blocking subscribe cost small and predictable
/// instead of one giant call for the whole ~32,000-request startup
/// burst. Real, live-observed motivation: `subscribe`/`bulk_subscribe`
/// are blocking calls on the validator side.
const MAX_SUBSCRIBES_PER_SLOT: usize = 128;

impl<'a> CommitHook for StateHelper<'a> {
    fn start(&mut self, slot: Slot) {
        assert!(self.o_commit_slot.replace(slot).is_none());
        self.state.last_slot = slot;
        let now = std::time::Instant::now();
        // Gap since the *previous* slot's start() -- covers evaluate()'s
        // own work, other event types, and idle time between commits,
        // not just this commit's own on_account/on_token/finish span
        // (see o_prev_commit_start_instant's doc comment). `None` on the
        // very first commit -- nothing to compare against yet.
        // Reset every call regardless of whether it logs below, so the
        // measurement window always means exactly "since the previous
        // start()" -- see the field's own doc comment.
        let ll_elapsed_ms =
            std::mem::take(&mut self.state.low_latency_elapsed_since_last_start).as_millis();
        let ll_count = std::mem::take(&mut self.state.low_latency_count_since_last_start);
        let ev_elapsed_ms =
            std::mem::take(&mut self.state.evaluate_elapsed_since_last_start).as_millis();
        let ev_count = std::mem::take(&mut self.state.evaluate_count_since_last_start);
        // Testing a new hypothesis: every death this session left its
        // last log line looking completely unremarkable -- no bookend
        // ever caught the freeze itself, even after every known blocking
        // call (subscribe/bulk_subscribe) was paced and bookended. A log
        // call's own internal buffer-overflow flush is a real host stdio
        // write that neither `log_warn!` nor any caller can bookend (the
        // flush happens *inside* the call trying to log something) --
        // see `stdio::take_flush_stats`'s doc comment.
        let (flush_elapsed, flush_count) = crate::stdio::take_flush_stats();
        let flush_elapsed_ms = flush_elapsed.as_millis();
        if let Some(prev) = self.state.o_prev_commit_start_instant.replace(now) {
            let gap_ms = now.duration_since(prev).as_millis();
            if gap_ms > SLOT_TIMING_TARGET_MS {
                log_warn!(
                    "{}ms since previous slot's start() (target: <{}ms) -- \
                     of that, {}ms was spent in low_latency() processing {} account/token \
                     updates, {}ms was spent across {} evaluate() calls, and {}ms was spent \
                     across {} stdio flush() calls, in the window",
                    gap_ms,
                    SLOT_TIMING_TARGET_MS,
                    ll_elapsed_ms,
                    ll_count,
                    ev_elapsed_ms,
                    ev_count,
                    flush_elapsed_ms,
                    flush_count,
                );
            }
        }
        self.state.o_commit_start_instant = Some(now);
        if slot % 100 == 0 {
            self.log_spot_price_probe();
        }
    }

    fn on_account(&mut self, header: &Header, body: &[u8]) {
        // Real, live-confirmed gap: `Wallet::on_account` (the child
        // wallet's own SOL-balance tracking, used by `balance_sol()`,
        // which `test_swap_to_usdc` depends on) was never called from
        // anywhere in this codebase -- not here, not in `low_latency`,
        // not in any other bot mode. `balance_sol()` could therefore
        // never return anything but its zero-initialized default,
        // regardless of subscription depth or how long a run waited.
        self.wallet.on_account(header, body);
        if self.is_newer_than_low_latency(header.accountid, header.slot) {
            self.record_low_latency_slot(header.accountid, header.slot);
        }
        if let Some(dex) = self.state.o_dex.as_mut() {
            dex.on_account(header, body);
            dex.refresh_account_router(header.accountid, &mut self.state.spot_router);
        }
    }

    fn on_token(&mut self, token_account: &Tokenaccountv1) {
        self.wallet.token_mut().on_token(token_account, true);
    }

    fn finish(&mut self) {
        self.o_commit_slot = None;
        self.state.slot_delta_since_start += 1;
        // Breadcrumb, throttled to every 20 slots -- `bulk_subscribe`
        // (called inside both flush calls below) is a confirmed blocking
        // host call. If the guest ever goes permanently silent (no more
        // commit/gap logs at all, as happened this session), whichever
        // flush call's own timing never got logged below is the one that
        // never returned -- this line is the last-known-position marker
        // for that case, so it needs to land *before* the risky calls,
        // not be reconstructed after the fact.
        if self.state.last_slot % 20 == 0 {
            log_warn!(
                "slot {} entering subscription flush (dex pending={:?}/active={:?})",
                self.state.last_slot,
                self.state
                    .o_dex
                    .as_ref()
                    .map(|d| d.subscription_pending_count()),
                self.state
                    .o_dex
                    .as_ref()
                    .map(|d| d.subscription_active_count()),
            );
        }
        let t_dex_flush = std::time::Instant::now();
        if let Some(mut dex) = self.state.o_dex.take() {
            if let Err(e) = dex.flush_pool(self.graph, MAX_SUBSCRIBES_PER_SLOT) {
                log_error!(": failed to flush dex subscriptions: {e}");
            }
            // Drains a bounded slice of DexState's own queued startup
            // subscription burst (~32,000 requests across every sub-dex)
            // -- same 128/slot pacing as `subscription_queue` below, just
            // a separate queue instance owned by `DexState` itself.
            if let Err(e) = dex.flush_subscriptions(self.graph, MAX_SUBSCRIBES_PER_SLOT) {
                log_error!("failed to flush dex subscription queue: {e}");
            }
            self.state.o_dex.replace(dex);
        }
        let dex_flush_ms = t_dex_flush.elapsed().as_millis();
        // Unconditional, once per commit -- same per-commit rate as
        // graph.rs's `commit:border`/`commit:done` bookends, not a new
        // log-volume source. Localizes a future hang to one specific
        // flush call: if this line is missing for a commit whose
        // `commit:done` did print, the freeze is in `dex.flush_pool`/
        // `dex.flush_subscriptions` above; if this line prints but
        // nothing ever follows, it's in `subscription_queue.flush`
        // below instead.
        log_warn!(
            "slot {} dex flush done ({dex_flush_ms}ms), entering wallet subscription_queue flush",
            self.state.last_slot,
        );
        // Drain a bounded slice of the queued startup subscription burst
        // per slot, instead of one giant blocking bulk_subscribe call --
        // see SubscriptionQueue's own doc comment for the real,
        // live-observed motivation.
        let t_queue_flush = std::time::Instant::now();
        match self
            .state
            .subscription_queue
            .flush(self.graph, MAX_SUBSCRIBES_PER_SLOT)
        {
            Ok(0) => {}
            Ok(n) => {
                log_warn!(
                    "subscription_queue flushed {n} requests ({} still pending, {} active)",
                    self.state.subscription_queue.pending_count(),
                    self.state.subscription_queue.active_count(),
                );
            }
            Err(e) => {
                log_error!("subscription_queue flush failed: {e}");
            }
        }
        let queue_flush_ms = t_queue_flush.elapsed().as_millis();
        // Unconditional, once per commit -- bookends "dex flush done" so
        // a hang inside `subscription_queue.flush` (present) vs. one
        // that happens *after* `finish()` returns entirely (absent) can
        // be told apart -- closes the last gap in `finish()` itself.
        log_warn!(
            "slot {} finish() returning (queue_flush {queue_flush_ms}ms)",
            self.state.last_slot,
        );
        // Real-time budget check -- this commit's own start()-to-here
        // span (every on_account/on_token call plus the flush work
        // above), distinct from start()'s gap-since-previous-start check.
        // Deliberately only logs when over budget, not every slot: this
        // fires on every single commit, and unconditional per-slot
        // logging is exactly the kind of volume that was already found
        // to contribute to real `stdio timeout` disconnects this session
        // (see `test_swap_to_usdc`'s doc comment on `test_mark_action`
        // for that incident).
        if let Some(start) = self.state.o_commit_start_instant.take() {
            let elapsed_ms = start.elapsed().as_millis();
            if elapsed_ms > SLOT_TIMING_TARGET_MS {
                log_warn!(
                    "slot {} took {}ms to process (target: <{}ms) -- \
                     of that, {}ms was in dex pool/subscription flush and {}ms was in \
                     wallet subscription_queue flush",
                    self.state.last_slot,
                    elapsed_ms,
                    SLOT_TIMING_TARGET_MS,
                    dex_flush_ms,
                    queue_flush_ms,
                );
            }
        }
        // Periodically report this wallet's most-referenced accounts back
        // to the optimizer -- see Self::ACCOUNT_USAGE_REPORT_INTERVAL_SLOTS.
        if self.state.last_slot % Self::ACCOUNT_USAGE_REPORT_INTERVAL_SLOTS == 0 {
            let top = self
                .wallet
                .top_account_usage(Self::ACCOUNT_USAGE_REPORT_MAX_ENTRIES);
            if !top.is_empty() {
                self.q_msg.push_back(MessageSend::CommonAddressUpdate(top));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holding(
        symbol: &str,
        account_id: AccountId,
        balance_raw: u64,
        value_usd: f64,
        target_pct: f64,
    ) -> (String, AssetHolding) {
        (
            symbol.to_string(),
            AssetHolding {
                account_id,
                balance_raw,
                value_usd,
                target_pct,
            },
        )
    }

    #[test]
    fn plan_rebalance_legs_buys_when_underweight() {
        // $1000 total, SOL target 30% ($300), currently worth $100 --
        // needs a $200 buy.
        let holdings = vec![holding("SOL", 1, 1_000_000_000, 100.0, 0.30)];
        let (sells, buys, scale) = plan_rebalance_legs(&holdings, 1000.0, 1_000_000.0, 0.0);

        assert!(sells.is_empty());
        assert_eq!(scale, 1.0, "ample capital -- no scaling expected");
        assert_eq!(buys.len(), 1);
        let (symbol, account_id, amount_in) = &buys[0];
        assert_eq!(symbol, "SOL");
        assert_eq!(*account_id, 1);
        assert_eq!(*amount_in, 200_000_000); // $200 -> raw USDC (1e6 scale)
    }

    #[test]
    fn plan_rebalance_legs_sells_when_overweight() {
        // $1000 total, BTC target 10% ($100), currently worth $250 --
        // needs to sell 60% of the current raw balance ($150 / $250).
        let holdings = vec![holding("BTC", 2, 1_000_000, 250.0, 0.10)];
        let (sells, buys, _) = plan_rebalance_legs(&holdings, 1000.0, 1_000_000.0, 0.0);

        assert!(buys.is_empty());
        assert_eq!(sells.len(), 1);
        let (symbol, account_id, amount_in) = &sells[0];
        assert_eq!(symbol, "BTC");
        assert_eq!(*account_id, 2);
        assert_eq!(*amount_in, 600_000); // 60% of 1_000_000 raw
    }

    #[test]
    fn plan_rebalance_legs_skips_deltas_under_dust_threshold() {
        // $1000 total, ETH target 10% ($100), currently worth $100.50 --
        // $0.50 delta, below the $1 dust threshold.
        let holdings = vec![holding("ETH", 3, 500_000, 100.50, 0.10)];
        let (sells, buys, _) = plan_rebalance_legs(&holdings, 1000.0, 1_000_000.0, 0.0);

        assert!(
            sells.is_empty(),
            "expected no sell leg for a sub-dust delta: {sells:?}"
        );
        assert!(
            buys.is_empty(),
            "expected no buy leg for a sub-dust delta: {buys:?}"
        );
    }

    #[test]
    fn plan_rebalance_legs_sorts_by_symbol_for_determinism() {
        // All three underweight (all buys) -- HashMap iteration order is
        // arbitrary, so feed them in reverse-alphabetical order and
        // confirm the output is still alphabetical.
        let holdings = vec![
            holding("XRP", 3, 0, 0.0, 0.10),
            holding("ETH", 2, 0, 0.0, 0.10),
            holding("BTC", 1, 0, 0.0, 0.10),
        ];
        let (_, buys, _) = plan_rebalance_legs(&holdings, 1000.0, 1_000_000.0, 0.0);

        let symbols: Vec<&str> = buys.iter().map(|(s, _, _)| s.as_str()).collect();
        assert_eq!(symbols, vec!["BTC", "ETH", "XRP"]);
    }

    #[test]
    fn plan_rebalance_legs_produces_sells_and_buys_together() {
        // SOL overweight (sell), BTC underweight (buy), in one pass.
        let holdings = vec![
            holding("SOL", 1, 1_000_000_000, 400.0, 0.10), // target $100, sell $300 worth
            holding("BTC", 2, 1_000_000, 50.0, 0.30),      // target $300, buy $250 worth
        ];
        let (sells, buys, scale) = plan_rebalance_legs(&holdings, 1000.0, 1_000_000.0, 0.0);

        assert_eq!(scale, 1.0, "ample capital -- no scaling expected");
        assert_eq!(sells.len(), 1);
        assert_eq!(sells[0].0, "SOL");
        assert_eq!(buys.len(), 1);
        assert_eq!(buys[0].0, "BTC");
        assert_eq!(buys[0].2, 250_000_000);
    }

    #[test]
    fn plan_rebalance_legs_empty_holdings_produce_no_legs() {
        let (sells, buys, _) = plan_rebalance_legs(&[], 1000.0, 1_000_000.0, 0.0);
        assert!(sells.is_empty());
        assert!(buys.is_empty());
    }

    #[test]
    fn plan_rebalance_legs_scales_buys_proportionally_when_capital_is_short() {
        // BTC and ETH each want a $200 buy ($400 total desired), but
        // only $100 USDC is on hand and nothing is being sold this
        // pass -- capital covers 25% of demand, so both buys should be
        // scaled to 25%, not one fully funded and the other starved.
        let holdings = vec![
            holding("BTC", 1, 0, 0.0, 0.20), // target $200
            holding("ETH", 2, 0, 0.0, 0.20), // target $200
        ];
        let (sells, buys, scale) = plan_rebalance_legs(&holdings, 1000.0, 100.0, 0.0);

        assert!(sells.is_empty());
        assert_eq!(scale, 0.25);
        assert_eq!(buys.len(), 2);
        for (_, _, amount_in) in &buys {
            assert_eq!(*amount_in, 50_000_000); // $50 (25% of $200) -> raw USDC
        }
    }

    #[test]
    fn plan_rebalance_legs_suppresses_buys_when_reserve_exceeds_available_usdc() {
        // Mirrors the real wallet's situation this session: $3.95 USDC
        // on hand, but the funding-arb margin reserve alone ($10)
        // already exceeds it -- available_for_buys clamps to 0, so no
        // buy should be sized at all, not a tiny/rounded one.
        let holdings = vec![holding("SOL", 1, 0, 0.0, 0.30)]; // target $300
        let (sells, buys, scale) = plan_rebalance_legs(&holdings, 1000.0, 3.95, 10.0);

        assert!(sells.is_empty());
        assert_eq!(scale, 0.0);
        assert!(
            buys.is_empty(),
            "expected no buy when the reserve exceeds available USDC: {buys:?}"
        );
    }

    #[test]
    fn plan_rebalance_legs_sells_are_unaffected_by_the_capital_reserve() {
        // Same overweight-BTC scenario as
        // plan_rebalance_legs_sells_when_overweight, but with a reserve
        // far larger than usdc_value -- selling only ever frees USDC,
        // so it must produce the identical sell leg regardless.
        let holdings = vec![holding("BTC", 2, 1_000_000, 250.0, 0.10)];
        let (sells, buys, _) = plan_rebalance_legs(&holdings, 1000.0, 0.0, 1000.0);

        assert!(buys.is_empty());
        assert_eq!(sells.len(), 1);
        let (symbol, account_id, amount_in) = &sells[0];
        assert_eq!(symbol, "BTC");
        assert_eq!(*account_id, 2);
        assert_eq!(*amount_in, 600_000);
    }

    #[test]
    fn decide_basis_trade_deposit_hedge_on_positive_funding() {
        // Positive funding is always worth a deposit-hedge -- no
        // threshold against Solend's rate, since depositing only ever
        // earns, never costs.
        assert_eq!(
            decide_basis_trade(5.0, 20.0),
            Some(BasisDirection::DepositHedge)
        );
        assert_eq!(
            decide_basis_trade(0.01, 0.0),
            Some(BasisDirection::DepositHedge)
        );
    }

    #[test]
    fn decide_basis_trade_borrow_hedge_only_when_funding_exceeds_borrow_cost() {
        // -20% funding vs 5% borrow APY -- funding collected comfortably
        // exceeds the interest paid.
        assert_eq!(
            decide_basis_trade(-20.0, 5.0),
            Some(BasisDirection::BorrowHedge)
        );
        // -3% funding vs 5% borrow APY -- borrowing would cost more than
        // the funding collected, not profitable.
        assert_eq!(decide_basis_trade(-3.0, 5.0), None);
    }

    #[test]
    fn decide_basis_trade_none_for_zero_funding() {
        assert_eq!(decide_basis_trade(0.0, 5.0), None);
    }

    #[test]
    fn decide_basis_trade_none_at_exact_borrow_cost_boundary() {
        // Exactly equal to the borrow cost -- no edge after paying it.
        assert_eq!(decide_basis_trade(-5.0, 5.0), None);
    }
}
