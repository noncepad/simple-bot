use catscope_rust_bot::{log_debug, trader::dex::update::StateChange};

/// CustomState is for coding agents to modify
#[derive(Debug)]
pub(crate) struct CustomState {}
impl Default for CustomState {
    fn default() -> Self {
        Self {}
    }
}
impl CustomState {
    pub(crate) fn on_state_change(change: StateChange) {
        match change {
            StateChange::Blank => {}
            StateChange::Spot {
                token_a,
                token_b,
                price,
            } => {
                // have token_a/token_b (the pool's two mints) + price
                log_debug!("state change: {change:?}");
            }
            StateChange::Lending {
                account_id,
                borrow_apy,
                supply_apy,
                price_usd,
            } => {
                log_debug!("state change: {change:?}");
            }
            StateChange::Perp {
                token_a,
                token_b,
                mark_price_usd,
            } => {
                log_debug!("state change: {change:?}");
            }
        };
    }
}
