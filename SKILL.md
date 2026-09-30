# Implementing a trading strategy in simple-bot

Put trade logic in `./src/brain/state.rs` `evaluate_inner` function.

## logic

Split development into two parts:

1. write READ only code; use `log_warn` to confirm the correct data is being compiled
1. use the `State` struct to store data to figure out what transactions to do

## trading

### Spot Trade

Put instructions here on how to use TradeRouter to construct a spot leg.

### Perp Trade

Put instructions here on how to use PerpRouter to construct a spot leg.

### Lending

Put instructions here on how to borrow/lend money using LendingRouter.

## log

Use `log_warn` to show what is happening in the bot.
