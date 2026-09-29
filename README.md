# Simple Bot

This code pulls in [catscope-rust-bot](https://github.com/noncepad/catscope-rust-bot) as a dependency to be compiled into a web assembly trading bot that runs in the [Catscope Bot runtime](https://catscope.io).

Business logic goes into `./src/brain` with `./src/brain/testperpv1` serving as an example.

To implement a new trading strategy, copy testperpv1 into a new directory and then call the object in `./src/lib.rs` as the `sampler` object.
