# bkfwob

Broker integration layer for the fwob toolchain. [mdfwob](https://github.com/stcmz/mdfwob) owns
market data, [qtfwob](https://github.com/stcmz/qtfwob) owns research and backtesting; bkfwob owns
the account: positions, orders, fills, and the guardrails around sending them.

**Status: under construction. Paper accounts only until the guardrails are complete.**

## Layout

- `crates/bkfwob-core`: broker-neutral types, the `Broker` trait, order lifecycle, guardrails,
  write-ahead journal, reconciliation, and a scriptable fake broker for tests. No network code.
- `crates/bkfwob`: broker adapters (IBKR first), the `bkfwob` CLI, and `bkfwob mcp`.

## License

Apache-2.0
