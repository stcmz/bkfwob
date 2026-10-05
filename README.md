# bkfwob

Broker integration layer for the fwob toolchain. [mdfwob](https://github.com/stcmz/mdfwob) owns
market data, [qtfwob](https://github.com/stcmz/qtfwob) owns research and backtesting; bkfwob owns
the account: positions, orders, fills, and the guardrails around sending them.

**Status: under construction.** The core, the guardrails and the CLI work against the built-in
`fake` broker and, read-only so far, against IB paper; placing through IB awaits a paper gateway
with the API read-only setting off. The MCP server is next.

## What happens when you send an order

Every order goes through one path, the desk:

1. **Guardrails.** The kill switch, what the broker supports, then the limits: per-order, daily
   and position notional; open orders; orders per minute; symbol lists; a collar on limit prices
   that are far through the market; a fresh quote for market orders; and no sell larger than the
   position, counting sells already working. A refusal is journaled and nothing is sent (exit 3).
2. **Write-ahead.** The order gets a client id (`bk-YYYYMMDD-NNNN`) and is journaled to disk
   *before* the broker sees it.
3. **Classified outcome.** A rejection is final. A lost reply leaves the order `unknown`: it is
   never re-sent, and `bkfwob reconcile` looks it up by client id (exit 4).

`bkfwob reconcile` also checks positions against the fills since the last check. A position that
moved with no fill to explain it halts trading until a person looks.

## CLI

Tokens, not flags, in any order, case-sensitive: symbols are upper case and every keyword is
lower case.

```text
bkfwob status                              connection, mode, halt, today's usage
bkfwob account | positions | quote AAPL
bkfwob buy AAPL 10 lmt:185.50 gtc          mkt | lmt:P | stp:P | stp-lmt:S,L | trail:A | trail:P%
bkfwob sell AAPL usd:2000 preview          show the guardrail verdict, send nothing
bkfwob orders [open|done|all] [AAPL] [2026-10-01..]
bkfwob fills [AAPL] [START..END]
bkfwob modify bk-20261005-0007 lmt:184.90
bkfwob cancel ID... | all [SYMBOL...]
bkfwob flatten AAPL | all                  cancel, then close at market; works while halted
bkfwob reconcile
bkfwob watch [orders|fills|account]
bkfwob halt [REASON...] | resume           kill switch for every bkfwob process
```

Common tokens: a broker (`ibkr`, `fake`), a mode (`paper`, `live`), a format (`table`, `csv`,
`md`, `jsonl`), and an optional `CONFIG.toml` as the first argument. Orders default to `day`
and regular hours (`rth`); extended hours must be asked for with `eth`.

## Paper and live

Paper is the default. Live takes two keys: `mode = "live"` in the config **and** `BKFWOB_LIVE=1`
in the environment; the account the broker reports must also actually be live. The `live`
token on a command line only *confirms* a live config; it cannot switch one on. Live orders,
flattening and resuming ask for confirmation unless `--yes`.

## Configuration

See [`config.example.toml`](config.example.toml). Looked up as: a `.toml` first argument, else
`BKFWOB_CONFIG`, else `~/.bkfwob/config.toml`, else defaults (IBKR paper). Unknown keys are
errors. State (journals, the id sequence, the `HALT` file) lives in `BKFWOB_STATE` or
`~/.bkfwob`.

## Layout

- `crates/bkfwob-core`: broker-neutral types, the `Broker` trait, order lifecycle, guardrails,
  write-ahead journal, reconciliation, and a scriptable fake broker. No network code.
- `crates/bkfwob`: the CLI and broker adapters (`ibkr`, on by default).

## IBKR

Talks to IB Gateway or TWS (Gateway ports 4002 paper / 4001 live by default; client id 17, kept
stable because IB only lets the placing client change an order). Our client order id rides in
IB's `orderRef`, which is how an order of unknown fate is found again. Paper or live is read
from the account id (`DU…` is paper), never from the port. Market orders outside regular hours,
notional quantities and fractional shares are refused up front. Quotes carry the last trade's
time, so on a closed market a market order is refused as stale.

The opt-in paper suite: `BKFWOB_IBKR_PAPER=1 cargo test -p bkfwob --test ibkr_paper --
--ignored --test-threads=1`.

## License

Apache-2.0
