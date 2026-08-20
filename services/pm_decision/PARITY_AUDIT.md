# Strategy parity audit

This adapter is an independent implementation of the reviewed strategy
contract. It does not import or execute the old live tree. The following paths
identify the source snapshot used during the audit; they are evidence, not
runtime dependencies.

## Authoritative frozen selection

The `gcy_3/poly_parity` handoff is authoritative where an older executor and
the parity handoff disagree:

- `gcy_3/poly_parity/package.py` records the fixed strategy manifest:
  v1 edge `>0.30`, near `>=2.36`; v2 edge `>0.10`, near `>=1.50`, both hourly
  veto factors required, maximum hourly age one hour.
- `gcy_3/poly_parity/parity/strategies.py` disables cross-sectional ranking,
  selects only `World/Geopolitics`, keeps a three-hour prediction edge age,
  and constructs the same fixed-threshold v1/v2 pair.
- `gcy_3/handoff_live/strategy_reference/wgeo_mult_factor_v1.py` overrides
  the downsampled base to `confirm_bars=1` and `min_entry_gap=0`.
- `gcy_3/handoff_live/strategy_reference/wgeo_mult_factor_v1_downsampled.py`
  supplies the strict spread/price comparisons, ten-minute grid, and 48-hour
  holding semantics.

The configured target notionals are deployment policy. The handoff reference
is v1=5 USD and the reviewed leaderboard override is v2=10 USD. The adapter
therefore requires both values explicitly and has no silent defaults.

## Reused pure calculations

The following old functions were suitable for a pure, dependency-free port:

- `gcy/PM_trading_v2/mult_factor_v2/executor.py`:
  15-level dz0p1 near-book calculation, mid validity band, liquidity minimum,
  relative spread, and strict comparison directions;
- `gcy/PM_trading_v2/mult_factor_v2/hourly_bar.py`:
  right-closed UTC hourly buckets, no minute/hour fill, missing-hour segment
  reset, and closed-bar lookup;
- `gcy/PM_trading_v2/mult_factor_v2/mom_macd.py`:
  MOM(12) and TA-Lib-seeded MACD signal (12,26,9), computed independently per
  continuous segment.

The adapter ports only these deterministic calculations. Its tests fix the
first valid MOM/MACD indices, the v2 `>0.10` edge boundary, and the v1/v2
hourly-data separation.

## Deliberately not reused

- `gcy/PM_trading_v2/mult_factor_wgeo/executor.py` uses the superseded v1
  confirm-3, 30-minute entry gap, and 3.5 USD stake.
- `gcy/PM_trading_v2/mult_factor_v2/executor.py` uses the superseded v2
  `edge >0.18` gate and includes its own ladder/execution behavior.
- Old wallet selection, balance checks, order submission, fill accounting,
  local state, retries, and reconciliation are not strategy calculations and
  are not present in this service.
- The old multi-level ladder price is not used. The v4 execution contract is
  exact top-of-book `LIMIT + FOK`; Go independently validates that price and
  remains authoritative for risk, account, reservation, and venue submission.

These exclusions are load-bearing: importing either old executor wholesale
would silently change the selected strategy and duplicate execution authority
that belongs to Trading Execution.
