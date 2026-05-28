-- ─────────────────────────────────────────────────────────────────────────────
-- Python UDF: COMPUTE_ES_97_5
-- Regulatory context: Basel IV FRTB – BCBS MAR33.1
--
-- WHY SQL FAILS HERE:
--   SQL PERCENTILE_CONT(0.975) returns the 97.5th-percentile threshold value
--   (i.e., VaR). Expected Shortfall is the MEAN of all losses BEYOND that
--   threshold. SQL cannot express:
--     (a) conditional aggregation over a dynamic tail cut of an arbitrary array
--     (b) floor(N × 0.025) tail size with minimum-1 guard
--     (c) per-desk custom loss distributions stored as ARRAY columns
--   These require array sorting + slice math, which is natural in Python.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_ES_97_5(
  pnl_losses ARRAY
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'compute_es'
AS $$
def compute_es(pnl_losses):
    """
    Expected Shortfall at 97.5% confidence (Basel IV FRTB MAR33.1).

    Sorts daily P&L losses ascending (most negative = biggest loss first),
    slices the worst 2.5% tail, and returns the positive mean ES value.

    Args:
        pnl_losses: ARRAY of daily P&L floats (negative = loss).
                    Standard FRTB input: 250 trading-day observations.
    Returns:
        ES as a positive float (capital charge units match input units).
    """
    if not pnl_losses:
        return 0.0
    losses = [float(x) for x in pnl_losses if x is not None]
    if not losses:
        return 0.0
    losses.sort()                                      # ascending: worst losses first
    n          = len(losses)
    tail_count = max(1, int(n * 0.025))               # floor(N × 2.5%), min 1 obs
    es         = -sum(losses[:tail_count]) / tail_count
    return max(0.0, es)
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Usage (FRTB market risk RWA pipeline — replaces VaR-based approach):
--
--   SELECT
--     desk_id,
--     REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_ES_97_5(
--       ARRAY_AGG(daily_pnl) WITHIN GROUP (ORDER BY scenario_date)
--     ) AS es_97_5_capital_charge
--   FROM trading_pnl_history
--   WHERE scenario_date >= DATEADD('year', -1, CURRENT_DATE)
--   GROUP BY desk_id;
-- ─────────────────────────────────────────────────────────────────────────────
