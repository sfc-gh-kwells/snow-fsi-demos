-- ─────────────────────────────────────────────────────────────────────────────
-- Python UDF: COMPUTE_SA_CCR_PFE_AGG
-- Regulatory context: Basel IV SA-CCR – BCBS CRE52
--
-- WHY SQL FAILS HERE:
--   The Basel IV SA-CCR PFE aggregation formula for a hedging set is:
--
--     PFE_agg = sqrt( ρ² · (Σ D_i)² + (1-ρ²) · Σ D_i² )
--
--   where D_i is the adjusted notional delta per trade and ρ is the
--   asset-class correlation (e.g., 50% for FX, 70% for IR).
--
--   SQL SUM() can compute Σ D_i and Σ D_i² independently, but combining
--   them inside a sqrt() with conditional ρ per asset class across nesting
--   sets produces a deeply nested expression that is:
--     (a) error-prone to maintain across 5 BCBS asset classes
--     (b) impossible to unit-test in isolation
--     (c) not extendable to the full SA-CCR supervisory delta adjustments
--   A Python UDF with numpy expresses the formula directly and is testable.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_SA_CCR_PFE_AGG(
  adjusted_notionals  ARRAY,   -- list of D_i values per trade in the hedging set
  asset_class         VARCHAR  -- 'IR', 'FX', 'CREDIT', 'EQUITY', 'COMMODITY'
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy')
HANDLER = 'compute_pfe_agg'
AS $$
import numpy as np

# Supervisory correlations from BCBS CRE52.48
SUPERVISORY_RHO = {
    'IR':        0.70,
    'FX':        0.50,
    'CREDIT':    0.50,
    'EQUITY':    0.65,
    'COMMODITY': 0.40,
}

def compute_pfe_agg(adjusted_notionals, asset_class):
    """
    SA-CCR hedging-set PFE aggregation per BCBS CRE52.

    Formula: PFE_agg = sqrt( rho^2 * (sum_D)^2 + (1-rho^2) * sum_D_sq )

    Args:
        adjusted_notionals: ARRAY of adjusted notional deltas (D_i) per trade.
                            Can include negative values (short positions).
        asset_class:        One of IR, FX, CREDIT, EQUITY, COMMODITY.
    Returns:
        Non-negative PFE aggregation float (capital add-on units).
    """
    if not adjusted_notionals:
        return 0.0

    rho  = SUPERVISORY_RHO.get((asset_class or '').upper(), 0.50)
    D    = np.array([float(x) for x in adjusted_notionals if x is not None])

    sum_D    = float(np.sum(D))
    sum_D_sq = float(np.sum(D ** 2))

    pfe_agg = np.sqrt(rho**2 * sum_D**2 + (1 - rho**2) * sum_D_sq)
    return max(0.0, float(pfe_agg))
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Usage (SA-CCR derivatives EAD pipeline):
--
--   SELECT
--     netting_set_id,
--     1.4 * (
--       GREATEST(0, SUM(mtm_value) - SUM(collateral_value))   -- RC
--       + REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_SA_CCR_PFE_AGG(
--           ARRAY_AGG(adjusted_notional_delta),
--           asset_class
--         )
--     ) AS ead_sa_ccr
--   FROM derivatives
--   GROUP BY netting_set_id, asset_class;
-- ─────────────────────────────────────────────────────────────────────────────
