-- ============================================================================
-- Pipeline: Tier 1 Leverage Ratio
-- Owner: Capital Planning & Reporting Team
-- Schedule: Quarterly
-- Description: Calculates the Tier 1 leverage ratio as Tier 1 capital
--   divided by total on-balance-sheet exposure. This non-risk-based measure
--   serves as a backstop to the risk-weighted capital framework. The
--   minimum requirement is 3%.
-- Source Tables: consolidated_balance_sheet
-- Target: risk_warehouse.leverage_ratio_quarterly
-- ============================================================================

SELECT
    entity_id,
    tier1_capital,
    total_on_balance_sheet_exposure,
    tier1_capital / total_on_balance_sheet_exposure AS leverage_ratio
FROM consolidated_balance_sheet;
