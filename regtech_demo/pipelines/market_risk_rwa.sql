-- ============================================================================
-- Pipeline: Market Risk RWA Calculation
-- Owner: Trading Risk Analytics Team
-- Schedule: Daily T+1
-- Description: Calculates risk-weighted assets for the trading book using
--   Value-at-Risk (99th percentile, 10-day holding period) with square-root-
--   of-time scaling. Feeds into the consolidated capital adequacy report.
-- Source Tables: trading_positions
-- Target: risk_warehouse.market_risk_rwa_daily
-- ============================================================================

SELECT 
    desk_id,
    trading_date,
    SUM(var_99_10d * sqrt_scaling_factor) AS market_risk_rwa
FROM trading_positions
GROUP BY desk_id, trading_date;
