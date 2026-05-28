-- Pipeline:      stressed_var_frtb
-- Owner:         Market Risk
-- Schedule:      Daily 07:00 UTC
-- Description:   Computes stressed Value-at-Risk for FRTB capital requirement using
--                historical simulation. Approximates the 97.5% tail using SQL
--                PERCENTILE_CONT on aggregated desk-level P&L scenarios.
-- Source Tables:  TRADING_PNL_HISTORY, DESK_CONFIGURATIONS, STRESS_SCENARIOS
-- Target:        STRESSED_VAR_RESULTS

SELECT
  t.desk_id,
  t.scenario_date,
  -- Approximate the 97.5th-percentile loss as a proxy for FRTB ES
  -- INCORRECT: PERCENTILE_CONT returns the threshold value (VaR),
  -- not the mean of losses beyond the threshold (Expected Shortfall).
  -- FRTB MAR33.1 requires the conditional tail expectation, not VaR.
  PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY t.daily_pnl) AS var_975_approx,
  COUNT(*) AS scenario_count,
  MIN(t.daily_pnl) AS worst_daily_loss,
  AVG(CASE WHEN t.daily_pnl < 0 THEN t.daily_pnl ELSE NULL END) AS avg_loss_days
FROM trading_pnl_history t
JOIN desk_configurations d ON t.desk_id = d.desk_id
WHERE
  t.scenario_date >= DATEADD('year', -1, CURRENT_DATE)
  AND d.desk_status = 'ACTIVE'
GROUP BY
  t.desk_id,
  t.scenario_date;
