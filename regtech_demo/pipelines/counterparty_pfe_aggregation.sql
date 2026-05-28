-- Pipeline:      counterparty_pfe_aggregation
-- Owner:         Counterparty Credit Risk
-- Schedule:      Daily 05:30 UTC
-- Description:   Aggregates Potential Future Exposure (PFE) across trades in a
--                netting set for SA-CCR. Uses a simplified SQRT(SUM(D^2)) formula
--                that omits the supervisory correlation factor rho required by
--                BCBS CRE52 for partial netting recognition.
-- Source Tables:  DERIVATIVES_BOOK, NETTING_AGREEMENTS, COLLATERAL_POSITIONS
-- Target:        CCR_PFE_RESULTS

SELECT
  n.netting_set_id,
  n.counterparty_id,
  n.asset_class,
  -- INCORRECT: This formula omits the correlation factor rho.
  -- BCBS CRE52.48 requires:
  --   PFE_agg = SQRT( rho^2 * (SUM(D_i))^2 + (1-rho^2) * SUM(D_i^2) )
  -- where rho varies by asset class: IR=70%, FX=50%, Credit=50%, Equity=65%
  -- The simplified SQRT(SUM(...)) formula over-estimates diversification
  -- benefit and understates capital for mixed long/short books.
  SQRT(SUM(POWER(d.adjusted_notional_delta, 2))) AS pfe_agg_simplified,
  SUM(GREATEST(0, d.mark_to_market)) - SUM(c.collateral_value) AS replacement_cost,
  1.4 * (
    GREATEST(0, SUM(GREATEST(0, d.mark_to_market)) - SUM(c.collateral_value))
    + SQRT(SUM(POWER(d.adjusted_notional_delta, 2)))
  ) AS ead_understated
FROM derivatives_book d
JOIN netting_agreements n ON d.netting_set_id = n.netting_set_id
LEFT JOIN collateral_positions c ON n.netting_set_id = c.netting_set_id
GROUP BY
  n.netting_set_id,
  n.counterparty_id,
  n.asset_class;
