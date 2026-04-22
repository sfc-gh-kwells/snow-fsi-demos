-- ============================================================================
-- Pipeline: Derivatives Exposure at Default (EAD)
-- Owner: Counterparty Credit Risk Team
-- Schedule: Daily T+1
-- Description: Calculates Exposure at Default for OTC derivative portfolios
--   using the Current Exposure Method (CEM). Replacement cost is the positive
--   mark-to-market value; potential future exposure uses notional-based
--   add-on factors by asset class.
-- Source Tables: otc_derivatives
-- Target: risk_warehouse.derivatives_ead_daily
-- ============================================================================

SELECT 
    netting_set_id,
    counterparty_id,
    GREATEST(0, mark_to_market_value) + notional_amount * add_on_factor AS exposure_at_default
FROM otc_derivatives
GROUP BY netting_set_id, counterparty_id;
