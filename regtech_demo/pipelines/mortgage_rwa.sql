-- ============================================================================
-- Pipeline: Residential Mortgage Credit Risk RWA
-- Owner: Retail Credit Risk Team
-- Schedule: Monthly
-- Description: Calculates risk-weighted assets for the residential mortgage
--   book. Applies a flat 35% risk weight to all performing mortgage
--   exposures per the standardised approach. Non-performing loans are
--   excluded and handled by a separate provisioning pipeline.
-- Source Tables: residential_mortgage_portfolio
-- Target: risk_warehouse.mortgage_rwa_monthly
-- ============================================================================

SELECT
    loan_id,
    borrower_id,
    outstanding_balance,
    outstanding_balance * 0.35 AS risk_weighted_amount
FROM residential_mortgage_portfolio
WHERE loan_status = 'PERFORMING';
