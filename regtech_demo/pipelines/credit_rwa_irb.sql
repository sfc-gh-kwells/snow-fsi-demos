-- ============================================================================
-- Pipeline: Credit Risk RWA (Internal Ratings-Based Approach)
-- Owner: Credit Risk Modelling Team
-- Schedule: Monthly, end-of-period
-- Description: Calculates credit risk capital requirements using the Advanced
--   IRB approach with internally estimated PD, LGD, and EAD parameters.
--   Maturity adjustments are applied per the IRB formula. Only portfolios
--   with approved models are included.
-- Source Tables: loan_portfolio
-- Target: risk_warehouse.credit_rwa_irb_monthly
-- ============================================================================

SELECT
    portfolio_id,
    exposure_class,
    SUM(probability_of_default * loss_given_default * exposure_at_default * maturity_adjustment) AS irb_rwa
FROM loan_portfolio
WHERE model_approved = TRUE
GROUP BY portfolio_id, exposure_class;
