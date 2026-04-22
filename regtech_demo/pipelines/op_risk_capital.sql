-- ============================================================================
-- Pipeline: Operational Risk Capital Requirement
-- Owner: Operational Risk Management Team
-- Schedule: Annually
-- Description: Calculates the operational risk capital charge using the Basic
--   Indicator Approach (BIA). The capital requirement is set at 15% of the
--   average positive annual gross income over the previous three years.
--   This feeds the total capital adequacy calculation.
-- Source Tables: annual_income_statement
-- Target: risk_warehouse.op_risk_capital_annual
-- ============================================================================

SELECT
    reporting_year,
    AVG(gross_income) OVER (ORDER BY reporting_year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) * 0.15 AS op_risk_capital
FROM annual_income_statement
WHERE reporting_year >= YEAR(CURRENT_DATE) - 3;
