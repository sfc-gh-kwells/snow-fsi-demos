-- ============================================================================
-- Pipeline: CET1 Capital Deductions
-- Owner: Capital Planning & Reporting Team
-- Schedule: Quarterly
-- Description: Applies regulatory deductions to Common Equity Tier 1 capital.
--   Deferred tax assets (DTAs) that exceed the 10% individual threshold are
--   deducted from CET1. This pipeline feeds the capital adequacy ratio
--   calculation for regulatory reporting.
-- Source Tables: capital_components
-- Target: risk_warehouse.cet1_deductions_quarterly
-- ============================================================================

SELECT
    entity_id,
    cet1_before_deductions,
    GREATEST(0, deferred_tax_assets - 0.10 * cet1_before_deductions) AS dta_deduction,
    cet1_before_deductions - GREATEST(0, deferred_tax_assets - 0.10 * cet1_before_deductions) AS cet1_after_deductions
FROM capital_components;
