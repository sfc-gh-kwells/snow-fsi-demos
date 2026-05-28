-- Pipeline:      sma_ilm_capital
-- Owner:         Operational Risk
-- Schedule:      Quarterly
-- Description:   Computes SMA operational risk capital using the Basel IV
--                Internal Loss Multiplier (ILM). Attempts to implement the
--                ILM log formula in SQL using EXP and LN functions.
-- Source Tables:  BUSINESS_INDICATOR_DATA, OPERATIONAL_LOSS_EVENTS
-- Target:        OP_RISK_SMA_CAPITAL

WITH bic_calc AS (
  SELECT
    entity_id,
    business_indicator,
    -- PARTIALLY INCORRECT: BIC tiered marginal rates are applied as a simple
    -- CASE expression, but the carry-over across bucket boundaries requires
    -- running totals. The formula below double-counts the boundary amounts
    -- when BI spans multiple buckets. BCBS OPE10.4 requires marginal rates
    -- applied to each tier separately (like a tax bracket calculation).
    CASE
      WHEN business_indicator <= 1e9  THEN business_indicator * 0.12
      WHEN business_indicator <= 30e9 THEN business_indicator * 0.15   -- WRONG: should be 1e9*0.12 + (BI-1e9)*0.15
      ELSE                                 business_indicator * 0.18   -- WRONG: should be tiered sum
    END AS bic_incorrect
  FROM business_indicator_data
),
loss_summary AS (
  SELECT
    entity_id,
    AVG(annual_loss_total) AS avg_annual_loss_10yr
  FROM operational_loss_events
  WHERE loss_year >= YEAR(CURRENT_DATE) - 10
  GROUP BY entity_id
),
ilm_calc AS (
  SELECT
    b.entity_id,
    b.bic_incorrect,
    l.avg_annual_loss_10yr,
    -- INCORRECT: The ILM formula is  ln( e - 1 + (LC/BIC)^0.8 )
    -- SQL LN/EXP does not have a clean cross-dialect implementation of
    -- the (x)^0.8 non-integer exponent, and the formula requires the
    -- Euler number (e ≈ 2.71828), not EXP(1) in some SQL engines.
    -- Using POWER(ratio, 0.8) also loses precision for very small ratios.
    LN(EXP(1) - 1 + POWER(NULLIF(l.avg_annual_loss_10yr, 0) / NULLIF(b.bic_incorrect, 0), 0.8)) AS ilm_approx
  FROM bic_calc b
  LEFT JOIN loss_summary l ON b.entity_id = l.entity_id
)
SELECT
  entity_id,
  bic_incorrect,
  avg_annual_loss_10yr,
  GREATEST(1.0, COALESCE(ilm_approx, 1.0)) AS ilm_factor,
  bic_incorrect * GREATEST(1.0, COALESCE(ilm_approx, 1.0)) AS sma_capital
FROM ilm_calc;
