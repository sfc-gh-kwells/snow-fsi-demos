-- ─────────────────────────────────────────────────────────────────────────────
-- Python UDF: CLASSIFY_SMA_ILM_BUCKET + COMPUTE_SMA_CAPITAL
-- Regulatory context: Basel IV Op Risk SMA – BCBS OPE10
--
-- WHY SQL FAILS HERE:
--   The SMA Internal Loss Multiplier (ILM) uses:
--     ILM = ln(exp(1) - 1 + (LC / BIC)^0.8)
--
--   where LC = 10-year average annual loss component.
--   SQL has no native ln(exp(1)-1 + x) function and EXP/LN chaining is
--   brittle. More critically:
--     - The BIC tiered coefficient table (12% / 15% / 18%) requires
--       running totals across BI buckets, not a simple CASE expression
--     - The ILM formula involves non-integer exponentiation (^0.8) which
--       varies across SQL dialects
--     - Bucketing thousands of loss events into the 3-tier BI bucket
--       structure with carry-over between tiers is error-prone in SQL
--   Python handles all of this cleanly with standard math and clear logic.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Part 1: BIC calculation (tiered business indicator component) ─────────────
CREATE OR REPLACE FUNCTION REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_BIC(
  business_indicator FLOAT   -- BI in EUR (the sum of interest, services, financial components)
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'compute_bic'
AS $$
import math

# BCBS OPE10.4 BIC marginal coefficient buckets (BI in EUR)
BUCKET_THRESHOLDS = [1_000_000_000, 30_000_000_000]   # 1B and 30B
BUCKET_RATES      = [0.12, 0.15, 0.18]                # 12%, 15%, 18%

def compute_bic(business_indicator):
    """
    Basel IV SMA Business Indicator Component (BIC).

    Three-tier marginal rate structure per BCBS OPE10.4:
      Bucket 1 (BI ≤  1B): 12%
      Bucket 2 (BI ≤ 30B): 15% on the portion above  1B
      Bucket 3 (BI > 30B): 18% on the portion above 30B

    Args:
        business_indicator: Annual BI value in the bank's reporting currency.
    Returns:
        BIC as a float (same currency units).
    """
    if business_indicator is None or business_indicator <= 0:
        return 0.0

    bi     = float(business_indicator)
    bic    = 0.0
    prev   = 0.0

    for i, threshold in enumerate(BUCKET_THRESHOLDS):
        if bi <= threshold:
            bic += (bi - prev) * BUCKET_RATES[i]
            return bic
        bic  += (threshold - prev) * BUCKET_RATES[i]
        prev  = threshold

    # Bucket 3: remainder above 30B
    bic += (bi - BUCKET_THRESHOLDS[-1]) * BUCKET_RATES[-1]
    return bic
$$;

-- ── Part 2: ILM factor and final SMA capital ──────────────────────────────────
CREATE OR REPLACE FUNCTION REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_SMA_CAPITAL(
  business_indicator      FLOAT,  -- annual BI (same currency as loss data)
  avg_annual_loss_10yr    FLOAT   -- 10-year average annual operational loss (LC)
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'compute_sma'
AS $$
import math

BUCKET_THRESHOLDS = [1_000_000_000, 30_000_000_000]
BUCKET_RATES      = [0.12, 0.15, 0.18]

def _bic(bi):
    bi   = max(0.0, float(bi or 0))
    bic  = 0.0; prev = 0.0
    for threshold, rate in zip(BUCKET_THRESHOLDS, BUCKET_RATES[:2]):
        if bi <= threshold:
            return bic + (bi - prev) * rate
        bic += (threshold - prev) * rate; prev = threshold
    return bic + (bi - BUCKET_THRESHOLDS[-1]) * BUCKET_RATES[-1]

def compute_sma(business_indicator, avg_annual_loss_10yr):
    """
    Basel IV SMA Capital = BIC × ILM per BCBS OPE10.

    ILM formula: ln( e - 1 + (LC/BIC)^0.8 )
    If loss history < 10 years, ILM defaults to 1.0 (no adjustment).

    Args:
        business_indicator:   Annual BI value (EUR or reporting currency).
        avg_annual_loss_10yr: 10-year average annual loss component (LC).
                              Pass 0 or None if < 10 years of history.
    Returns:
        SMA capital requirement as a positive float.
    """
    if not business_indicator or business_indicator <= 0:
        return 0.0

    bic = _bic(business_indicator)
    if bic <= 0:
        return 0.0

    lc = float(avg_annual_loss_10yr or 0)
    if lc <= 0:
        ilm = 1.0                                     # Default ILM without loss data
    else:
        ratio = lc / bic
        ilm   = math.log(math.e - 1 + ratio ** 0.8)  # BCBS OPE10.6 formula
        ilm   = max(1.0, ilm)                         # ILM floor = 1.0

    return bic * ilm
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Usage (replaces BIA op_risk_capital.sql pipeline):
--
--   SELECT
--     entity_id,
--     REGTECH_DEMO_DB.REGULATORY_REPORTING.COMPUTE_SMA_CAPITAL(
--       annual_business_indicator,
--       avg_annual_loss_10yr
--     ) AS sma_capital_requirement
--   FROM business_indicator_summary;
-- ─────────────────────────────────────────────────────────────────────────────
