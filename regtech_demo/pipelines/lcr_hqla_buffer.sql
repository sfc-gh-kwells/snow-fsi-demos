-- ============================================================================
-- Pipeline: LCR High-Quality Liquid Assets Buffer
-- Owner: Treasury & Liquidity Risk Team
-- Schedule: Daily
-- Description: Classifies the liquid asset inventory into HQLA tiers for
--   the Liquidity Coverage Ratio calculation. Applies appropriate haircuts
--   to each asset class. Level 1 assets receive no haircut; Level 2A assets
--   receive a 15% haircut. The resulting HQLA buffer is compared against
--   30-day net cash outflows to compute the LCR.
-- Source Tables: liquid_asset_inventory
-- Target: risk_warehouse.lcr_hqla_daily
-- ============================================================================

SELECT
    asset_id,
    asset_type,
    CASE asset_type
        WHEN 'CASH' THEN market_value
        WHEN 'SOVEREIGN_DEBT' THEN market_value
        WHEN 'RMBS' THEN market_value * 0.85  -- 15% haircut
        WHEN 'CORPORATE_BOND' THEN market_value * 0.85
    END AS hqla_value,
    CASE asset_type
        WHEN 'CASH' THEN 'Level1'
        WHEN 'SOVEREIGN_DEBT' THEN 'Level1'
        WHEN 'RMBS' THEN 'Level2A'
        WHEN 'CORPORATE_BOND' THEN 'Level2A'
    END AS hqla_tier
FROM liquid_asset_inventory;
