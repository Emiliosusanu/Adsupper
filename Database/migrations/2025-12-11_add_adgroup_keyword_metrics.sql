-- Ensure metrics and raw_data columns exist for ad groups and keywords used by the VPS sync
-- Safe to run multiple times due to IF NOT EXISTS

-- Ad groups: performance metrics + raw_data
ALTER TABLE public.amazon_ad_groups
  ADD COLUMN IF NOT EXISTS spend numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS impressions integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS clicks integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS orders integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS acos numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS ctr numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cpc numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- Keywords: ctr/cpc + raw_data for metrics enrichment
ALTER TABLE public.amazon_keywords
  ADD COLUMN IF NOT EXISTS ctr numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cpc numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_data jsonb;
