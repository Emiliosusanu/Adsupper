-- Adds metrics columns to ad groups and keywords so we can persist Amazon v3 report fields
-- Safe to run multiple times due to IF NOT EXISTS

-- Ad groups: add standard metrics and raw_data
ALTER TABLE public.amazon_ad_groups
  ADD COLUMN IF NOT EXISTS spend numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS impressions integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS clicks integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS orders integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS acos numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS ctr numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cpc numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- Keywords: add ctr/cpc and raw_data to complement existing metrics
ALTER TABLE public.amazon_keywords
  ADD COLUMN IF NOT EXISTS ctr numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cpc numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_data jsonb;
