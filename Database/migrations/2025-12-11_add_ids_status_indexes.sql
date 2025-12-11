-- Add missing identifiers/fields and helpful indexes for Amazon entities
-- Safe to run multiple times due to IF NOT EXISTS

-- 1) Ad groups: identifiers, status, bids, timestamps
ALTER TABLE public.amazon_ad_groups
  ADD COLUMN IF NOT EXISTS status text,
  ADD COLUMN IF NOT EXISTS default_bid numeric,
  ADD COLUMN IF NOT EXISTS amazon_ad_group_id text,
  ADD COLUMN IF NOT EXISTS created_at timestamp with time zone DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamp with time zone DEFAULT now();

-- 2) Keywords: missing Amazon identifier
ALTER TABLE public.amazon_keywords
  ADD COLUMN IF NOT EXISTS amazon_keyword_id text;

-- 3) Campaigns: optional sales top-level (we still keep sales in raw_data)
ALTER TABLE public.amazon_campaigns
  ADD COLUMN IF NOT EXISTS sales numeric;

-- 4) Indexes for common filters/joins (non-unique)
CREATE INDEX IF NOT EXISTS idx_campaigns_account ON public.amazon_campaigns(account_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_profile ON public.amazon_campaigns(amazon_profile_id_text);

CREATE INDEX IF NOT EXISTS idx_ad_groups_account ON public.amazon_ad_groups(account_id);
CREATE INDEX IF NOT EXISTS idx_ad_groups_campaign ON public.amazon_ad_groups(campaign_id);
CREATE INDEX IF NOT EXISTS idx_ad_groups_amzn_id ON public.amazon_ad_groups(amazon_ad_group_id);

CREATE INDEX IF NOT EXISTS idx_keywords_adgroup ON public.amazon_keywords(ad_group_id);
CREATE INDEX IF NOT EXISTS idx_keywords_campaign ON public.amazon_keywords(campaign_id);
CREATE INDEX IF NOT EXISTS idx_keywords_profile ON public.amazon_keywords(amazon_profile_id_text);
CREATE INDEX IF NOT EXISTS idx_keywords_amzn_id ON public.amazon_keywords(amazon_keyword_id);

-- 5) Optional uniqueness at the profile scope (enable only after deduping existing rows)
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_profile ON public.amazon_campaigns(amazon_profile_id_text, campaign_id);
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_adgroup_profile ON public.amazon_ad_groups(amazon_profile_id_text, amazon_ad_group_id);
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_keyword_profile ON public.amazon_keywords(amazon_profile_id_text, amazon_keyword_id);
