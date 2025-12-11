ALTER TABLE public.amazon_accounts
  ADD COLUMN IF NOT EXISTS daily_window_days integer DEFAULT 3,
  ADD COLUMN IF NOT EXISTS weekly_window_days integer DEFAULT 7,
  ADD COLUMN IF NOT EXISTS monthly_window_days integer DEFAULT 30,
  ADD COLUMN IF NOT EXISTS last_3d_sync_at timestamp with time zone,
  ADD COLUMN IF NOT EXISTS last_7d_sync_at timestamp with time zone,
  ADD COLUMN IF NOT EXISTS last_30d_sync_at timestamp with time zone;

CREATE INDEX IF NOT EXISTS idx_accounts_last_3d ON public.amazon_accounts(last_3d_sync_at);
CREATE INDEX IF NOT EXISTS idx_accounts_last_7d ON public.amazon_accounts(last_7d_sync_at);
CREATE INDEX IF NOT EXISTS idx_accounts_last_30d ON public.amazon_accounts(last_30d_sync_at);
