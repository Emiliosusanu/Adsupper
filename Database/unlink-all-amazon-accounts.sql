// supabase/functions/unlink-all-amazon-accounts/index.ts
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.42.0';

const corsHeaders: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers':
    'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }
  if (req.method !== 'POST') {
    return jsonResponse({ success: false, error: 'Method not allowed' }, 405);
  }

  const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
    global: { headers: { Authorization: req.headers.get('Authorization') ?? '' } },
  });

  try {
    const { data: { user }, error: userErr } = await supabase.auth.getUser();
    if (userErr || !user) {
      return jsonResponse(
        { success: false, error: 'Unauthorized: no user session' },
        401,
      );
    }

    const { data: accounts, error: accErr } = await supabase
      .from('amazon_accounts')
      .select('id')
      .eq('user_id', user.id);

    if (accErr) {
      return jsonResponse(
        { success: false, error: 'Failed to load accounts', details: accErr.message },
        500,
      );
    }

    const accountIds = (accounts ?? []).map((a: any) => a.id);
    let totalDeleted = 0;

    for (const accountId of accountIds) {
      // campaigns for this account
      const { data: campaigns, error: campErr } = await supabase
        .from('amazon_campaigns')
        .select('id')
        .eq('account_id', accountId);
      if (campErr) continue;

      const campaignIds = (campaigns ?? []).map((c: any) => c.id);

      if (campaignIds.length > 0) {
        await supabase
          .from('amazon_keywords')
          .delete()
          .in('campaign_id', campaignIds);
      }

      await supabase
        .from('amazon_ad_groups')
        .delete()
        .eq('account_id', accountId);

      await supabase
        .from('optimization_logs')
        .delete()
        .eq('amazon_account_id', accountId);

      await supabase
        .from('error_logs')
        .delete()
        .eq('account_id', accountId);

      await supabase
        .from('amazon_campaigns')
        .delete()
        .eq('account_id', accountId);

      await supabase
        .from('amazon_accounts')
        .delete()
        .eq('id', accountId);

      totalDeleted++;
    }

    return jsonResponse({
      success: true,
      message: `Unlinked ${totalDeleted} Amazon account(s) and deleted related data.`,
    });
  } catch (err: any) {
    console.error('unlink-all-amazon-accounts error', err);
    return jsonResponse(
      { success: false, error: err?.message ?? 'Unknown error' },
      500,
    );
  }
});