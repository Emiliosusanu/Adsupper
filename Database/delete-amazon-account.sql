// supabase/functions/delete-amazon-account/index.ts
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
    const body = await req.json().catch(() => ({}));
    const { user_id, account_id } = body ?? {};

    if (!user_id || !account_id) {
      return jsonResponse(
        { success: false, error: 'user_id and account_id are required' },
        400,
      );
    }

    const { data: account, error: accErr } = await supabase
      .from('amazon_accounts')
      .select('id, user_id')
      .eq('id', account_id)
      .single();

    if (accErr || !account) {
      return jsonResponse(
        { success: false, error: 'Account not found' },
        404,
      );
    }

    if (account.user_id !== user_id) {
      return jsonResponse(
        { success: false, error: 'Account does not belong to this user' },
        403,
      );
    }

    // Get campaigns for this account
    const { data: campaigns, error: campErr } = await supabase
      .from('amazon_campaigns')
      .select('id')
      .eq('account_id', account_id);

    if (campErr) {
      return jsonResponse(
        { success: false, error: 'Failed to load campaigns for deletion', details: campErr.message },
        500,
      );
    }

    const campaignIds = (campaigns ?? []).map((c: any) => c.id);

    if (campaignIds.length > 0) {
      // Delete keywords
      const { error: kwErr } = await supabase
        .from('amazon_keywords')
        .delete()
        .in('campaign_id', campaignIds);
      if (kwErr) {
        return jsonResponse(
          { success: false, error: 'Failed to delete keywords', details: kwErr.message },
          500,
        );
      }
    }

    // Delete ad groups for this account
    const { error: agErr } = await supabase
      .from('amazon_ad_groups')
      .delete()
      .eq('account_id', account_id);
    if (agErr) {
      return jsonResponse(
        { success: false, error: 'Failed to delete ad groups', details: agErr.message },
        500,
      );
    }

    // Delete optimization & error logs for this account
    await supabase
      .from('optimization_logs')
      .delete()
      .eq('amazon_account_id', account_id);

    await supabase
      .from('error_logs')
      .delete()
      .eq('account_id', account_id);

    // Delete campaigns
    const { error: delCampErr } = await supabase
      .from('amazon_campaigns')
      .delete()
      .eq('account_id', account_id);
    if (delCampErr) {
      return jsonResponse(
        { success: false, error: 'Failed to delete campaigns', details: delCampErr.message },
        500,
      );
    }

    // Finally delete the account
    const { error: delAccErr } = await supabase
      .from('amazon_accounts')
      .delete()
      .eq('id', account_id);
    if (delAccErr) {
      return jsonResponse(
        { success: false, error: 'Failed to delete account', details: delAccErr.message },
        500,
      );
    }

    return jsonResponse({
      success: true,
      message: 'Amazon account and related data deleted.',
    });
  } catch (err: any) {
    console.error('delete-amazon-account error', err);
    return jsonResponse(
      { success: false, error: err?.message ?? 'Unknown error' },
      500,
    );
  }
});