// supabase/functions/finalize-amazon-link-with-profiles/index.ts
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.42.0';

const corsHeaders: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers':
    'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const AMAZON_CLIENT_ID = Deno.env.get('AMAZON_CLIENT_ID')!;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}

function getRegionFromCountry(countryCode?: string | null): string {
  if (!countryCode) return 'na';
  const cc = countryCode.toUpperCase();
  if (['US', 'CA', 'MX', 'BR'].includes(cc)) return 'na';
  if (
    ['UK', 'GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'SE', 'PL', 'TR', 'AE', 'SA']
      .includes(cc)
  )
    return 'eu';
  if (['JP', 'AU', 'SG', 'IN'].includes(cc)) return 'fe';
  return 'na';
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
    const {
      selected_profile_ids,
      tokens,
      user_id,
      relink_account_id,
    } = body ?? {};

    if (!Array.isArray(selected_profile_ids) || selected_profile_ids.length === 0) {
      return jsonResponse(
        { success: false, error: 'No selected_profile_ids provided' },
        400,
      );
    }
    if (!tokens?.access_token || !tokens?.refresh_token || !tokens?.expires_in) {
      return jsonResponse(
        { success: false, error: 'Invalid or missing tokens' },
        400,
      );
    }

    const { data: { user }, error: userErr } = await supabase.auth.getUser();
    if (userErr || !user) {
      return jsonResponse({ success: false, error: 'Unauthorized user' }, 401);
    }
    if (user_id && user_id !== user.id) {
      return jsonResponse(
        { success: false, error: 'User mismatch' },
        403,
      );
    }

    const expiryIso = new Date(
      Date.now() + (tokens.expires_in ?? 3600) * 1000,
    ).toISOString();

    const linkedProfiles: any[] = [];

    for (const p of selected_profile_ids) {
      const profileId = String(p.profileId ?? p.profile_id ?? '');
      if (!profileId) continue;

      const apiRegion =
        p.apiRegion ??
        getRegionFromCountry(p.countryCode ?? p.country_code);
      const name =
        p.accountInfo?.name ??
        p.accountInfo?.accountName ??
        `Profile ${profileId}`;

      // existing account for user + profile?
      const { data: existing, error: existingErr } = await supabase
        .from('amazon_accounts')
        .select('id, status')
        .eq('user_id', user.id)
        .eq('amazon_profile_id', profileId)
        .maybeSingle();

      let accountId: string | null = null;
      let alreadyLinked = false;

      if (existingErr && existingErr.code !== 'PGRST116') {
        // PGRST116 = no rows
        return jsonResponse(
          { success: false, error: 'Error checking existing account', details: existingErr.message },
          500,
        );
      }

      if (existing) {
        alreadyLinked = true;
        accountId = existing.id;
        const { error: updateErr } = await supabase
          .from('amazon_accounts')
          .update({
            client_id: AMAZON_CLIENT_ID,
            refresh_token: tokens.refresh_token,
            access_token: tokens.access_token,
            token_expires_at: expiryIso,
            amazon_profile_id: profileId,
            amazon_region: apiRegion,
            scope: tokens.scope ?? null,
            status: 'active',
            updated_at: new Date().toISOString(),
          })
          .eq('id', existing.id);

        if (updateErr) {
          linkedProfiles.push({
            profileId,
            name,
            apiRegion,
            alreadyLinked,
            success: false,
            error: updateErr.message,
          });
          continue;
        }
      } else {
        const { data: inserted, error: insertErr } = await supabase
          .from('amazon_accounts')
          .insert({
            user_id: user.id,
            name,
            client_id: AMAZON_CLIENT_ID,
            refresh_token: tokens.refresh_token,
            access_token: tokens.access_token,
            token_expires_at: expiryIso,
            amazon_user_id: null,
            scope: tokens.scope ?? null,
            amazon_profile_id: profileId,
            amazon_region: apiRegion,
            status: 'connected',
          })
          .select('id')
          .single();

        if (insertErr) {
          linkedProfiles.push({
            profileId,
            name,
            apiRegion,
            alreadyLinked: false,
            success: false,
            error: insertErr.message,
          });
          continue;
        }

        accountId = inserted.id;
        alreadyLinked = false;
      }

      linkedProfiles.push({
        profileId,
        name,
        apiRegion,
        accountId,
        alreadyLinked,
        success: true,
      });
    }

    const anySuccess = linkedProfiles.some((p) => p.success);
    return jsonResponse({
      success: anySuccess,
      linkedProfiles,
    });
  } catch (err: any) {
    console.error('finalize-amazon-link-with-profiles error', err);
    return jsonResponse(
      { success: false, error: err?.message ?? 'Unknown error' },
      500,
    );
  }
});