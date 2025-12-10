// supabase/functions/amazon-token-exchange/index.ts
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
const AMAZON_CLIENT_SECRET = Deno.env.get('AMAZON_CLIENT_SECRET')!;

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
      code,
      scope,
      client_id_from_state,
      redirect_uri_from_state,
      relink_account_id,
      fetch_profiles_only,
    } = body ?? {};

    if (!code || !redirect_uri_from_state) {
      return jsonResponse(
        { success: false, error: 'Missing code or redirect_uri_from_state' },
        400,
      );
    }

    // 1) Exchange auth code for tokens
    const tokenRes = await fetch('https://api.amazon.com/auth/o2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code: String(code),
        redirect_uri: String(redirect_uri_from_state),
        client_id: AMAZON_CLIENT_ID,
        client_secret: AMAZON_CLIENT_SECRET,
      }),
    });

    if (!tokenRes.ok) {
      const text = await tokenRes.text().catch(() => '');
      return jsonResponse(
        {
          success: false,
          error: `Token exchange failed: ${tokenRes.status}`,
          details: text,
        },
        400,
      );
    }

    const tokenJson: any = await tokenRes.json();
    const tokens = {
      access_token: tokenJson.access_token,
      refresh_token: tokenJson.refresh_token,
      expires_in: tokenJson.expires_in,
      scope: tokenJson.scope ?? scope ?? '',
      token_type: tokenJson.token_type,
    };

    // 2) Fetch profiles
    const profilesRes = await fetch(
      'https://advertising-api.amazon.com/v2/profiles',
      {
        headers: {
          Authorization: `Bearer ${tokens.access_token}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Content-Type': 'application/json',
        },
      },
    );

    if (!profilesRes.ok) {
      const text = await profilesRes.text().catch(() => '');
      return jsonResponse(
        {
          success: false,
          error: `Failed to fetch profiles: ${profilesRes.status}`,
          details: text,
        },
        400,
      );
    }

    const rawProfiles: any[] = await profilesRes.json();
    const availableProfiles = rawProfiles.map((p) => ({
      profileId: p.profileId,
      countryCode: p.countryCode,
      currencyCode: p.currencyCode,
      timezone: p.timezone,
      accountInfo: p.accountInfo ?? null,
      apiRegion: p.apiRegion ?? getRegionFromCountry(p.countryCode),
    }));

    let linkedProfile: any = null;

    // 3) Re-link flow: directly update existing amazon_accounts row
    if (relink_account_id) {
      const { data: { user }, error: userErr } = await supabase.auth.getUser();
      if (userErr || !user) {
        return jsonResponse(
          { success: false, error: 'Unauthorized user' },
          401,
        );
      }

      const { data: account, error: accErr } = await supabase
        .from('amazon_accounts')
        .select('*')
        .eq('id', relink_account_id)
        .eq('user_id', user.id)
        .single();

      if (accErr || !account) {
        return jsonResponse(
          { success: false, error: 'Amazon account not found for user' },
          404,
        );
      }

      let profileToUse =
        availableProfiles.find(
          (p) =>
            String(p.profileId) === String(account.amazon_profile_id),
        ) ?? availableProfiles[0];

      if (!profileToUse) {
        return jsonResponse(
          {
            success: false,
            error: 'No profiles returned from Amazon for re-link',
          },
          400,
        );
      }

      const tokenExpiry = new Date(
        Date.now() + (tokens.expires_in ?? 3600) * 1000,
      ).toISOString();

      const { error: updateErr } = await supabase
        .from('amazon_accounts')
        .update({
          access_token: tokens.access_token,
          refresh_token: tokens.refresh_token,
          token_expires_at: tokenExpiry,
          amazon_profile_id: String(profileToUse.profileId),
          amazon_region: profileToUse.apiRegion,
          scope: tokens.scope,
          status: 'active',
          updated_at: new Date().toISOString(),
        })
        .eq('id', relink_account_id)
        .eq('user_id', user.id);

      if (updateErr) {
        return jsonResponse(
          { success: false, error: 'Failed to update account', details: updateErr.message },
          500,
        );
      }

      linkedProfile = {
        profileId: profileToUse.profileId,
        name: profileToUse.accountInfo?.name ??
          `Profile ${profileToUse.profileId}`,
        apiRegion: profileToUse.apiRegion,
        alreadyLinked: false,
        success: true,
      };

      // In re-link flow, frontend will treat this as complete and skip final function
      return jsonResponse({
        success: true,
        tokens,
        availableProfiles,
        linkedProfile,
      });
    }

    // 4) New-link flow: return tokens + profiles; finalize function will save to DB
    return jsonResponse({
      success: true,
      tokens,
      availableProfiles,
      linkedProfile: null,
    });
  } catch (err: any) {
    console.error('amazon-token-exchange error', err);
    return jsonResponse(
      { success: false, error: err?.message ?? 'Unknown error' },
      500,
    );
  }
});