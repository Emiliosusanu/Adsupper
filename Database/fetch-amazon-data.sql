// supabase/functions/fetch-amazon-data/index.ts
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

// Base (global) host; we’ll override with region host where needed
const AMAZON_API_BASE = 'https://advertising-api.amazon.com';

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}

function normalizeRegion(region?: string | null) {
  if (!region) return 'na';
  const r = region.toLowerCase();
  if (['na', 'north_america'].includes(r)) return 'na';
  if (['eu', 'europe'].includes(r)) return 'eu';
  if (['fe', 'far_east', 'apac'].includes(r)) return 'fe';
  return 'na';
}

async function refreshAccessToken(refreshToken: string) {
  const res = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      client_id: AMAZON_CLIENT_ID,
      client_secret: AMAZON_CLIENT_SECRET,
    }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Token refresh failed: ${res.status} - ${text}`);
  }

  const data: any = await res.json();
  return {
    access_token: data.access_token,
    refresh_token: data.refresh_token || refreshToken,
    expires_in: data.expires_in,
  };
}

async function downloadReportRows(downloadUrl: string): Promise<any[]> {
  const res = await fetch(downloadUrl);
  if (!res.ok) {
    console.error('Report download failed', res.status);
    return [];
  }

  let text: string;
  try {
    const encoding = res.headers.get('content-encoding') || '';

    if (encoding.includes('gzip') || downloadUrl.endsWith('.gz')) {
      const ds = new DecompressionStream('gzip');

      if (res.body) {
        const stream = res.body.pipeThrough(ds);
        text = await new Response(stream).text();
      } else {
        const buf = new Uint8Array(await res.arrayBuffer());
        const stream = new Blob([buf]).stream().pipeThrough(ds);
        text = await new Response(stream).text();
      }
    } else {
      text = await res.text();
    }
  } catch (e) {
    console.error('GZIP decompress failed, falling back to text()', e);
    text = await res.text();
  }

  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return text
      .trim()
      .split('\n')
      .filter((l) => l.trim().length > 0)
      .map((l) => {
        try {
          return JSON.parse(l);
        } catch {
          return null;
        }
      })
      .filter((r) => r) as any[];
  }
}

/**
 * Fetch campaign-level performance via Amazon Reporting API v3
 * and aggregate metrics per campaignId.
 */
async function getCampaignMetricsFromKeywordReport(
  region: string,
  profileId: string,
  accessToken: string,
  campaignIds: string[],
  daysWindow = 7,
) {
  if (!campaignIds || campaignIds.length === 0) {
    return new Map<string, any>();
  }

  // Use only fully finalized days for reporting: endDate = yesterday.
  const DAY_MS = 24 * 60 * 60 * 1000;
  const end = new Date(Date.now() - DAY_MS);
  const endDate = end.toISOString().split('T')[0];
  const start = new Date(end.getTime() - (daysWindow - 1) * DAY_MS);
  const startDate = start.toISOString().split('T')[0];

  const createBody = {
    name: 'Robotads campaign performance',
    startDate,
    endDate,
    configuration: {
      adProduct: 'SPONSORED_PRODUCTS',
      reportTypeId: 'spCampaigns',
      timeUnit: 'SUMMARY',
      groupBy: ['campaign'],
      columns: [
        'campaignId',
        'impressions',
        'clicks',
        'cost',
        'purchases14d',
        'sales14d',
      ],
      format: 'GZIP_JSON',
    },
  };

  try {
    const createRes = await fetch(`${AMAZON_API_BASE}/reporting/reports`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
        'Amazon-Advertising-API-Scope': profileId,
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify(createBody),
    });

    const createJson: any = await createRes.json().catch(() => ({}));
    let reportId: string | null = null;

    if (createRes.ok && createJson.reportId) {
      reportId = String(createJson.reportId);
    } else if (
      createRes.status === 425 &&
      typeof createJson.detail === 'string'
    ) {
      // Duplicate request; extract existing reportId from the detail message
      const match =
        createJson.detail.match(
          /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/,
        );
      if (match) {
        reportId = match[1];
      } else {
        console.error(
          'Campaign performance duplicate error but no reportId in detail',
          createJson,
        );
      }
    }

    if (!reportId) {
      console.error(
        'Campaign performance API error',
        createRes.status,
        createJson,
      );
      if (
        createRes.status === 400 ||
        createRes.status === 403 ||
        createRes.status === 404 ||
        createRes.status === 405 ||
        createRes.status === 425
      ) {
        // Reporting not available; fall back to zero metrics
        return new Map<string, any>();
      }
      throw new Error(`Campaign performance failed: ${createRes.status}`);
    }

    // Poll until report is ready
    let downloadUrl: string | null = null;
    let lastPollJson: any = null;
    // Poll for up to ~60 seconds (20 * 3s) to keep within Edge Function limits
    for (let i = 0; i < 20; i++) {
      const pollRes = await fetch(
        `${AMAZON_API_BASE}/reporting/reports/${reportId}`,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
            'Amazon-Advertising-API-Scope': profileId,
          },
        },
      );
      const pollJson: any = await pollRes.json().catch(() => ({}));
      lastPollJson = pollJson;
      if (
        pollJson.status === 'SUCCESS' ||
        pollJson.status === 'COMPLETED'
      ) {
        downloadUrl = pollJson.location || pollJson.url || null;
        break;
      }
      await new Promise((r) => setTimeout(r, 3000));
    }

    if (!downloadUrl) {
      console.error(
        'Campaign performance report polling timed out or failed',
        lastPollJson,
      );
      return new Map<string, any>();
    }

    const perf = await downloadReportRows(downloadUrl);
    const metricsByCampaignId = new Map<
      string,
      {
        impressions: number;
        clicks: number;
        cost: number;
        orders: number;
        sales: number;
        acos: number;
        ctr: number;
        cpc: number;
      }
    >();

    for (const row of perf) {
      const id = String(row.campaignId ?? row.campaign_id);
      if (!id) continue;

      const impressions = Number(row.impressions ?? 0) || 0;
      const clicks = Number(row.clicks ?? 0) || 0;
      const cost = Number(row.cost ?? 0) || 0;
      const orders =
        Number(row.purchases14d ?? row.orders ?? 0) || 0;
      const sales = Number(row.sales14d ?? row.sales ?? 0) || 0;
      const acos =
        Number(row.acos ?? 0) || (sales > 0 ? cost / sales : 0);
      const ctr =
        Number(row.ctr ?? 0) ||
        (impressions > 0 ? clicks / impressions : 0);
      const cpc =
        Number(row.cpc ?? 0) || (clicks > 0 ? cost / clicks : 0);

      metricsByCampaignId.set(id, {
        impressions,
        clicks,
        cost,
        orders,
        sales,
        acos,
        ctr,
        cpc,
      });
    }

    return metricsByCampaignId;
  } catch (e) {
    console.error('Error in getCampaignMetricsFromKeywordReport', e);
    return new Map<string, any>();
  }
}

async function getKeywordMetricsFromReport(
  region: string,
  profileId: string,
  accessToken: string,
  keywordIds: string[],
  daysWindow = 7,
) {
  if (!keywordIds || keywordIds.length === 0) {
    return new Map<string, any>();
  }

  // Use only fully finalized days: endDate = yesterday
  const DAY_MS = 24 * 60 * 60 * 1000;
  const end = new Date(Date.now() - DAY_MS);
  const endDate = end.toISOString().split('T')[0];
  const start = new Date(end.getTime() - (daysWindow - 1) * DAY_MS);
  const startDate = start.toISOString().split('T')[0];

  const createBody = {
    name: 'Robotads keyword performance',
    startDate,
    endDate,
    configuration: {
      adProduct: 'SPONSORED_PRODUCTS',
      reportTypeId: 'spTargeting',
      timeUnit: 'SUMMARY',
      groupBy: ['targeting'],
      columns: [
        'campaignId',
        'adGroupId',
        'keywordId',
        'keyword',
        'matchType',
        'impressions',
        'clicks',
        'cost',
        'purchases14d',
        'sales14d',
      ],
      filters: [
        {
          field: 'keywordType',
          values: ['BROAD', 'PHRASE', 'EXACT'],
        },
      ],
      format: 'GZIP_JSON',
    },
  };

  try {
    const createRes = await fetch(`${AMAZON_API_BASE}/reporting/reports`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
        'Amazon-Advertising-API-Scope': profileId,
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify(createBody),
    });

    const createJson: any = await createRes.json().catch(() => ({}));

    let reportId: string | null = null;

    if (createRes.ok && createJson.reportId) {
      reportId = String(createJson.reportId);
    } else if (
      createRes.status === 425 &&
      typeof createJson.detail === 'string'
    ) {
      // Duplicate request; extract existing reportId from the detail message
      const match =
        createJson.detail.match(
          /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/,
        );
      if (match) {
        reportId = match[1];
      }
    }

    if (!reportId) {
      console.error('Keyword report creation failed', createRes.status, createJson);
      if (
        createRes.status === 400 ||
        createRes.status === 403 ||
        createRes.status === 404 ||
        createRes.status === 405 ||
        createRes.status === 425
      ) {
        return new Map<string, any>();
      }
      throw new Error(
        `Keyword report creation failed: ${createRes.status} ${JSON.stringify(
          createJson,
        )}`,
      );
    }

    const finalReportId = String(reportId);

    let downloadUrl: string | null = null;
    let lastPollJson: any = null;
    // Poll for up to ~72 seconds (24 * 3s) to keep within Edge Function limits
    for (let i = 0; i < 24; i++) {
      const pollRes = await fetch(
        `${AMAZON_API_BASE}/reporting/reports/${finalReportId}`,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
            'Amazon-Advertising-API-Scope': profileId,
          },
        },
      );
      const pollJson: any = await pollRes.json().catch(() => ({}));
      lastPollJson = pollJson;
      if (
        pollJson.status === 'SUCCESS' ||
        pollJson.status === 'COMPLETED'
      ) {
        downloadUrl = pollJson.location || pollJson.url || null;
        break;
      }
      await new Promise((r) => setTimeout(r, 3000));
    }

    if (!downloadUrl) {
      console.error('Keyword report polling timed out or failed.', lastPollJson);
      return new Map<string, any>();
    }

    const rows = await downloadReportRows(downloadUrl);

    const metricsByKeywordId = new Map<
      string,
      {
        impressions: number;
        clicks: number;
        cost: number;
        orders: number;
        sales: number;
      }
    >();

    for (const row of rows) {
      const keywordId = String(row.keywordId ?? row.keyword_id ?? '');
      if (!keywordId) continue;

      const impressions = Number(row.impressions ?? 0) || 0;
      const clicks = Number(row.clicks ?? 0) || 0;
      const cost = Number(row.cost ?? 0) || 0;
      const orders =
        Number(row.purchases14d ?? row.orders ?? 0) || 0;
      const sales = Number(row.sales14d ?? row.sales ?? 0) || 0;

      metricsByKeywordId.set(keywordId, {
        impressions,
        clicks,
        cost,
        orders,
        sales,
      });
    }

    // Optionally filter to the specific keywordIds we care about
    if (keywordIds && keywordIds.length > 0) {
      const set = new Set(keywordIds.map((id) => String(id)));
      for (const key of Array.from(metricsByKeywordId.keys())) {
        if (!set.has(key)) {
          metricsByKeywordId.delete(key);
        }
      }
    }

    return metricsByKeywordId;
  } catch (e) {
    console.error('Error in getKeywordMetricsFromReport', e);
    return new Map<string, any>();
  }
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

    if (!account_id) {
      return jsonResponse(
        { success: false, error: 'account_id is required' },
        400,
      );
    }

    // 1) Load account
    const { data: account, error: accErr } = await supabase
      .from('amazon_accounts')
      .select('*')
      .eq('id', account_id)
      .maybeSingle();

    if (accErr || !account) {
      return jsonResponse(
        { success: false, error: 'Amazon account not found.' },
        404,
      );
    }

    if (user_id && account.user_id !== user_id) {
      return jsonResponse(
        { success: false, error: 'Account does not belong to this user.' },
        403,
      );
    }

    if (!account.amazon_profile_id) {
      return jsonResponse({
        success: false,
        error: 'Amazon Profile ID is missing for this account.',
      });
    }

    const region = normalizeRegion(account.amazon_region);
    let accessToken: string | null = account.access_token;
    const refreshToken: string | null = account.refresh_token;

    // 2) Ensure valid token
    const now = new Date();
    const expiresAt = account.token_expires_at
      ? new Date(account.token_expires_at)
      : null;
    const needsRefresh =
      !accessToken ||
      !expiresAt ||
      expiresAt.getTime() - now.getTime() < 5 * 60 * 1000;

    if (needsRefresh) {
      if (!refreshToken) {
        await supabase
          .from('amazon_accounts')
          .update({ status: 'reauth_required' })
          .eq('id', account.id);
        return jsonResponse({
          success: false,
          error: 'Re-authentication required: no refresh token stored.',
          needsReauth: true,
        });
      }

      try {
        const refreshed = await refreshAccessToken(refreshToken);
        accessToken = refreshed.access_token;

        const newExpiry = new Date(
          Date.now() + (refreshed.expires_in ?? 3600) * 1000,
        ).toISOString();

        await supabase
          .from('amazon_accounts')
          .update({
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            token_expires_at: newExpiry,
            updated_at: new Date().toISOString(),
            status: 'active',
          })
          .eq('id', account.id);
      } catch (e: any) {
        console.error('Token refresh failed in fetch-amazon-data', e);
        await supabase
          .from('amazon_accounts')
          .update({ status: 'reauth_required' })
          .eq('id', account.id);
        return jsonResponse({
          success: false,
          error: `Re-authentication required: ${e.message ?? 'Token refresh failed.'}`,
          needsReauth: true,
        });
      }
    }

    if (!accessToken) {
      return jsonResponse({
        success: false,
        error: 'Access token missing after refresh. Re-authentication required.',
        needsReauth: true,
      });
    }

    // 3) Delete old data for this account (structure only)
    const { data: oldCampaigns } = await supabase
      .from('amazon_campaigns')
      .select('id')
      .eq('account_id', account.id);

    const oldCampaignIds = (oldCampaigns ?? []).map((c: any) => c.id);

    if (oldCampaignIds.length > 0) {
      await supabase
        .from('amazon_keywords')
        .delete()
        .in('campaign_id', oldCampaignIds);
    }

    await supabase
      .from('amazon_ad_groups')
      .delete()
      .eq('account_id', account.id);

    await supabase
      .from('amazon_campaigns')
      .delete()
      .eq('account_id', account.id);

    // 4) Fetch campaigns (structure) from Amazon
    const campaignsRes = await fetch(`${AMAZON_API_BASE}/v2/campaigns`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
        'Amazon-Advertising-API-Scope': String(account.amazon_profile_id),
        'Content-Type': 'application/json',
      },
    });

    if (!campaignsRes.ok) {
      const text = await campaignsRes.text().catch(() => '');
      if (campaignsRes.status === 401 || campaignsRes.status === 403) {
        await supabase
          .from('amazon_accounts')
          .update({ status: 'reauth_required' })
          .eq('id', account.id);
        return jsonResponse({
          success: false,
          error: `Re-authentication required: Amazon API returned ${campaignsRes.status}.`,
          needsReauth: true,
        });
      }

      return jsonResponse({
        success: false,
        error: `Failed to fetch campaigns: ${campaignsRes.status}`,
        details: text,
      });
    }

    const amazonCampaigns: any[] = await campaignsRes.json();

    // 5) Fetch performance metrics (via campaign reports) and aggregate to campaigns
    const campaignIdsForReport = amazonCampaigns.map((c) =>
      String(c.campaignId),
    );
    const metricsByCampaignId = await getCampaignMetricsFromKeywordReport(
      region,
      String(account.amazon_profile_id),
      accessToken,
      campaignIdsForReport,
      7,
    );

    const campaignsToInsert = amazonCampaigns.map((c) => {
      const m = metricsByCampaignId.get(String(c.campaignId)) ?? {
        impressions: 0,
        clicks: 0,
        cost: 0,
        orders: 0,
        sales: 0,
      };

      const spend = m.cost;
      const impressions = m.impressions;
      const clicks = m.clicks;
      const orders = m.orders;
      const sales = m.sales;
      const acos = sales > 0 ? spend / sales : 0;
      const ctr = impressions > 0 ? clicks / impressions : 0;
      const cpc = clicks > 0 ? spend / clicks : 0;

      return {
        account_id: account.id,
        campaign_id: String(c.campaignId),
        name: c.name ?? `Campaign ${c.campaignId}`,
        status: String(c.state ?? c.status ?? 'unknown').toLowerCase(),
        budget: c.dailyBudget ?? 0,
        spend,
        impressions,
        clicks,
        orders,
        acos,
        ctr,
        cpc,
        amazon_campaign_id_text: String(c.campaignId),
        amazon_profile_id_text: String(account.amazon_profile_id),
        raw_data: {
          ...c,
          spend,
          impressions,
          clicks,
          orders,
          sales,
          acos,
          ctr,
          cpc,
        },
      };
    });

    let insertedCampaigns: any[] = [];
    if (campaignsToInsert.length > 0) {
      const { data, error: insertCampErr } = await supabase
        .from('amazon_campaigns')
        .insert(campaignsToInsert)
        .select('id, campaign_id');

      if (insertCampErr) {
        console.error('Insert campaigns error', insertCampErr);
        return jsonResponse({
          success: false,
          error: 'Failed to save campaigns to database.',
          details: insertCampErr.message,
        });
      }

      insertedCampaigns = data ?? [];
    }

    // Build map of Amazon campaignId -> internal campaign row id
    const campaignIdMap = new Map<string, string>();
    for (const c of insertedCampaigns) {
      campaignIdMap.set(String(c.campaign_id), c.id);
    }

    // 6) Fetch and insert ad groups (structure only)
    const adGroupRows: any[] = [];
    const adGroupAmazonToCampaignAmazon = new Map<string, string>();

    for (const c of amazonCampaigns) {
      const campaignAmazonId = String(c.campaignId);
      const parentDbId = campaignIdMap.get(campaignAmazonId);
      if (!parentDbId) continue;

      try {
        const agRes = await fetch(
          `${AMAZON_API_BASE}/v2/adGroups?campaignIdFilter=${campaignAmazonId}`,
          {
            headers: {
              Authorization: `Bearer ${accessToken}`,
              'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
              'Amazon-Advertising-API-Scope': String(account.amazon_profile_id),
              'Content-Type': 'application/json',
            },
          },
        );

        if (!agRes.ok) {
          console.error(
            'Failed to fetch ad groups for campaign',
            campaignAmazonId,
            agRes.status,
          );
          continue;
        }

        const agJson: any[] = await agRes.json();
        for (const ag of agJson) {
          const agAmazonId = String(ag.adGroupId);
          adGroupRows.push({
            account_id: account.id,
            campaign_id: parentDbId,
            name: ag.name ?? `Ad Group ${ag.adGroupId}`,
            status: String(ag.state ?? ag.status ?? 'unknown').toLowerCase(),
            default_bid: ag.defaultBid ?? 0,
            amazon_profile_id_text: String(account.amazon_profile_id),
            amazon_region: account.amazon_region ?? null,
            amazon_ad_group_id: agAmazonId,
          });
          adGroupAmazonToCampaignAmazon.set(agAmazonId, campaignAmazonId);
        }
      } catch (e) {
        console.error('Error fetching ad groups for campaign', campaignAmazonId, e);
      }
    }

    let insertedAdGroups: any[] = [];
    if (adGroupRows.length > 0) {
      const { data: agData, error: insertAgErr } = await supabase
        .from('amazon_ad_groups')
        .insert(adGroupRows)
        .select('id, amazon_ad_group_id');

      if (insertAgErr) {
        console.error('Insert ad groups error', insertAgErr);
      } else {
        insertedAdGroups = agData ?? [];
      }
    }

    const adGroupIdMap = new Map<string, string>();
    for (const ag of insertedAdGroups) {
      adGroupIdMap.set(String(ag.amazon_ad_group_id), ag.id);
    }

    // 7) Fetch and insert keywords (structure + performance)
    const keywordRows: any[] = [];
    const keywordAmazonIds: string[] = [];

    for (const [agAmazonId, campaignAmazonId] of adGroupAmazonToCampaignAmazon.entries()) {
      const adGroupDbId = adGroupIdMap.get(agAmazonId);
      const campaignDbId = campaignAmazonId
        ? campaignIdMap.get(campaignAmazonId)
        : null;
      if (!adGroupDbId) continue;

      try {
        const kwRes = await fetch(
          `${AMAZON_API_BASE}/v2/keywords?adGroupIdFilter=${agAmazonId}`,
          {
            headers: {
              Authorization: `Bearer ${accessToken}`,
              'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
              'Amazon-Advertising-API-Scope': String(account.amazon_profile_id),
              'Content-Type': 'application/json',
            },
          },
        );

        if (!kwRes.ok) {
          console.error(
            'Failed to fetch keywords for ad group',
            agAmazonId,
            kwRes.status,
          );
          continue;
        }

        const kwJson: any[] = await kwRes.json();
        for (const kw of kwJson) {
          const keywordAmazonId = String(kw.keywordId);
          keywordAmazonIds.push(keywordAmazonId);

          keywordRows.push({
            campaign_id: campaignDbId,
            keyword_id: keywordAmazonId,
            text: kw.keywordText ?? '',
            match_type: kw.matchType ?? '',
            bid: kw.bid ?? 0,
            status: String(kw.state ?? 'unknown').toLowerCase(),
            spend: 0,
            impressions: 0,
            clicks: 0,
            orders: 0,
            acos: 0,
            ad_group_id: adGroupDbId,
            amazon_profile_id_text: String(account.amazon_profile_id),
            amazon_region: account.amazon_region ?? null,
            amazon_keyword_id: keywordAmazonId,
          });
        }
      } catch (e) {
        console.error('Error fetching keywords for ad group', agAmazonId, e);
      }
    }
    // Enrich keywords with performance metrics (last 30 days)
    if (keywordAmazonIds.length > 0) {
      const uniqueKeywordIds = Array.from(new Set(keywordAmazonIds));
      const keywordMetricsById = await getKeywordMetricsFromReport(
        region,
        String(account.amazon_profile_id),
        accessToken,
        uniqueKeywordIds,
        7,
      );

      if (keywordMetricsById && keywordMetricsById.size > 0) {
        for (const row of keywordRows) {
          const metrics = keywordMetricsById.get(
            String(row.amazon_keyword_id),
          );
          if (!metrics) continue;

          const spend = metrics.cost;
          const impressions = metrics.impressions;
          const clicks = metrics.clicks;
          const orders = metrics.orders;
          const sales = metrics.sales;

          row.spend = spend;
          row.impressions = impressions;
          row.clicks = clicks;
          row.orders = orders;
          row.acos = sales > 0 ? (spend / (sales || 1)) : 0;
        }
      }
    }

    if (keywordRows.length > 0) {
      const { error: insertKwErr } = await supabase
        .from('amazon_keywords')
        .insert(keywordRows);
      if (insertKwErr) {
        console.error('Insert keywords error', insertKwErr);
      }
    }

    // 8) Update account status
    await supabase
      .from('amazon_accounts')
      .update({
        last_sync: new Date().toISOString(),
        status: 'active',
      })
      .eq('id', account.id);

    return jsonResponse({
      success: true,
      message: `Sync completed: ${campaignsToInsert.length} campaigns (with 7-day performance).`,
    });
  } catch (err: any) {
    console.error('fetch-amazon-data error', err);
    return jsonResponse(
      { success: false, error: err?.message ?? 'Unknown error during sync.' },
      500,
    );
  }
});