// scripts/syncAmazonDataVps.js
// Standalone Node.js script to sync Amazon Ads data into Supabase.
// Runs on a VPS (no Supabase Edge limits) and mirrors the logic of the
// fetch-amazon-data function: fetch campaigns, ad groups, keywords and
// 7/30-day metrics via Amazon Reporting API v3.
//
// Requirements:
//   - Node 18+ (global fetch)
//   - npm install @supabase/supabase-js
//
// Env vars (required):
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY
//   AMAZON_CLIENT_ID
//   AMAZON_CLIENT_SECRET
//
// Optional env vars:
//   ACCOUNT_ID   -> limit sync to a single amazon_accounts.id
//   DAYS_WINDOW  -> number of days for metrics (default 30)

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET;

const ACCOUNT_ID = process.env.ACCOUNT_ID || null;
const DAYS_WINDOW = Number(process.env.DAYS_WINDOW || 30);

const AMAZON_API_BASE = 'https://advertising-api.amazon.com';

if (!SUPABASE_URL || !SERVICE_ROLE_KEY || !AMAZON_CLIENT_ID || !AMAZON_CLIENT_SECRET) {
  console.error(
    'Missing env vars. Required: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, AMAZON_CLIENT_ID, AMAZON_CLIENT_SECRET',
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

async function refreshAccessToken(refreshToken) {
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

  const data = await res.json();
  return {
    access_token: data.access_token,
    refresh_token: data.refresh_token || refreshToken,
    expires_in: data.expires_in,
  };
}

// ---- Reporting v3 helpers ----

function buildDateRange(daysWindow) {
  const DAY_MS = 24 * 60 * 60 * 1000;
  const end = new Date(Date.now() - DAY_MS); // yesterday
  const endDate = end.toISOString().split('T')[0];
  const start = new Date(end.getTime() - (daysWindow - 1) * DAY_MS);
  const startDate = start.toISOString().split('T')[0];
  return { startDate, endDate };
}

async function downloadReportRows(downloadUrl) {
  const res = await fetch(downloadUrl);
  if (!res.ok) {
    console.error('Report download failed', res.status);
    return [];
  }

  const ab = await res.arrayBuffer();
  let buffer = Buffer.from(ab);

  let text;
  try {
    // Try gunzip (Amazon sends GZIP_JSON)
    const zlib = await import('zlib');
    buffer = zlib.gunzipSync(buffer);
    text = buffer.toString('utf8');
  } catch {
    text = buffer.toString('utf8');
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
      .filter((r) => r);
  }
}

async function getCampaignMetrics(profileId, accessToken, daysWindow) {
  const { startDate, endDate } = buildDateRange(daysWindow);

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
        'Amazon-Advertising-API-Scope': String(profileId),
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify(createBody),
    });

    const createJson = await createRes.json().catch(() => ({}));
    let reportId = null;

    if (createRes.ok && createJson.reportId) {
      reportId = String(createJson.reportId);
    } else if (
      createRes.status === 425 &&
      typeof createJson.detail === 'string'
    ) {
      const match = createJson.detail.match(
        /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/,
      );
      if (match) reportId = match[1];
    }

    if (!reportId) {
      console.error('Campaign report create error', createRes.status, createJson);
      if ([400, 403, 404, 405, 425].includes(createRes.status)) {
        return new Map();
      }
      throw new Error(`Campaign report failed: ${createRes.status}`);
    }

    let downloadUrl = null;
    let lastPoll = null;
    // On VPS we can safely poll longer: up to ~10 minutes (120 * 5s)
    for (let i = 0; i < 120; i++) {
      const pollRes = await fetch(`${AMAZON_API_BASE}/reporting/reports/${reportId}`, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': String(profileId),
        },
      });
      const pollJson = await pollRes.json().catch(() => ({}));
      lastPoll = pollJson;
      if (pollJson.status === 'SUCCESS' || pollJson.status === 'COMPLETED') {
        downloadUrl = pollJson.location || pollJson.url || null;
        break;
      }
      await new Promise((r) => setTimeout(r, 5000));
    }

    if (!downloadUrl) {
      console.error('Campaign report polling timed out', lastPoll);
      return new Map();
    }

    const rows = await downloadReportRows(downloadUrl);
    const byId = new Map();

    for (const row of rows) {
      const id = String(row.campaignId ?? row.campaign_id ?? '');
      if (!id) continue;

      const impressions = Number(row.impressions ?? 0) || 0;
      const clicks = Number(row.clicks ?? 0) || 0;
      const cost = Number(row.cost ?? 0) || 0;
      const orders = Number(row.purchases14d ?? row.orders ?? 0) || 0;
      const sales = Number(row.sales14d ?? row.sales ?? 0) || 0;
      const acos = sales > 0 ? cost / sales : 0;
      const ctr = impressions > 0 ? clicks / impressions : 0;
      const cpc = clicks > 0 ? cost / clicks : 0;

      byId.set(id, {
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

    return byId;
  } catch (e) {
    console.error('Error in getCampaignMetrics', e);
    return new Map();
  }
}

async function getKeywordMetrics(profileId, accessToken, daysWindow) {
  const { startDate, endDate } = buildDateRange(daysWindow);

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
        'Amazon-Advertising-API-Scope': String(profileId),
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify(createBody),
    });

    const createJson = await createRes.json().catch(() => ({}));
    let reportId = null;

    if (createRes.ok && createJson.reportId) {
      reportId = String(createJson.reportId);
    } else if (
      createRes.status === 425 &&
      typeof createJson.detail === 'string'
    ) {
      const match = createJson.detail.match(
        /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/,
      );
      if (match) reportId = match[1];
    }

    if (!reportId) {
      console.error('Keyword report create error', createRes.status, createJson);
      if ([400, 403, 404, 405, 425].includes(createRes.status)) {
        return new Map();
      }
      throw new Error(`Keyword report failed: ${createRes.status}`);
    }

    let downloadUrl = null;
    let lastPoll = null;
    // On VPS we can safely poll longer: up to ~10 minutes (120 * 5s)
    for (let i = 0; i < 120; i++) {
      const pollRes = await fetch(`${AMAZON_API_BASE}/reporting/reports/${reportId}`, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': String(profileId),
        },
      });
      const pollJson = await pollRes.json().catch(() => ({}));
      lastPoll = pollJson;
      if (pollJson.status === 'SUCCESS' || pollJson.status === 'COMPLETED') {
        downloadUrl = pollJson.location || pollJson.url || null;
        break;
      }
      await new Promise((r) => setTimeout(r, 5000));
    }

    if (!downloadUrl) {
      console.error('Keyword report polling timed out', lastPoll);
      return new Map();
    }

    const rows = await downloadReportRows(downloadUrl);
    const byKeywordId = new Map();

    for (const row of rows) {
      const keywordId = String(row.keywordId ?? row.keyword_id ?? '');
      if (!keywordId) continue;

      const impressions = Number(row.impressions ?? 0) || 0;
      const clicks = Number(row.clicks ?? 0) || 0;
      const cost = Number(row.cost ?? 0) || 0;
      const orders = Number(row.purchases14d ?? row.orders ?? 0) || 0;
      const sales = Number(row.sales14d ?? row.sales ?? 0) || 0;

      byKeywordId.set(keywordId, {
        impressions,
        clicks,
        cost,
        orders,
        sales,
      });
    }

    return byKeywordId;
  } catch (e) {
    console.error('Error in getKeywordMetrics', e);
    return new Map();
  }
}

// ---- Main sync per account ----

async function syncAccount(account) {
  console.log(`\n=== Syncing account ${account.id} (${account.name || ''}) ===`);

  let accessToken = account.access_token;
  const refreshToken = account.refresh_token;

  const now = new Date();
  const expiresAt = account.token_expires_at ? new Date(account.token_expires_at) : null;
  const needsRefresh =
    !accessToken ||
    !expiresAt ||
    expiresAt.getTime() - now.getTime() < 5 * 60 * 1000;

  if (needsRefresh) {
    if (!refreshToken) {
      console.error('No refresh token; marking account as reauth_required');
      await supabase
        .from('amazon_accounts')
        .update({ status: 'reauth_required' })
        .eq('id', account.id);
      return;
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

      console.log('Token refreshed for account', account.id);
    } catch (e) {
      console.error('Token refresh failed for account', account.id, e);
      await supabase
        .from('amazon_accounts')
        .update({ status: 'reauth_required' })
        .eq('id', account.id);
      return;
    }
  }

  if (!accessToken) {
    console.error('No access token after refresh; skipping account', account.id);
    return;
  }

  if (!account.amazon_profile_id) {
    console.error('Missing amazon_profile_id for account', account.id);
    return;
  }

  // Load existing metrics so we can carry them over if new reports are missing
  console.log('Deleting old data for account', account.id);

  const { data: oldCampaigns } = await supabase
    .from('amazon_campaigns')
    .select('id, campaign_id, spend, impressions, clicks, orders, acos, ctr, cpc')
    .eq('account_id', account.id);

  const oldCampaignMetricsByCampaignId = new Map();
  const oldCampaignIds = (oldCampaigns || []).map((c) => {
    if (c.campaign_id) {
      oldCampaignMetricsByCampaignId.set(String(c.campaign_id), {
        spend: Number(c.spend ?? 0) || 0,
        impressions: Number(c.impressions ?? 0) || 0,
        clicks: Number(c.clicks ?? 0) || 0,
        orders: Number(c.orders ?? 0) || 0,
        acos: Number(c.acos ?? 0) || 0,
        ctr: Number(c.ctr ?? 0) || 0,
        cpc: Number(c.cpc ?? 0) || 0,
      });
    }
    return c.id;
  });

  const oldKeywordMetricsByKeywordId = new Map();
  if (oldCampaignIds.length > 0) {
    const { data: oldKeywords } = await supabase
      .from('amazon_keywords')
      .select('keyword_id, spend, impressions, clicks, orders, acos')
      .in('campaign_id', oldCampaignIds);

    for (const k of oldKeywords || []) {
      if (!k.keyword_id) continue;
      oldKeywordMetricsByKeywordId.set(String(k.keyword_id), {
        spend: Number(k.spend ?? 0) || 0,
        impressions: Number(k.impressions ?? 0) || 0,
        clicks: Number(k.clicks ?? 0) || 0,
        orders: Number(k.orders ?? 0) || 0,
        acos: Number(k.acos ?? 0) || 0,
      });
    }

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

  // Fetch campaigns from Amazon v2
  console.log('Fetching campaigns from Amazon...');
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
    console.error('Failed to fetch campaigns', campaignsRes.status, text);
    return;
  }

  const amazonCampaigns = await campaignsRes.json();
  console.log('Fetched', amazonCampaigns.length, 'campaigns.');

  // Metrics via v3
  const metricsByCampaignId = await getCampaignMetrics(
    String(account.amazon_profile_id),
    accessToken,
    DAYS_WINDOW,
  );

  const campaignsToInsert = amazonCampaigns.map((c) => {
    const campaignKey = String(c.campaignId);
    const newM = metricsByCampaignId.get(campaignKey) || null;
    const oldM = oldCampaignMetricsByCampaignId.get(campaignKey) || null;
    const base = newM || oldM || {
      impressions: 0,
      clicks: 0,
      cost: 0,
      orders: 0,
      sales: 0,
      acos: 0,
      ctr: 0,
      cpc: 0,
    };

    const spend = base.cost ?? base.spend ?? 0;
    const impressions = base.impressions ?? 0;
    const clicks = base.clicks ?? 0;
    const orders = base.orders ?? 0;
    const sales = base.sales ?? 0;
    const acos = sales > 0 ? spend / sales : (base.acos ?? 0);
    const ctr = impressions > 0 ? clicks / impressions : (base.ctr ?? 0);
    const cpc = clicks > 0 ? spend / clicks : (base.cpc ?? 0);

    return {
      account_id: account.id,
      campaign_id: String(c.campaignId),
      name: c.name || `Campaign ${c.campaignId}`,
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

  let insertedCampaigns = [];
  if (campaignsToInsert.length > 0) {
    const { data, error } = await supabase
      .from('amazon_campaigns')
      .insert(campaignsToInsert)
      .select('id, campaign_id');
    if (error) {
      console.error('Insert campaigns error', error);
      return;
    }
    insertedCampaigns = data || [];
  }

  const campaignIdMap = new Map();
  for (const c of insertedCampaigns) {
    campaignIdMap.set(String(c.campaign_id), c.id);
  }

  // Fetch ad groups
  console.log('Fetching ad groups...');
  const adGroupRows = [];
  const adGroupAmazonToCampaignAmazon = new Map();

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
        console.error('Failed to fetch ad groups for campaign', campaignAmazonId, agRes.status);
        continue;
      }

      const agJson = await agRes.json();
      for (const ag of agJson) {
        const agAmazonId = String(ag.adGroupId);
        adGroupRows.push({
          account_id: account.id,
          campaign_id: parentDbId,
          name: ag.name || `Ad Group ${ag.adGroupId}`,
          status: String(ag.state ?? ag.status ?? 'unknown').toLowerCase(),
          default_bid: ag.defaultBid ?? 0,
          amazon_profile_id_text: String(account.amazon_profile_id),
          amazon_region: account.amazon_region || null,
          amazon_ad_group_id: agAmazonId,
        });
        adGroupAmazonToCampaignAmazon.set(agAmazonId, campaignAmazonId);
      }
    } catch (e) {
      console.error('Error fetching ad groups for campaign', campaignAmazonId, e);
    }
  }

  let insertedAdGroups = [];
  if (adGroupRows.length > 0) {
    const { data: agData, error: insertAgErr } = await supabase
      .from('amazon_ad_groups')
      .insert(adGroupRows)
      .select('id, amazon_ad_group_id');
    if (insertAgErr) {
      console.error('Insert ad groups error', insertAgErr);
    } else {
      insertedAdGroups = agData || [];
    }
  }

  const adGroupIdMap = new Map();
  for (const ag of insertedAdGroups) {
    adGroupIdMap.set(String(ag.amazon_ad_group_id), ag.id);
  }

  // Fetch keywords and structure
  console.log('Fetching keywords...');
  const keywordRows = [];
  const keywordAmazonIds = [];

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
        console.error('Failed to fetch keywords for ad group', agAmazonId, kwRes.status);
        continue;
      }

      const kwJson = await kwRes.json();
      for (const kw of kwJson) {
        const keywordAmazonId = String(kw.keywordId);
        keywordAmazonIds.push(keywordAmazonId);

        const oldKw = oldKeywordMetricsByKeywordId.get(keywordAmazonId) || {};

        keywordRows.push({
          campaign_id: campaignDbId,
          keyword_id: keywordAmazonId,
          text: kw.keywordText || '',
          match_type: kw.matchType || '',
          bid: kw.bid ?? 0,
          status: String(kw.state ?? 'unknown').toLowerCase(),
          spend: oldKw.spend ?? 0,
          impressions: oldKw.impressions ?? 0,
          clicks: oldKw.clicks ?? 0,
          orders: oldKw.orders ?? 0,
          acos: oldKw.acos ?? 0,
          ad_group_id: adGroupDbId,
          amazon_profile_id_text: String(account.amazon_profile_id),
          amazon_region: account.amazon_region || null,
          amazon_keyword_id: keywordAmazonId,
        });
      }
    } catch (e) {
      console.error('Error fetching keywords for ad group', agAmazonId, e);
    }
  }

  // Enrich keywords with performance metrics
  if (keywordAmazonIds.length > 0) {
    console.log('Fetching keyword metrics via v3...');
    const keywordMetricsById = await getKeywordMetrics(
      String(account.amazon_profile_id),
      accessToken,
      DAYS_WINDOW,
    );

    if (keywordMetricsById && keywordMetricsById.size > 0) {
      for (const row of keywordRows) {
        const metrics = keywordMetricsById.get(String(row.amazon_keyword_id));
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
        row.acos = sales > 0 ? spend / (sales || 1) : 0;
      }
    }
  }

  if (keywordRows.length > 0) {
    let { error: insertKwErr } = await supabase
      .from('amazon_keywords')
      .insert(keywordRows);

    // If FK to amazon_ad_groups fails for some reason, retry without ad_group_id
    if (insertKwErr && insertKwErr.code === '23503') {
      console.error('Insert keywords FK error, retrying without ad_group_id', insertKwErr);
      const rowsWithoutAdGroup = keywordRows.map(({ ad_group_id, ...rest }) => rest);
      ({ error: insertKwErr } = await supabase
        .from('amazon_keywords')
        .insert(rowsWithoutAdGroup));
    }

    if (insertKwErr) {
      console.error('Insert keywords error', insertKwErr);
    } else {
      console.log('Inserted', keywordRows.length, 'keyword rows.');
    }
  }

  await supabase
    .from('amazon_accounts')
    .update({
      last_sync: new Date().toISOString(),
      status: 'active',
    })
    .eq('id', account.id);

  console.log(
    `✅ Sync completed for account ${account.id}: ${campaignsToInsert.length} campaigns (with ${DAYS_WINDOW}-day performance).`,
  );
}

async function main() {
  console.log('▶️ Amazon -> Supabase VPS sync started at', new Date().toISOString());

  let query = supabase.from('amazon_accounts').select('*');
  if (ACCOUNT_ID) {
    query = query.eq('id', ACCOUNT_ID);
  }

  const { data: accounts, error } = await query;
  if (error) {
    console.error('Error loading amazon_accounts:', error);
    process.exit(1);
  }

  if (!accounts || !accounts.length) {
    console.log('No amazon_accounts found; nothing to sync.');
    return;
  }

  for (const account of accounts) {
    try {
      await syncAccount(account);
    } catch (e) {
      console.error(`❌ Error syncing account ${account.id}:`, e);
    }
  }

  console.log('🏁 Amazon -> Supabase VPS sync finished at', new Date().toISOString());
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error('Fatal sync error:', e);
    process.exit(1);
  });
}
