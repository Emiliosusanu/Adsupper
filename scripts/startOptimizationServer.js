import cron from 'node-cron';
import { runDailyOptimization } from './optimizationCron.js';
import { createServer } from 'http';
import { supabase } from '../src/lib/supabaseClient.js';

const AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID || process.env.VITE_AMAZON_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET || process.env.VITE_AMAZON_CLIENT_SECRET;

function normalizeRegion(region) {
  if (!region) return 'na';
  const r = String(region).toLowerCase();
  if (['eu', 'europe'].includes(r)) return 'eu';
  if (['fe', 'far_east', 'apac', 'asia'].includes(r)) return 'fe';
  return 'na';
}

function regionApiBase(region) {
  const r = normalizeRegion(region);
  if (r === 'eu') return 'https://advertising-api-eu.amazon.com';
  if (r === 'fe') return 'https://advertising-api-fe.amazon.com';
  return 'https://advertising-api.amazon.com';
}

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
    const t = await res.text().catch(() => '');
    throw new Error(`Token refresh failed: ${res.status} ${t}`);
  }
  const data = await res.json();
  return {
    access_token: data.access_token,
    refresh_token: data.refresh_token || refreshToken,
    expires_in: data.expires_in,
  };
}

async function withRetry(fn, label) {
  let delay = 500;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      return await fn();
    } catch (e) {
      const msg = String(e?.message || e);
      if (attempt >= 3) throw e;
      if (/goaway|server_shutting_down|http2|ECONNRESET|ETIMEDOUT|ENETUNREACH|EAI_AGAIN|429|502|503|504/i.test(msg)) {
        await new Promise((r) => setTimeout(r, delay));
        delay *= 2;
        continue;
      }
      throw e;
    }
  }
}

async function applyAmazonUpdates({ accountId, type, items }) {
  const { data: accounts, error } = await supabase
    .from('amazon_accounts')
    .select('*')
    .eq('id', accountId);
  if (error || !accounts || !accounts[0]) throw new Error('Account not found');
  const account = accounts[0];
  if (!account.refresh_token || !account.amazon_profile_id) throw new Error('Account missing refresh_token or profile');
  const apiBase = regionApiBase(account.amazon_region);
  let { access_token } = account;

  async function ensureToken() {
    try {
      const tok = await refreshAccessToken(account.refresh_token);
      access_token = tok.access_token;
      await supabase
        .from('amazon_accounts')
        .update({ access_token: tok.access_token, refresh_token: tok.refresh_token, token_expires_at: new Date(Date.now() + (tok.expires_in || 3600) * 1000).toISOString(), updated_at: new Date().toISOString(), status: 'active' })
        .eq('id', accountId);
    } catch (e) {
      throw e;
    }
  }

  await ensureToken();

  const headers = () => ({
    Authorization: `Bearer ${access_token}`,
    'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
    'Amazon-Advertising-API-Scope': String(account.amazon_profile_id),
    'Content-Type': 'application/json',
  });

  async function putJson(url, body) {
    return withRetry(async () => {
      const res = await fetch(url, { method: 'PUT', headers: headers(), body: JSON.stringify(body) });
      if (res.status === 401) {
        await ensureToken();
        const retry = await fetch(url, { method: 'PUT', headers: headers(), body: JSON.stringify(body) });
        if (!retry.ok) throw new Error(`PUT ${url} failed after refresh: ${retry.status} ${await retry.text().catch(()=>'')}`);
        return retry.json().catch(() => ({}));
      }
      if (!res.ok) throw new Error(`PUT ${url} failed: ${res.status} ${await res.text().catch(()=>'')}`);
      return res.json().catch(() => ({}));
    }, 'putJson');
  }

  if (type === 'campaign') {
    const body = items.map((it) => ({ campaignId: String(it.amazonId), budget: Number(it.value) }));
    await putJson(`${apiBase}/v2/campaigns`, body);
    for (const it of items) {
      await supabase.from('amazon_campaigns').update({ budget: Number(it.value), updated_at: new Date().toISOString() }).eq('account_id', accountId).eq('campaign_id', String(it.amazonId));
    }
    return { success: true };
  }
  if (type === 'campaign_status') {
    const body = items.map((it) => ({ campaignId: String(it.amazonId), state: String(it.value || '').toLowerCase() }));
    await putJson(`${apiBase}/v2/campaigns`, body);
    for (const it of items) {
      await supabase
        .from('amazon_campaigns')
        .update({ status: String(it.value || '').toLowerCase(), updated_at: new Date().toISOString() })
        .eq('account_id', accountId)
        .eq('campaign_id', String(it.amazonId));
    }
    return { success: true };
  }
  if (type === 'adgroup') {
    const body = items.map((it) => ({ adGroupId: String(it.amazonId), defaultBid: Number(it.value) }));
    await putJson(`${apiBase}/v2/adGroups`, body);
    for (const it of items) {
      await supabase.from('amazon_ad_groups').update({ default_bid: Number(it.value), updated_at: new Date().toISOString() }).eq('account_id', accountId).eq('amazon_ad_group_id', String(it.amazonId));
    }
    return { success: true };
  }
  if (type === 'adgroup_status') {
    const body = items.map((it) => ({ adGroupId: String(it.amazonId), state: String(it.value || '').toLowerCase() }));
    await putJson(`${apiBase}/v2/adGroups`, body);
    for (const it of items) {
      await supabase
        .from('amazon_ad_groups')
        .update({ status: String(it.value || '').toLowerCase(), updated_at: new Date().toISOString() })
        .eq('account_id', accountId)
        .eq('amazon_ad_group_id', String(it.amazonId));
    }
    return { success: true };
  }
  if (type === 'keyword') {
    const body = items.map((it) => ({ keywordId: String(it.amazonId), bid: Number(it.value) }));
    await putJson(`${apiBase}/v2/keywords/biddable`, body);
    for (const it of items) {
      await supabase.from('amazon_keywords').update({ bid: Number(it.value), updated_at: new Date().toISOString() }).eq('amazon_keyword_id', String(it.amazonId));
    }
    return { success: true };
  }
  if (type === 'keyword_status') {
    const body = items.map((it) => ({ keywordId: String(it.amazonId), state: String(it.value || '').toLowerCase() }));
    await putJson(`${apiBase}/v2/keywords`, body);
    for (const it of items) {
      await supabase
        .from('amazon_keywords')
        .update({ status: String(it.value || '').toLowerCase(), updated_at: new Date().toISOString() })
        .eq('amazon_keyword_id', String(it.amazonId));
    }
    return { success: true };
  }
  throw new Error('Unsupported type');
}

console.log('Starting optimization server...');

// Her gün saat 02:00'de optimizasyon çalıştır
const optimizationSchedule = '0 2 * * *'; // Her gün saat 02:00

// Cron job'ı başlat
const optimizationJob = cron.schedule(optimizationSchedule, async () => {
  console.log('Running scheduled optimization...');
  try {
    await runDailyOptimization();
    console.log('Scheduled optimization completed successfully');
  } catch (error) {
    console.error('Scheduled optimization failed:', error);
  }
}, {
  scheduled: true,
  timezone: "Europe/Istanbul"
});

// Server'ı başlat
console.log(`Optimization server started. Jobs will run at: ${optimizationSchedule}`);

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Stopping optimization server...');
  optimizationJob.stop();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('Stopping optimization server...');
  optimizationJob.stop();
  process.exit(0);
});

// Manuel test için endpoint (opsiyonel)
if (process.env.ENABLE_MANUAL_ENDPOINT === 'true') {
  const server = createServer(async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'healthy', timestamp: new Date().toISOString() }));
      return;
    }
    if (req.url === '/optimize' && req.method === 'POST') {
      try {
        await runDailyOptimization();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'success', message: 'Optimization completed' }));
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'error', message: error.message }));
      }
      return;
    }
    if (req.url === '/amazon/update' && req.method === 'POST') {
      try {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        const raw = Buffer.concat(chunks).toString('utf8');
        const body = raw ? JSON.parse(raw) : {};
        const { accountId, type, items } = body || {};
        if (!accountId || !type || !Array.isArray(items) || items.length === 0) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'error', message: 'Missing accountId, type, or items' }));
          return;
        }
        const result = await applyAmazonUpdates({ accountId, type, items });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'success', result }));
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'error', message: error.message }));
      }
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'not found' }));
  });

  const port = process.env.OPTIMIZATION_SERVER_PORT || 3001;
  server.listen(port, () => {
    console.log(`Optimization server listening on port ${port}`);
  });
} 