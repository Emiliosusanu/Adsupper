// scripts/optimizerVps.js
// Fully server-side keyword optimizer for Robotads, to run on a VPS.
// Uses Supabase (service role) + Amazon Ads API v2 and the same
// optimization_rules table as the frontend.
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
//   ACCOUNT_ID  -> limit to a single amazon_accounts.id
//   USER_ID     -> limit to rules/accounts of a single auth.users.id

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET;

const ACCOUNT_ID = process.env.ACCOUNT_ID || null;
const USER_ID = process.env.USER_ID || null;

const AMAZON_API_BASE = 'https://advertising-api.amazon.com';

if (!SUPABASE_URL || !SERVICE_ROLE_KEY || !AMAZON_CLIENT_ID || !AMAZON_CLIENT_SECRET) {
  console.error(
    'Missing env vars. Required: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, AMAZON_CLIENT_ID, AMAZON_CLIENT_SECRET',
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

// ---- helpers ----

function nowIso() {
  return new Date().toISOString();
}

function hoursBetween(a, b) {
  const t1 = new Date(a).getTime();
  const t2 = new Date(b).getTime();
  return Math.abs(t1 - t2) / (1000 * 60 * 60);
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

// metrics from DB row
function getMetricValue(row, metric) {
  const m = String(metric || '').toLowerCase();
  const spend = Number(row.spend ?? 0) || 0;
  const impressions = Number(row.impressions ?? 0) || 0;
  const clicks = Number(row.clicks ?? 0) || 0;
  const orders = Number(row.orders ?? 0) || 0;
  const acos = Number(row.acos ?? 0) || 0;

  switch (m) {
    case 'spend':
      return spend;
    case 'impressions':
      return impressions;
    case 'clicks':
      return clicks;
    case 'orders':
      return orders;
    case 'acos':
      return acos;
    case 'ctr':
      return impressions > 0 ? clicks / impressions : 0;
    case 'cpc':
      return clicks > 0 ? spend / clicks : 0;
    default: {
      const v = Number(row[m]);
      return Number.isFinite(v) ? v : 0;
    }
  }
}

function compareMetric(value, op, target) {
  const t = Number(target ?? 0);
  switch (op) {
    case 'greater_than':
    case '>':
      return value > t;
    case 'less_than':
    case '<':
      return value < t;
    case 'greater_or_equal':
    case '>=':
      return value >= t;
    case 'less_or_equal':
    case '<=':
      return value <= t;
    case 'equals':
    case '==':
    case '=':
      return Math.abs(value - t) < 1e-6;
    default:
      return false;
  }
}

function ruleDue(rule) {
  const settings = rule.settings || {};
  // main: frequency_days; fallback: frequency_hours / 24; default: 1 day
  const freqDays = settings.frequency_days != null
    ? Number(settings.frequency_days) || 0
    : (settings.frequency_hours != null
        ? (Number(settings.frequency_hours) || 0) / 24
        : 1);

  if (freqDays <= 0) return true;
  if (!rule.last_run) return true;

  const hoursSinceLast = hoursBetween(rule.last_run, nowIso());
  return hoursSinceLast >= freqDays * 24;
}

function filterKeywordsByScope(keywords, scope) {
  if (!scope || scope.type === 'ALL') return keywords;
  const type = scope.type;

  if (type === 'CAMPAIGNS' && Array.isArray(scope.campaign_ids) && scope.campaign_ids.length) {
    const set = new Set(scope.campaign_ids.map(String));
    return keywords.filter((k) => k.campaign_id && set.has(String(k.campaign_id)));
  }

  if (type === 'KEYWORDS' && Array.isArray(scope.keyword_ids) && scope.keyword_ids.length) {
    const set = new Set(scope.keyword_ids.map(String));
    return keywords.filter((k) => k.id && set.has(String(k.id)));
  }

  return keywords;
}

async function sendBidAdjustments(account, accessToken, actions) {
  const results = [];

  for (const a of actions) {
    const endpoint = `${AMAZON_API_BASE}/v2/sp/keywords/${a.keywordAmazonId}`;
    const payload = {};

    if (a.type === 'pause') {
      payload.state = 'paused';
    } else if (a.type === 'adjust_bid_percentage') {
      payload.bid = a.newBid;
    }

    try {
      const res = await fetch(endpoint, {
        method: 'PUT',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': String(account.amazon_profile_id),
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      let json = {};
      try {
        json = await res.json();
      } catch {}

      results.push({
        keywordAmazonId: a.keywordAmazonId,
        status: res.status,
        response: json,
      });
    } catch (e) {
      console.error('Error sending adjustment for keyword', a.keywordAmazonId, e);
      results.push({
        keywordAmazonId: a.keywordAmazonId,
        status: 0,
        response: { error: String(e.message || e) },
      });
    }
  }

  return results;
}

async function logActions(account, rule, actions, resultsByKeyword) {
  if (!actions.length) return;

  const rows = actions.map((a) => {
    const r = resultsByKeyword.get(a.keywordAmazonId);
    const status = r?.status ?? 0;
    const actionName = a.logAction || a.type;

    return {
      user_id: account.user_id,
      campaign_id: a.campaignId,
      keyword_id: a.keywordRowId,
      action: actionName,
      reason: `Rule ${rule.name}`,
      rule_id: rule.id,
      amazon_account_id: account.id,
      entity_type: 'keyword',
      entity_id: a.keywordAmazonId,
      details: {
        metrics_snapshot: a.metricsSnapshot,
        action: {
          ui_action: actionName,
          api_type: a.type,
          current_bid: a.currentBid,
          new_bid: a.newBid,
        },
        api_status: status,
        api_response: r?.response ?? null,
      },
      created_at: nowIso(),
    };
  });

  const { error } = await supabase.from('optimization_logs').insert(rows);
  if (error) {
    console.error('Failed to insert optimization_logs:', error);
  }
}

async function logJob(status, message, errorDetails = null) {
  const now = nowIso();
  const { error } = await supabase.from('optimization_job_logs').insert({
    job_type: 'keyword_optimizer_vps',
    status,
    message,
    started_at: now,
    completed_at: now,
    error_details: errorDetails,
  });
  if (error) console.error('Error logging job:', error);
}

// Build actions for one rule and a list of keywords
function buildActionsForRule(rule, account, keywordsForAccount) {
  const settings = rule.settings || {};
  const scope = settings.scope || {};
  const conditions = Array.isArray(settings.conditions)
    ? settings.conditions
    : settings.metric
    ? [{ metric: settings.metric, comparison: settings.condition || settings.condition_op || '>', value: settings.threshold }]
    : [];
  const actionConf = settings.action || {};

  const entity = (settings.entity || 'keyword').toLowerCase();
  if (entity !== 'keyword') return [];

  const scopedKeywords = filterKeywordsByScope(keywordsForAccount, scope);
  const actions = [];

  for (const kw of scopedKeywords) {
    let allMatch = true;
    const snapshot = {};

    for (const cond of conditions) {
      const metricName = cond.metric;
      const val = getMetricValue(kw, metricName);
      snapshot[metricName] = val;
      const cmp = cond.comparison || cond.condition || '>';
      const target = cond.value != null ? cond.value : cond.threshold;
      if (!compareMetric(val, cmp, target)) {
        allMatch = false;
        break;
      }
    }

    if (!allMatch) continue;

    const currentBid = Number(kw.bid ?? 0) || 0;
    // settings.action is the user-facing action string from the UI
    const uiAction = typeof settings.action === 'string'
      ? settings.action
      : (actionConf.type || 'adjust_bid_percentage');

    // Map UI action to low-level API type
    let type = uiAction;
    if (uiAction === 'pause_keyword') {
      type = 'pause';
    } else if (uiAction === 'decrease_bid' || uiAction === 'increase_bid') {
      type = 'adjust_bid_percentage';
    }

    let newBid = currentBid;

    if (type === 'adjust_bid_percentage') {
      const rawPct = Number(actionConf.value ?? settings.action_value ?? 0) || 0;
      const pct = uiAction === 'decrease_bid'
        ? -Math.abs(rawPct)
        : uiAction === 'increase_bid'
        ? Math.abs(rawPct)
        : rawPct;
      const raw = currentBid * (1 + pct / 100);
      const minBid = Number(actionConf.min_bid ?? 0.02);
      const maxBid = Number(actionConf.max_bid ?? 10.0);
      newBid = Math.min(maxBid, Math.max(minBid, Number(raw.toFixed(2))));
    }

    const act = {
      type,
      logAction: uiAction,
      keywordAmazonId: String(kw.keyword_id),
      keywordRowId: kw.id,
      campaignId: kw.campaign_id,
      currentBid,
      newBid,
      metricsSnapshot: snapshot,
    };

    actions.push(act);
  }

  return actions;
}

async function loadActiveRules() {
  let query = supabase.from('optimization_rules').select('*').eq('enabled', true);
  if (USER_ID) query = query.eq('user_id', USER_ID);
  const { data, error } = await query;
  if (error) throw new Error(`Failed to load optimization_rules: ${error.message}`);
  return data || [];
}

async function loadAccounts() {
  let query = supabase.from('amazon_accounts').select('*').eq('status', 'active');
  if (ACCOUNT_ID) query = query.eq('id', ACCOUNT_ID);
  if (USER_ID) query = query.eq('user_id', USER_ID);
  const { data, error } = await query;
  if (error) throw new Error(`Failed to load amazon_accounts: ${error.message}`);
  return data || [];
}

async function processAccount(account, rulesForUser) {
  console.log(`\n▶️ Optimizing account ${account.id} (${account.name || ''})`);

  // Token refresh
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
      console.error('No refresh token for account', account.id);
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
          updated_at: nowIso(),
          status: 'active',
        })
        .eq('id', account.id);
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

  // campaigns for this account
  const { data: campaigns, error: campErr } = await supabase
    .from('amazon_campaigns')
    .select('id')
    .eq('account_id', account.id);
  if (campErr) {
    console.error('Failed to load campaigns for account', account.id, campErr.message);
    return;
  }

  const campaignIds = (campaigns || []).map((c) => c.id);
  if (!campaignIds.length) {
    console.log('No campaigns for account', account.id);
    return;
  }

  // load keywords + metrics from DB
  const { data: keywords, error: kwErr } = await supabase
    .from('amazon_keywords')
    .select('id, campaign_id, keyword_id, text, match_type, bid, status, spend, impressions, clicks, orders, acos')
    .in('campaign_id', campaignIds);
  if (kwErr) {
    console.error('Failed to load keywords for account', account.id, kwErr.message);
    return;
  }

  const keywordsForAccount = keywords || [];
  if (!keywordsForAccount.length) {
    console.log('No keywords for account', account.id);
    return;
  }

  for (const rule of rulesForUser) {
    if (!ruleDue(rule)) {
      console.log(`Skipping rule ${rule.name} for account ${account.id} (frequency)`);
      continue;
    }

    const actions = buildActionsForRule(rule, account, keywordsForAccount);
    if (!actions.length) {
      console.log(`Rule ${rule.name} produced no actions for account ${account.id}`);
      // Still update last_run to avoid constant re-evaluation
      await supabase
        .from('optimization_rules')
        .update({ last_run: nowIso() })
        .eq('id', rule.id);
      continue;
    }

    console.log(`Rule ${rule.name} -> ${actions.length} action(s) for account ${account.id}`);

    const results = await sendBidAdjustments(account, accessToken, actions);
    const byKw = new Map(results.map((r) => [String(r.keywordAmazonId), r]));
    await logActions(account, rule, actions, byKw);

    await supabase
      .from('optimization_rules')
      .update({ last_run: nowIso() })
      .eq('id', rule.id);
  }
}

async function main() {
  console.log('▶️ VPS Keyword Optimizer started at', nowIso());

  try {
    const [rules, accounts] = await Promise.all([
      loadActiveRules(),
      loadAccounts(),
    ]);

    if (!rules.length) {
      console.log('No enabled rules. Nothing to do.');
      await logJob('success', 'No rules to run');
      return;
    }

    if (!accounts.length) {
      console.log('No active amazon_accounts. Nothing to do.');
      await logJob('success', 'No accounts to optimize');
      return;
    }

    // group rules by user_id
    const rulesByUser = new Map();
    for (const r of rules) {
      if (!r.user_id) continue;
      const arr = rulesByUser.get(r.user_id) || [];
      arr.push(r);
      rulesByUser.set(r.user_id, arr);
    }

    for (const account of accounts) {
      const r = rulesByUser.get(account.user_id) || [];
      if (!r.length) {
        console.log(`No rules for user ${account.user_id}; skipping account ${account.id}`);
        continue;
      }

      try {
        await processAccount(account, r);
      } catch (e) {
        console.error(`Error optimizing account ${account.id}:`, e);
      }
    }

    console.log('🏁 VPS Keyword Optimizer finished at', nowIso());
    await logJob('success', 'Keyword optimizer finished successfully');
  } catch (e) {
    console.error('Fatal optimizer error:', e);
    await logJob('error', 'Keyword optimizer failed', { error: String(e.message || e) });
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error('Fatal error in main()', e);
    process.exit(1);
  });
}
