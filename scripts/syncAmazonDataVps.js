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
//   DAYS_WINDOW  -> number of days for metrics (default 7)

import { createClient } from "@supabase/supabase-js";
import { randomUUID } from "node:crypto";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET;

const ACCOUNT_ID = process.env.ACCOUNT_ID || null;
// Default to 7 days (same as Edge Function). Can be overridden via env.
const DAYS_WINDOW = Number(process.env.DAYS_WINDOW || 7);
const SKIP_METRICS =
  process.env.SKIP_METRICS === "1" ||
  process.env.SKIP_METRICS === "true" ||
  process.env.SKIP_METRICS === "TRUE";
// Tunable reporting wait and poll interval
const REPORT_MAX_MIN = Number(process.env.REPORT_MAX_MIN || 30);
const REPORT_POLL_INTERVAL_MS = Number(
  process.env.REPORT_POLL_INTERVAL_MS || 5000
);
// Strict mode: only persist campaign metrics when the spCampaigns report is ready.
// Default OFF (false) to allow fallback to aggregation from keywords if campaign report fails.
const STRICT_ONLY_REPORTS = (() => {
  const v = process.env.STRICT_ONLY_REPORTS;
  if (v == null) return false;
  return !(v === "0" || v === "false" || v === "FALSE");
})();
// Daemon mode: when >0, run sync in a loop with this sleep in minutes between cycles
const SYNC_LOOP_MIN = Number(process.env.SYNC_LOOP_MIN || 0);
// Stream structure upserts as we fetch (faster perceived availability)
const STREAM_UPSERTS = (() => {
  const v = process.env.STREAM_UPSERTS;
  if (v == null) return true; // default ON
  return v === "1" || v === "true" || v === "TRUE";
})();
// Rolling schedule (30d weekly/daily strategy) executed by the daemon loop
const ROLLING_SCHEDULE = (() => {
  const v = process.env.ROLLING_SCHEDULE;
  if (v == null) return true; // default ON
  return v === "1" || v === "true" || v === "TRUE";
})();

// Region-aware base selection
function normalizeRegion(region) {
  if (!region) return "na";
  const r = String(region).toLowerCase();
  if (["na", "north_america"].includes(r)) return "na";
  if (["eu", "europe"].includes(r)) return "eu";
  if (["fe", "far_east", "apac", "asia"].includes(r)) return "fe";
  return "na";
}

function regionApiBase(region) {
  const r = normalizeRegion(region);
  if (r === "eu") return "https://advertising-api-eu.amazon.com";
  if (r === "fe") return "https://advertising-api-fe.amazon.com";
  return "https://advertising-api.amazon.com";
}

if (
  !SUPABASE_URL ||
  !SERVICE_ROLE_KEY ||
  !AMAZON_CLIENT_ID ||
  !AMAZON_CLIENT_SECRET
) {
  console.error(
    "Missing env vars. Required: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, AMAZON_CLIENT_ID, AMAZON_CLIENT_SECRET"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

async function refreshAccessToken(refreshToken) {
  const res = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      client_id: AMAZON_CLIENT_ID,
      client_secret: AMAZON_CLIENT_SECRET,
    }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
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
  const endDate = end.toISOString().split("T")[0];
  const start = new Date(end.getTime() - (daysWindow - 1) * DAY_MS);
  const startDate = start.toISOString().split("T")[0];
  return { startDate, endDate };
}

async function downloadReportRows(downloadUrl) {
  try {
    const res = await fetch(downloadUrl);
    if (!res.ok) {
      console.error("Report download failed", res.status);
      return [];
    }

    let text;
    try {
      const encoding = res.headers.get("content-encoding") || "";

      if (encoding.includes("gzip") || downloadUrl.endsWith(".gz")) {
        // Use DecompressionStream for GZIP content
        const ds = new DecompressionStream("gzip");

        if (res.body) {
          const stream = res.body.pipeThrough(ds);
          text = await new Response(stream).text();
        } else {
          // Fallback for environments without streaming
          const buf = new Uint8Array(await res.arrayBuffer());
          const { gunzipSync } = await import("node:zlib");
          text = gunzipSync(buf).toString("utf8");
        }
      } else {
        text = await res.text();
      }
    } catch (e) {
      console.error("GZIP decompress failed, falling back to text()", e);
      text = await res.text();
    }

    // Try to parse as JSON array first
    try {
      const parsed = JSON.parse(text);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      // Fallback: NDJSON (one JSON object per line)
      return text
        .trim()
        .split("\n")
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
  } catch (e) {
    console.error("Error in downloadReportRows:", e);
    return [];
  }
}

async function getCampaignMetrics(profileId, accessToken, daysWindow, apiBase) {
  const { startDate, endDate } = buildDateRange(daysWindow);

  const createBody = {
    name: "Robotads campaign performance",
    startDate,
    endDate,
    configuration: {
      adProduct: "SPONSORED_PRODUCTS",
      reportTypeId: "spCampaigns",
      timeUnit: "SUMMARY",
      groupBy: ["campaign"],
      columns: [
        "campaignId",
        "impressions",
        "clicks",
        "cost",
        "purchases14d",
        "sales14d",
      ],
      format: "GZIP_JSON",
    },
  };

  try {
    console.log(`Creating campaign report ${startDate}..${endDate}`);
    const createRes = await fetch(`${apiBase}/reporting/reports`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
        "Amazon-Advertising-API-Scope": String(profileId),
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
      },
      body: JSON.stringify(createBody),
    });

    const createJson = await createRes.json().catch(() => ({}));
    let reportId = null;

    if (createRes.ok && createJson.reportId) {
      reportId = String(createJson.reportId);
    } else if (
      createRes.status === 425 &&
      typeof createJson.detail === "string"
    ) {
      const match = createJson.detail.match(
        /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/
      );
      if (match) reportId = match[1];
    }

    if (!reportId) {
      console.error(
        "Campaign report create error",
        createRes.status,
        createJson
      );
      if ([400, 403, 404, 405, 425].includes(createRes.status)) {
        return new Map();
      }
      throw new Error(`Campaign report failed: ${createRes.status}`);
    }

    let downloadUrl = null;
    let lastPoll = null;
    const maxPolls = Math.ceil(
      (REPORT_MAX_MIN * 60 * 1000) / REPORT_POLL_INTERVAL_MS
    );
    const infiniteWait = REPORT_MAX_MIN <= 0;
    const startedAt = Date.now();
    console.log(
      `Polling campaign report ${
        infiniteWait ? "indefinitely" : `up to ${REPORT_MAX_MIN}m`
      } (interval ${Math.round(REPORT_POLL_INTERVAL_MS / 1000)}s)`
    );
    for (let i = 0; ; i++) {
      const pollRes = await fetch(`${apiBase}/reporting/reports/${reportId}`, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
          "Amazon-Advertising-API-Scope": String(profileId),
        },
      });
      if (pollRes.status === 401) {
        console.warn(
          "Campaign report poll received 401 (token expired). Breaking to refresh..."
        );
        break;
      }
      const pollJson = await pollRes.json().catch(() => ({}));
      lastPoll = pollJson;
      if (
        i === 0 ||
        i % Math.max(1, Math.floor(30000 / REPORT_POLL_INTERVAL_MS)) === 0
      ) {
        const sec = Math.round((Date.now() - startedAt) / 1000);
        console.log(
          `Campaign report poll #${i + 1}/${
            infiniteWait ? "∞" : maxPolls
          } (${sec}s): status=${pollJson.status || "UNKNOWN"}`
        );
      }
      if (
        pollJson?.message &&
        /unauthorized|invalid token/i.test(String(pollJson.message))
      ) {
        console.warn(
          "Campaign report poll message indicates unauthorized token; breaking to refresh..."
        );
        break;
      }
      if (pollJson.status === "SUCCESS" || pollJson.status === "COMPLETED") {
        downloadUrl = pollJson.location || pollJson.url || null;
        break;
      }
      if (
        ["CANCELLED", "FAILURE", "ERROR", "FAILED"].includes(
          String(pollJson.status)
        )
      ) {
        console.error("Campaign report ended with failure status", pollJson);
        break;
      }
      await new Promise((r) => setTimeout(r, REPORT_POLL_INTERVAL_MS));
      if (!infiniteWait && i + 1 >= maxPolls) break;
    }

    if (!downloadUrl) {
      console.error("Campaign report polling timed out", lastPoll);
      return new Map();
    }

    const rows = await downloadReportRows(downloadUrl);
    const byId = new Map();

    for (const row of rows) {
      const id = String(row.campaignId ?? row.campaign_id ?? "");
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
    console.error("Error in getCampaignMetrics", e);
    return new Map();
  }
}

async function getKeywordMetrics(profileId, accessToken, daysWindow, apiBase) {
  const { startDate, endDate } = buildDateRange(daysWindow);

  const createBody = {
    name: "Robotads keyword performance",
    startDate,
    endDate,
    configuration: {
      adProduct: "SPONSORED_PRODUCTS",
      reportTypeId: "spTargeting",
      timeUnit: "SUMMARY",
      groupBy: ["targeting"],
      columns: [
        "campaignId",
        "adGroupId",
        "keywordId",
        "keyword",
        "matchType",
        "impressions",
        "clicks",
        "cost",
        "purchases14d",
        "sales14d",
      ],
      filters: [
        {
          field: "keywordType",
          values: ["BROAD", "PHRASE", "EXACT"],
        },
      ],
      format: "GZIP_JSON",
    },
  };

  try {
    console.log(`Creating keyword report ${startDate}..${endDate}`);
    const createRes = await fetch(`${apiBase}/reporting/reports`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
        "Amazon-Advertising-API-Scope": String(profileId),
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
      },
      body: JSON.stringify(createBody),
    });

    const createJson = await createRes.json().catch(() => ({}));
    let reportId = null;

    if (createRes.ok && createJson.reportId) {
      reportId = String(createJson.reportId);
    } else if (
      createRes.status === 425 &&
      typeof createJson.detail === "string"
    ) {
      const match = createJson.detail.match(
        /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/
      );
      if (match) reportId = match[1];
    }

    if (!reportId) {
      console.error(
        "Keyword report create error",
        createRes.status,
        createJson
      );
      if ([400, 403, 404, 405, 425].includes(createRes.status)) {
        return new Map();
      }
      throw new Error(`Keyword report failed: ${createRes.status}`);
    }

    let downloadUrl = null;
    let lastPoll = null;
    const maxPolls = Math.ceil(
      (REPORT_MAX_MIN * 60 * 1000) / REPORT_POLL_INTERVAL_MS
    );
    const infiniteWait = REPORT_MAX_MIN <= 0;
    const startedAt = Date.now();
    console.log(
      `Polling keyword report ${
        infiniteWait ? "indefinitely" : `up to ${REPORT_MAX_MIN}m`
      } (interval ${Math.round(REPORT_POLL_INTERVAL_MS / 1000)}s)`
    );
    for (let i = 0; ; i++) {
      const pollRes = await fetch(`${apiBase}/reporting/reports/${reportId}`, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
          "Amazon-Advertising-API-Scope": String(profileId),
        },
      });
      if (pollRes.status === 401) {
        console.warn(
          "Keyword report poll received 401 (token expired). Breaking to refresh..."
        );
        break;
      }
      const pollJson = await pollRes.json().catch(() => ({}));
      lastPoll = pollJson;
      if (
        i === 0 ||
        i % Math.max(1, Math.floor(30000 / REPORT_POLL_INTERVAL_MS)) === 0
      ) {
        const sec = Math.round((Date.now() - startedAt) / 1000);
        console.log(
          `Keyword report poll #${i + 1}/${
            infiniteWait ? "∞" : maxPolls
          } (${sec}s): status=${pollJson.status || "UNKNOWN"}`
        );
      }
      if (
        pollJson?.message &&
        /unauthorized|invalid token/i.test(String(pollJson.message))
      ) {
        console.warn(
          "Keyword report poll message indicates unauthorized token; breaking to refresh..."
        );
        break;
      }
      if (pollJson.status === "SUCCESS" || pollJson.status === "COMPLETED") {
        downloadUrl = pollJson.location || pollJson.url || null;
        break;
      }
      if (
        ["CANCELLED", "FAILURE", "ERROR", "FAILED"].includes(
          String(pollJson.status)
        )
      ) {
        console.error("Keyword report ended with failure status", pollJson);
        break;
      }
      await new Promise((r) => setTimeout(r, REPORT_POLL_INTERVAL_MS));
      if (!infiniteWait && i + 1 >= maxPolls) break;
    }

    if (!downloadUrl) {
      console.error("Keyword report polling timed out", lastPoll);
      return new Map();
    }

    const rows = await downloadReportRows(downloadUrl);
    const byKeywordId = new Map();

    for (const row of rows) {
      const keywordId = String(row.keywordId ?? row.keyword_id ?? "");
      if (!keywordId) continue;

      const impressions = Number(row.impressions ?? 0) || 0;
      const clicks = Number(row.clicks ?? 0) || 0;
      const cost = Number(row.cost ?? 0) || 0;
      const orders = Number(row.purchases14d ?? row.orders ?? 0) || 0;
      const sales = Number(row.sales14d ?? row.sales ?? 0) || 0;

      byKeywordId.set(keywordId, {
        campaignId: String(row.campaignId ?? row.campaign_id ?? ""),
        adGroupId: String(row.adGroupId ?? row.ad_group_id ?? ""),
        impressions,
        clicks,
        cost,
        orders,
        sales,
      });
    }

    return byKeywordId;
  } catch (e) {
    console.error("Error in getKeywordMetrics", e);
    return new Map();
  }
}

// ---- Main sync per account ----

async function syncAccount(account, windowDays) {
  console.log(
    `\n=== Syncing account ${account.id} (${account.name || ""}) ===`
  );

  let accessToken = account.access_token;
  const refreshToken = account.refresh_token;

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
      console.error("No refresh token; marking account as reauth_required");
      await supabase
        .from("amazon_accounts")
        .update({ status: "reauth_required" })
        .eq("id", account.id);
      return;
    }

    try {
      const refreshed = await refreshAccessToken(refreshToken);
      accessToken = refreshed.access_token;
      const newExpiry = new Date(
        Date.now() + (refreshed.expires_in ?? 3600) * 1000
      ).toISOString();

      await supabase
        .from("amazon_accounts")
        .update({
          access_token: refreshed.access_token,
          refresh_token: refreshed.refresh_token,
          token_expires_at: newExpiry,
          updated_at: new Date().toISOString(),
          status: "active",
        })
        .eq("id", account.id);

      console.log("Token refreshed for account", account.id);
    } catch (e) {
      console.error("Token refresh failed for account", account.id, e);
      await supabase
        .from("amazon_accounts")
        .update({ status: "reauth_required" })
        .eq("id", account.id);
      return;
    }
  }

  if (!accessToken) {
    console.error(
      "No access token after refresh; skipping account",
      account.id
    );
    return;
  }

  if (!account.amazon_profile_id) {
    console.error("Missing amazon_profile_id for account", account.id);
    return;
  }

  const apiBase = regionApiBase(account.amazon_region);
  // Helper: mid-sync token refresh and persist
  async function refreshAndUpdateAccessToken() {
    try {
      const refreshed = await refreshAccessToken(refreshToken);
      accessToken = refreshed.access_token;
      const newExpiry = new Date(
        Date.now() + (refreshed.expires_in ?? 3600) * 1000
      ).toISOString();
      await supabase
        .from("amazon_accounts")
        .update({
          access_token: refreshed.access_token,
          refresh_token: refreshed.refresh_token,
          token_expires_at: newExpiry,
          updated_at: new Date().toISOString(),
          status: "active",
        })
        .eq("id", account.id);
      console.log("Access token refreshed mid-sync for account", account.id);
    } catch (e) {
      console.error("Mid-sync token refresh failed:", e?.message || e);
    }
  }
  // Load existing metrics so we can carry them over if new reports are missing
  // and prepare existing-id maps for safe upsert (no destructive delete)
  console.log("Loading existing data for account", account.id);

  const { data: oldCampaigns } = await supabase
    .from("amazon_campaigns")
    .select(
      "id, campaign_id, spend, impressions, clicks, orders, acos, ctr, cpc"
    )
    .eq("account_id", account.id);

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
  const existingKeywordIdByAmazonId = new Map();
  if (oldCampaignIds.length > 0) {
    const { data: oldKeywords } = await supabase
      .from("amazon_keywords")
      .select(
        "id, amazon_keyword_id, keyword_id, spend, impressions, clicks, orders, acos"
      )
      .in("campaign_id", oldCampaignIds);

    for (const k of oldKeywords || []) {
      if (!k.keyword_id) continue;
      oldKeywordMetricsByKeywordId.set(String(k.keyword_id), {
        spend: Number(k.spend ?? 0) || 0,
        impressions: Number(k.impressions ?? 0) || 0,
        clicks: Number(k.clicks ?? 0) || 0,
        orders: Number(k.orders ?? 0) || 0,
        acos: Number(k.acos ?? 0) || 0,
      });
      if (k.amazon_keyword_id && k.id) {
        existingKeywordIdByAmazonId.set(String(k.amazon_keyword_id), k.id);
      }
    }
  }

  // Existing ad groups for id-mapping during upsert
  const existingAdGroupIdMap = new Map();
  {
    const { data: existingAdGroups } = await supabase
      .from("amazon_ad_groups")
      .select("id, amazon_ad_group_id")
      .eq("account_id", account.id);
    for (const ag of existingAdGroups || []) {
      if (ag.amazon_ad_group_id && ag.id) {
        existingAdGroupIdMap.set(String(ag.amazon_ad_group_id), ag.id);
      }
    }
  }

  let insertedAdGroups = [];
  const adGroupIdMap = new Map();
  for (const [k, v] of existingAdGroupIdMap.entries())
    adGroupIdMap.set(String(k), v);

  // Fetch campaigns from Amazon v2
  console.log("Fetching campaigns from Amazon...");
  let campaignsRes = await fetch(`${apiBase}/v2/campaigns`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
      "Amazon-Advertising-API-Scope": String(account.amazon_profile_id),
      "Content-Type": "application/json",
    },
  });

  if (!campaignsRes.ok && campaignsRes.status === 401) {
    await refreshAndUpdateAccessToken();
    campaignsRes = await fetch(`${apiBase}/v2/campaigns`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
        "Amazon-Advertising-API-Scope": String(account.amazon_profile_id),
        "Content-Type": "application/json",
      },
    });
  }

  if (!campaignsRes.ok) {
    const text = await campaignsRes.text().catch(() => "");
    console.error("Failed to fetch campaigns", campaignsRes.status, text);
    return;
  }

  const amazonCampaigns = await campaignsRes.json();
  console.log("Fetched", amazonCampaigns.length, "campaigns.");

  // Kick off metrics reports in parallel (to overlap long v3 report generation)
  const campaignMetricsPromise = SKIP_METRICS
    ? Promise.resolve(new Map())
    : getCampaignMetrics(
        String(account.amazon_profile_id),
        accessToken,
        windowDays || DAYS_WINDOW,
        apiBase
      );
  const keywordMetricsPromise = SKIP_METRICS
    ? Promise.resolve(new Map())
    : getKeywordMetrics(
        String(account.amazon_profile_id),
        accessToken,
        windowDays || DAYS_WINDOW,
        apiBase
      );

  // Metrics via v3 (unless SKIP_METRICS)
  let metricsByCampaignId = new Map();
  if (!SKIP_METRICS) {
    metricsByCampaignId = await campaignMetricsPromise;
    // If empty (possible 401 during long polling), refresh and retry once
    if (!metricsByCampaignId || metricsByCampaignId.size === 0) {
      await refreshAndUpdateAccessToken();
      metricsByCampaignId = await getCampaignMetrics(
        String(account.amazon_profile_id),
        accessToken,
        windowDays || DAYS_WINDOW,
        apiBase
      );
    }
  } else {
    console.log(
      "SKIP_METRICS=1: skipping v3 campaign report and carrying over existing metrics."
    );
  }

  const campaignsToInsert = amazonCampaigns.map((c) => {
    const campaignKey = String(c.campaignId);
    const newM = metricsByCampaignId.get(campaignKey) || null;
    const oldM = oldCampaignMetricsByCampaignId.get(campaignKey) || null;
    const base = newM ||
      oldM || {
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
    const acos = sales > 0 ? spend / sales : base.acos ?? 0;
    const ctr = impressions > 0 ? clicks / impressions : base.ctr ?? 0;
    const cpc = clicks > 0 ? spend / clicks : base.cpc ?? 0;

    return {
      account_id: account.id,
      campaign_id: String(c.campaignId),
      name: c.name || `Campaign ${c.campaignId}`,
      status: String(c.state ?? c.status ?? "unknown").toLowerCase(),
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
    // Safe upsert: if an existing campaign row (by id) exists, update; otherwise insert.
    // We rely on primary key id for updates via upsert; for new rows 'id' is absent.
    const { data, error } = await supabase
      .from("amazon_campaigns")
      .upsert(
        campaignsToInsert.map((c) => {
          const existingId = oldCampaigns?.find(
            (oc) => String(oc.campaign_id) === c.campaign_id
          )?.id;
          return existingId ? { id: existingId, ...c } : c;
        })
      )
      .select("id, campaign_id");
    if (error) {
      console.error("Insert campaigns error", error);
      return;
    }
    insertedCampaigns = data || [];
    console.log("Upserted", insertedCampaigns.length, "campaign rows.");
  }

  const campaignIdMap = new Map();
  for (const c of insertedCampaigns) {
    campaignIdMap.set(String(c.campaign_id), c.id);
  }

  // Fetch ad groups
  console.log("Fetching ad groups...");
  const adGroupRows = [];
  const adGroupAmazonToCampaignAmazon = new Map();
  let keywordRows = [];
  let keywordAmazonIds = [];

  for (const c of amazonCampaigns) {
    const campaignAmazonId = String(c.campaignId);
    const parentDbId = campaignIdMap.get(campaignAmazonId);
    if (!parentDbId) continue;

    try {
      let agRes = await fetch(
        `${apiBase}/v2/adGroups?campaignIdFilter=${campaignAmazonId}`,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
            "Amazon-Advertising-API-Scope": String(account.amazon_profile_id),
            "Content-Type": "application/json",
          },
        }
      );

      if (!agRes.ok && agRes.status === 401) {
        await refreshAndUpdateAccessToken();
        agRes = await fetch(
          `${apiBase}/v2/adGroups?campaignIdFilter=${campaignAmazonId}`,
          {
            headers: {
              Authorization: `Bearer ${accessToken}`,
              "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
              "Amazon-Advertising-API-Scope": String(account.amazon_profile_id),
              "Content-Type": "application/json",
            },
          }
        );
      }

      if (!agRes.ok) {
        console.error(
          "Failed to fetch ad groups for campaign",
          campaignAmazonId,
          agRes.status
        );
        continue;
      }

      const agJson = await agRes.json();
      const perAgRows = [];
      for (const ag of agJson) {
        const agAmazonId = String(ag.adGroupId);
        const row = {
          account_id: account.id,
          campaign_id: parentDbId,
          name: ag.name || `Ad Group ${ag.adGroupId}`,
          status: String(ag.state ?? ag.status ?? "unknown").toLowerCase(),
          default_bid: ag.defaultBid ?? 0,
          amazon_profile_id_text: String(account.amazon_profile_id),
          amazon_region: account.amazon_region || null,
          amazon_ad_group_id: agAmazonId,
        };
        if (STREAM_UPSERTS) {
          perAgRows.push(row);
        } else {
          adGroupRows.push(row);
        }
        adGroupAmazonToCampaignAmazon.set(agAmazonId, campaignAmazonId);
      }

      if (STREAM_UPSERTS && perAgRows.length > 0) {
        const { data: agData, error: upsertAgErr } = await supabase
          .from("amazon_ad_groups")
          .upsert(perAgRows)
          .select("id, amazon_ad_group_id");
        if (upsertAgErr) {
          console.error("Upsert ad groups error", upsertAgErr);
        } else {
          insertedAdGroups.push(...(agData || []));
          for (const ag of agData || [])
            adGroupIdMap.set(String(ag.amazon_ad_group_id), ag.id);
          console.log(
            "Upserted",
            (agData || []).length,
            "ad group rows (stream)."
          );
        }

        // Stream keywords for these ad groups (structure + carry-over metrics)
        for (const agRow of perAgRows) {
          const agAmazonId = String(agRow.amazon_ad_group_id);
          const adGroupDbId = adGroupIdMap.get(agAmazonId);
          const campaignDbId = agRow.campaign_id;
          if (!adGroupDbId) continue;

          try {
            let kwRes = await fetch(
              `${apiBase}/v2/keywords?adGroupIdFilter=${agAmazonId}`,
              {
                headers: {
                  Authorization: `Bearer ${accessToken}`,
                  "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
                  "Amazon-Advertising-API-Scope": String(
                    account.amazon_profile_id
                  ),
                  "Content-Type": "application/json",
                },
              }
            );

            if (!kwRes.ok && kwRes.status === 401) {
              await refreshAndUpdateAccessToken();
              kwRes = await fetch(
                `${apiBase}/v2/keywords?adGroupIdFilter=${agAmazonId}`,
                {
                  headers: {
                    Authorization: `Bearer ${accessToken}`,
                    "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
                    "Amazon-Advertising-API-Scope": String(
                      account.amazon_profile_id
                    ),
                    "Content-Type": "application/json",
                  },
                }
              );
            }

            if (!kwRes.ok) {
              console.error(
                "Failed to fetch keywords for ad group",
                agAmazonId,
                kwRes.status
              );
              continue;
            }

            const kwJson = await kwRes.json();
            const perKwRows = [];
            for (const kw of kwJson) {
              const keywordAmazonId = String(kw.keywordId);
              keywordAmazonIds.push(keywordAmazonId);
              const oldKw =
                oldKeywordMetricsByKeywordId.get(keywordAmazonId) || {};
              const maybeExistingId =
                existingKeywordIdByAmazonId.get(keywordAmazonId);
              const row = {
                id: maybeExistingId || randomUUID(),
                campaign_id: campaignDbId,
                keyword_id: keywordAmazonId,
                text: kw.keywordText || "",
                match_type: kw.matchType || "",
                bid: kw.bid ?? 0,
                status: String(kw.state ?? "unknown").toLowerCase(),
                spend: oldKw.spend ?? 0,
                impressions: oldKw.impressions ?? 0,
                clicks: oldKw.clicks ?? 0,
                orders: oldKw.orders ?? 0,
                acos: oldKw.acos ?? 0,
                ad_group_id: adGroupDbId,
                amazon_profile_id_text: String(account.amazon_profile_id),
                amazon_region: account.amazon_region || null,
                amazon_keyword_id: keywordAmazonId,
              };
              perKwRows.push(row);
              keywordRows.push(row);
            }

            if (perKwRows.length > 0) {
              let { error: upsertKwErr } = await supabase
                .from("amazon_keywords")
                .upsert(perKwRows);
              if (upsertKwErr && upsertKwErr.code === "23503") {
                const rowsWithoutAdGroup = perKwRows.map(
                  ({ ad_group_id, ...rest }) => rest
                );
                ({ error: upsertKwErr } = await supabase
                  .from("amazon_keywords")
                  .upsert(rowsWithoutAdGroup));
              }
              if (upsertKwErr) {
                console.error("Upsert keywords error (stream)", upsertKwErr);
              } else {
                console.log(
                  "Upserted",
                  perKwRows.length,
                  "keyword rows (stream)."
                );
              }
            }
          } catch (e) {
            console.error(
              "Error fetching keywords for ad group",
              agAmazonId,
              e
            );
          }
        }
      }
    } catch (e) {
      console.error(
        "Error fetching ad groups for campaign",
        campaignAmazonId,
        e
      );
    }
  }

  insertedAdGroups = insertedAdGroups;
  if (adGroupRows.length > 0) {
    // Attach existing ids for upsert
    const upsertRows = adGroupRows.map((row) => {
      const existingId = existingAdGroupIdMap.get(
        String(row.amazon_ad_group_id)
      );
      return existingId ? { id: existingId, ...row } : row;
    });
    const { data: agData, error: upsertAgErr } = await supabase
      .from("amazon_ad_groups")
      .upsert(upsertRows)
      .select("id, amazon_ad_group_id");
    if (upsertAgErr) {
      console.error("Upsert ad groups error", upsertAgErr);
    } else {
      insertedAdGroups = agData || [];
      console.log("Upserted", insertedAdGroups.length, "ad group rows.");
    }
  }

  adGroupIdMap;
  // include existing first, then override/extend with upsert results
  for (const [k, v] of existingAdGroupIdMap.entries())
    adGroupIdMap.set(String(k), v);
  for (const ag of insertedAdGroups) {
    adGroupIdMap.set(String(ag.amazon_ad_group_id), ag.id);
  }

  // Fetch keywords and structure
  if (!STREAM_UPSERTS) {
    console.log("Fetching keywords...");
    keywordRows = [];
    keywordAmazonIds = [];

    for (const [
      agAmazonId,
      campaignAmazonId,
    ] of adGroupAmazonToCampaignAmazon.entries()) {
      const adGroupDbId = adGroupIdMap.get(agAmazonId);
      const campaignDbId = campaignAmazonId
        ? campaignIdMap.get(campaignAmazonId)
        : null;
      if (!adGroupDbId) continue;

      try {
        let kwRes = await fetch(
          `${apiBase}/v2/keywords?adGroupIdFilter=${agAmazonId}`,
          {
            headers: {
              Authorization: `Bearer ${accessToken}`,
              "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
              "Amazon-Advertising-API-Scope": String(account.amazon_profile_id),
              "Content-Type": "application/json",
            },
          }
        );

        if (!kwRes.ok && kwRes.status === 401) {
          await refreshAndUpdateAccessToken();
          kwRes = await fetch(
            `${apiBase}/v2/keywords?adGroupIdFilter=${agAmazonId}`,
            {
              headers: {
                Authorization: `Bearer ${accessToken}`,
                "Amazon-Advertising-API-ClientId": AMAZON_CLIENT_ID,
                "Amazon-Advertising-API-Scope": String(
                  account.amazon_profile_id
                ),
                "Content-Type": "application/json",
              },
            }
          );
        }

        if (!kwRes.ok) {
          console.error(
            "Failed to fetch keywords for ad group",
            agAmazonId,
            kwRes.status
          );
          continue;
        }

        const kwJson = await kwRes.json();
        for (const kw of kwJson) {
          const keywordAmazonId = String(kw.keywordId);
          keywordAmazonIds.push(keywordAmazonId);

          const oldKw = oldKeywordMetricsByKeywordId.get(keywordAmazonId) || {};

          const maybeExistingId =
            existingKeywordIdByAmazonId.get(keywordAmazonId);
          const row = {
            campaign_id: campaignDbId,
            keyword_id: keywordAmazonId,
            text: kw.keywordText || "",
            match_type: kw.matchType || "",
            bid: kw.bid ?? 0,
            status: String(kw.state ?? "unknown").toLowerCase(),
            spend: oldKw.spend ?? 0,
            impressions: oldKw.impressions ?? 0,
            clicks: oldKw.clicks ?? 0,
            orders: oldKw.orders ?? 0,
            acos: oldKw.acos ?? 0,
            ad_group_id: adGroupDbId,
            amazon_profile_id_text: String(account.amazon_profile_id),
            amazon_region: account.amazon_region || null,
            amazon_keyword_id: keywordAmazonId,
          };
          row.id = maybeExistingId || randomUUID();
          keywordRows.push(row);
        }
      } catch (e) {
        console.error("Error fetching keywords for ad group", agAmazonId, e);
      }
    }

    // Enrich keywords with performance metrics (unless SKIP_METRICS)
    if (!SKIP_METRICS && keywordRows.length > 0) {
      console.log("Fetching keyword metrics via v3...");
      let keywordMetricsById = await keywordMetricsPromise;
      if (!keywordMetricsById || keywordMetricsById.size === 0) {
        await refreshAndUpdateAccessToken();
        keywordMetricsById = await getKeywordMetrics(
          String(account.amazon_profile_id),
          accessToken,
          windowDays || DAYS_WINDOW,
          apiBase
        );
      }

      if (keywordMetricsById && keywordMetricsById.size > 0) {
        for (const row of keywordRows) {
          const metrics = keywordMetricsById.get(String(row.amazon_keyword_id));
          if (!metrics) continue;

          const spend = metrics.cost;
          const impressions = metrics.impressions;
          const clicks = metrics.clicks;
          const orders = metrics.orders;
          const sales = metrics.sales;
          const ctr = impressions > 0 ? clicks / impressions : 0;
          const cpc = clicks > 0 ? spend / clicks : 0;

          row.spend = spend;
          row.impressions = impressions;
          row.clicks = clicks;
          row.orders = orders;
          row.acos = sales > 0 ? spend / (sales || 1) : 0;
          row.ctr = ctr;
          row.cpc = cpc;
          row.raw_data = { spend, impressions, clicks, orders, sales };
        }

        // If campaign report is missing/empty, aggregate from keyword metrics (unless strict mode)
        if (
          (!metricsByCampaignId || metricsByCampaignId.size === 0) &&
          !STRICT_ONLY_REPORTS
        ) {
          const agg = new Map(); // campaignId -> totals
          for (const m of keywordMetricsById.values()) {
            const cid = String(m.campaignId || "");
            if (!cid) continue;
            const a = agg.get(cid) || {
              impressions: 0,
              clicks: 0,
              cost: 0,
              orders: 0,
              sales: 0,
            };
            a.impressions += Number(m.impressions || 0);
            a.clicks += Number(m.clicks || 0);
            a.cost += Number(m.cost || 0);
            a.orders += Number(m.orders || 0);
            a.sales += Number(m.sales || 0);
            agg.set(cid, a);
          }
          if (agg.size > 0) {
            console.log(
              `Using keyword report aggregation for campaign metrics (${agg.size} campaigns).`
            );
            metricsByCampaignId = new Map();
            for (const [cid, a] of agg.entries()) {
              const impressions = a.impressions;
              const clicks = a.clicks;
              const cost = a.cost;
              const orders = a.orders;
              const sales = a.sales;
              const ctr = impressions > 0 ? clicks / impressions : 0;
              const cpc = clicks > 0 ? cost / clicks : 0;
              const acos = sales > 0 ? cost / sales : 0;
              metricsByCampaignId.set(String(cid), {
                impressions,
                clicks,
                cost,
                orders,
                sales,
                ctr,
                cpc,
                acos,
              });
            }
          }
        } else if (
          (!metricsByCampaignId || metricsByCampaignId.size === 0) &&
          STRICT_ONLY_REPORTS
        ) {
          console.log(
            "STRICT_ONLY_REPORTS=1: campaign v3 report missing; skipping keyword aggregation and keeping previous metrics."
          );
        }

        // Aggregate ad group metrics from keyword metrics and update amazon_ad_groups
        const aggAg = new Map(); // adGroupId -> totals
        for (const m of keywordMetricsById.values()) {
          const agid = String(m.adGroupId || "");
          if (!agid) continue;
          const a = aggAg.get(agid) || {
            impressions: 0,
            clicks: 0,
            cost: 0,
            orders: 0,
            sales: 0,
          };
          a.impressions += Number(m.impressions || 0);
          a.clicks += Number(m.clicks || 0);
          a.cost += Number(m.cost || 0);
          a.orders += Number(m.orders || 0);
          a.sales += Number(m.sales || 0);
          aggAg.set(agid, a);
        }
        if (aggAg.size > 0) {
          const updates = [];
          for (const [agAmazonId, a] of aggAg.entries()) {
            const dbId = adGroupIdMap.get(String(agAmazonId));
            if (!dbId) continue;
            const impressions = a.impressions;
            const clicks = a.clicks;
            const cost = a.cost;
            const orders = a.orders;
            const sales = a.sales;
            const ctr = impressions > 0 ? clicks / impressions : 0;
            const cpc = clicks > 0 ? cost / clicks : 0;
            const acos = sales > 0 ? cost / sales : 0;
            updates.push({
              id: dbId,
              impressions,
              clicks,
              spend: cost,
              orders,
              ctr,
              cpc,
              acos,
              raw_data: { impressions, clicks, spend: cost, orders, sales },
            });
          }
          if (updates.length > 0) {
            const { error: agUpdErr } = await supabase
              .from("amazon_ad_groups")
              .upsert(updates);
            if (agUpdErr)
              console.error("Failed to upsert ad group metrics", agUpdErr);
            else
              console.log(
                "Updated",
                updates.length,
                "ad groups with aggregated metrics."
              );
          }
        }
      }
    }

    if (keywordRows.length > 0) {
      let { error: upsertKwErr } = await supabase
        .from("amazon_keywords")
        .upsert(keywordRows);

      // If FK to amazon_ad_groups fails for some reason, retry without ad_group_id
      if (upsertKwErr && upsertKwErr.code === "23503") {
        console.error(
          "Upsert keywords FK error, retrying without ad_group_id",
          upsertKwErr
        );
        const rowsWithoutAdGroup = keywordRows.map(
          ({ ad_group_id, ...rest }) => rest
        );
        ({ error: upsertKwErr } = await supabase
          .from("amazon_keywords")
          .upsert(rowsWithoutAdGroup));
      }

      if (upsertKwErr) {
        console.error("Upsert keywords error", upsertKwErr);
      } else {
        console.log("Upserted", keywordRows.length, "keyword rows.");
      }
    }

    // Update campaign metrics using whichever metrics we have at this point
    if (
      metricsByCampaignId &&
      metricsByCampaignId.size > 0 &&
      insertedCampaigns.length > 0
    ) {
      let updated = 0;
      for (const c of amazonCampaigns) {
        const amazonId = String(c.campaignId);
        const dbId = campaignIdMap.get(amazonId);
        const m = metricsByCampaignId.get(amazonId);
        if (!dbId || !m) continue;
        const spend = m.cost ?? m.spend ?? 0;
        const impressions = m.impressions ?? 0;
        const clicks = m.clicks ?? 0;
        const orders = m.orders ?? 0;
        const sales = m.sales ?? 0;
        const acos = sales > 0 ? spend / sales : m.acos ?? 0;
        const ctr = impressions > 0 ? clicks / impressions : m.ctr ?? 0;
        const cpc = clicks > 0 ? spend / clicks : m.cpc ?? 0;
        const { error: updErr } = await supabase
          .from("amazon_campaigns")
          .update({ spend, impressions, clicks, orders, acos, ctr, cpc })
          .eq("id", dbId);
        if (updErr) {
          console.error(
            "Failed to update metrics for campaign",
            amazonId,
            updErr
          );
        } else {
          updated++;
        }
      }
      console.log("Updated", updated, "campaigns with metrics.");
    }

    await supabase
      .from("amazon_accounts")
      .update({
        last_sync: new Date().toISOString(),
        status: "active",
      })
      .eq("id", account.id);

    console.log(
      `✅ Sync completed for account ${account.id}: ${
        campaignsToInsert.length
      } campaigns (with ${windowDays || DAYS_WINDOW}-day performance).`
    );
  }
}

async function main() {
  do {
    console.log(
      "▶️ Amazon -> Supabase VPS sync started at",
      new Date().toISOString()
    );

    let query = supabase.from("amazon_accounts").select("*");
    if (ACCOUNT_ID) {
      query = query.eq("id", ACCOUNT_ID);
    }

    const { data: accounts, error } = await query;
    if (error) {
      console.error("Error loading amazon_accounts:", error);
      if (SYNC_LOOP_MIN > 0) {
        console.log(
          `⏱ Sleeping ${SYNC_LOOP_MIN}m due to load error before retry...`
        );
        await new Promise((r) => setTimeout(r, SYNC_LOOP_MIN * 60 * 1000));
        continue;
      } else {
        process.exit(1);
      }
    }

    if (!accounts || !accounts.length) {
      console.log("No amazon_accounts found; nothing to sync.");
    } else {
      for (const account of accounts) {
        try {
          if (ROLLING_SCHEDULE) {
            const now = Date.now();
            const DAY_MS = 24 * 60 * 60 * 1000;
            const dailyWin = Number(account.daily_window_days || 3);
            const weeklyWin = Number(account.weekly_window_days || 7);
            const monthlyWin = Number(account.monthly_window_days || 30);

            const last3 = account.last_3d_sync_at
              ? new Date(account.last_3d_sync_at).getTime()
              : 0;
            const last7 = account.last_7d_sync_at
              ? new Date(account.last_7d_sync_at).getTime()
              : 0;
            const last30 = account.last_30d_sync_at
              ? new Date(account.last_30d_sync_at).getTime()
              : 0;

            const dueMonthly = !last30 || now - last30 >= 30 * DAY_MS;
            const dueWeekly = !last7 || now - last7 >= 7 * DAY_MS;
            const dueDaily = !last3 || now - last3 >= 1 * DAY_MS;

            let ranAny = false;
            if (dueMonthly) {
              await syncAccount(account, monthlyWin);
              await supabase
                .from("amazon_accounts")
                .update({ last_30d_sync_at: new Date().toISOString() })
                .eq("id", account.id);
              ranAny = true;
            }
            if (dueWeekly) {
              await syncAccount(account, weeklyWin);
              await supabase
                .from("amazon_accounts")
                .update({ last_7d_sync_at: new Date().toISOString() })
                .eq("id", account.id);
              ranAny = true;
            }
            if (dueDaily) {
              await syncAccount(account, dailyWin);
              await supabase
                .from("amazon_accounts")
                .update({ last_3d_sync_at: new Date().toISOString() })
                .eq("id", account.id);
              ranAny = true;
            }

            if (!ranAny) {
              // Nothing due now; fall back to default window for a quick refresh
              await syncAccount(account, Number(process.env.DAYS_WINDOW || 7));
            }
          } else {
            await syncAccount(account);
          }
        } catch (e) {
          console.error(`❌ Error syncing account ${account.id}:`, e);
        }
      }
    }

    console.log(
      "🏁 Amazon -> Supabase VPS sync finished at",
      new Date().toISOString()
    );
    if (SYNC_LOOP_MIN > 0) {
      console.log(`⏱ Sleeping ${SYNC_LOOP_MIN}m before next cycle...`);
      await new Promise((r) => setTimeout(r, SYNC_LOOP_MIN * 60 * 1000));
    }
  } while (SYNC_LOOP_MIN > 0);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error("Fatal sync error:", e);
    process.exit(1);
  });
}
