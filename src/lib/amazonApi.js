import { supabase } from './supabaseClient.js';

// Amazon Ads API Configuration (works in both browser and Node)
function readEnv(key) {
  try {
    if (typeof import.meta !== 'undefined' && import.meta.env && import.meta.env[key] != null) {
      return import.meta.env[key];
    }
  } catch {}
  try {
    if (typeof process !== 'undefined' && process.env && process.env[key] != null) {
      return process.env[key];
    }
  } catch {}
  return undefined;
}

const AMAZON_CLIENT_ID = readEnv('VITE_AMAZON_CLIENT_ID') || readEnv('AMAZON_CLIENT_ID');
const AMAZON_CLIENT_SECRET = readEnv('VITE_AMAZON_CLIENT_SECRET') || readEnv('AMAZON_CLIENT_SECRET');
const AMAZON_REDIRECT_URI =
  readEnv('VITE_AMAZON_REDIRECT_URI') ||
  readEnv('AMAZON_REDIRECT_URI') ||
  'http://localhost:5173/amazon-callback';

// Amazon API endpoints
const AMAZON_AUTH_URL = 'https://www.amazon.com/ap/oa';
const AMAZON_TOKEN_URL = 'https://api.amazon.com/auth/o2/token';
const AMAZON_API_BASE = 'https://advertising-api.amazon.com';

export class AmazonAdsAPI {
  constructor() {
    this.accessToken = null;
    this.refreshToken = null;
    this.profileId = null;
    this.region = null;
  }

  // OAuth URL oluştur
  getAuthUrl() {
    const params = new URLSearchParams({
      client_id: AMAZON_CLIENT_ID,
      scope: 'advertising::campaign_management',
      response_type: 'code',
      redirect_uri: AMAZON_REDIRECT_URI,
      state: this.generateState()
    });

    return `${AMAZON_AUTH_URL}?${params.toString()}`;
  }

  // State token oluştur
  generateState() {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
  }

  // Authorization code ile access token al
  async getAccessToken(authCode) {
    try {
      const response = await fetch(AMAZON_TOKEN_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          grant_type: 'authorization_code',
          code: authCode,
          redirect_uri: AMAZON_REDIRECT_URI,
          client_id: AMAZON_CLIENT_ID,
          client_secret: AMAZON_CLIENT_SECRET,
        }),
      });

      if (!response.ok) {
        throw new Error(`Token request failed: ${response.status}`);
      }

      const data = await response.json();
      return {
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        expires_in: data.expires_in,
        token_type: data.token_type
      };
    } catch (error) {
      console.error('Error getting access token:', error);
      throw error;
    }
  }

  // Refresh token ile yeni access token al
  async refreshAccessToken(refreshToken) {
    try {
      const response = await fetch(AMAZON_TOKEN_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          grant_type: 'refresh_token',
          refresh_token: refreshToken,
          client_id: AMAZON_CLIENT_ID,
          client_secret: AMAZON_CLIENT_SECRET,
        }),
      });

      if (!response.ok) {
        throw new Error(`Token refresh failed: ${response.status}`);
      }

      const data = await response.json();
      return {
        access_token: data.access_token,
        refresh_token: data.refresh_token || refreshToken,
        expires_in: data.expires_in,
        token_type: data.token_type
      };
    } catch (error) {
      console.error('Error refreshing token:', error);
      throw error;
    }
  }

  // Profil listesini al
  async getProfiles(accessToken) {
    try {
      const response = await fetch(`${AMAZON_API_BASE}/v2/profiles`, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to get profiles: ${response.status}`);
      }

      const profiles = await response.json();
      return profiles;
    } catch (error) {
      console.error('Error getting profiles:', error);
      throw error;
    }
  }

  // Kampanyaları al
  async getCampaigns(accessToken, profileId, region = 'NA') {
    try {
      const response = await fetch(`${AMAZON_API_BASE}/v2/campaigns`, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to get campaigns: ${response.status}`);
      }

      const campaigns = await response.json();
      return campaigns;
    } catch (error) {
      console.error('Error getting campaigns:', error);
      throw error;
    }
  }

  // Ad gruplarını al
  async getAdGroups(accessToken, profileId, campaignId) {
    try {
      const response = await fetch(`${AMAZON_API_BASE}/v2/adGroups?campaignIdFilter=${campaignId}`, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to get ad groups: ${response.status}`);
      }

      const adGroups = await response.json();
      return adGroups;
    } catch (error) {
      console.error('Error getting ad groups:', error);
      throw error;
    }
  }

  // Anahtar kelimeleri al
  async getKeywords(accessToken, profileId, adGroupId) {
    try {
      const response = await fetch(`${AMAZON_API_BASE}/v2/keywords?adGroupIdFilter=${adGroupId}`, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to get keywords: ${response.status}`);
      }

      const keywords = await response.json();
      return keywords;
    } catch (error) {
      console.error('Error getting keywords:', error);
      throw error;
    }
  }

  // Kampanya performans verilerini al
  async getCampaignPerformance(accessToken, profileId, campaignIds, startDate, endDate) {
    try {
      const body = {
        campaignIds: campaignIds,
        dateRange: {
          start: startDate,
          end: endDate
        },
        metrics: ['impressions', 'clicks', 'cost', 'orders', 'sales']
      };

      const response = await fetch(`${AMAZON_API_BASE}/v2/reports/campaigns`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        throw new Error(`Failed to get campaign performance: ${response.status}`);
      }

      const performance = await response.json();
      return performance;
    } catch (error) {
      console.error('Error getting campaign performance:', error);
      throw error;
    }
  }

  // Anahtar kelime teklifini güncelle
  async updateKeywordBid(accessToken, profileId, keywordId, bid) {
    try {
      const body = [{
        keywordId: keywordId,
        bid: bid
      }];

      const response = await fetch(`${AMAZON_API_BASE}/v2/keywords/biddable`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        throw new Error(`Failed to update keyword bid: ${response.status}`);
      }

      const result = await response.json();
      return result;
    } catch (error) {
      console.error('Error updating keyword bid:', error);
      throw error;
    }
  }

  // Kampanya bütçesini güncelle
  async updateCampaignBudget(accessToken, profileId, campaignId, budget) {
    try {
      const body = [{
        campaignId: campaignId,
        budget: budget
      }];

      const response = await fetch(`${AMAZON_API_BASE}/v2/campaigns`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Amazon-Advertising-API-ClientId': AMAZON_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        throw new Error(`Failed to update campaign budget: ${response.status}`);
      }

      const result = await response.json();
      return result;
    } catch (error) {
      console.error('Error updating campaign budget:', error);
      throw error;
    }
  }
}

// Supabase ile entegrasyon fonksiyonları
export const amazonDbService = {
  // Kullanıcının Amazon hesabını kaydet
  async saveAmazonAccount(userId, accountData) {
    try {
      const { data, error } = await supabase
        .from('amazon_accounts')
        .upsert({
          user_id: userId,
          name: accountData.name,
          client_id: AMAZON_CLIENT_ID,
          refresh_token: accountData.refresh_token,
          access_token: accountData.access_token,
          token_expires_at: new Date(Date.now() + accountData.expires_in * 1000).toISOString(),
          amazon_user_id: accountData.amazon_user_id,
          scope: accountData.scope,
          amazon_profile_id: accountData.profile_id,
          amazon_region: accountData.region,
          status: 'connected'
        });

      if (error) throw error;
      return data;
    } catch (error) {
      console.error('Error saving Amazon account:', error);
      throw error;
    }
  },

  // Kullanıcının Amazon hesabını al
  async getAmazonAccount(userId) {
    try {
      const { data, error } = await supabase
        .from('amazon_accounts')
        .select('*')
        .eq('user_id', userId)
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      console.error('Error getting Amazon account:', error);
      throw error;
    }
  },

  // Token'ı güncelle
  async updateTokens(accountId, accessToken, refreshToken, expiresIn) {
    try {
      const { error } = await supabase
        .from('amazon_accounts')
        .update({
          access_token: accessToken,
          refresh_token: refreshToken,
          token_expires_at: new Date(Date.now() + expiresIn * 1000).toISOString(),
          updated_at: new Date().toISOString()
        })
        .eq('id', accountId);

      if (error) throw error;
    } catch (error) {
      console.error('Error updating tokens:', error);
      throw error;
    }
  },

  // Kampanyaları kaydet
  async saveCampaigns(accountId, campaigns) {
    try {
      const campaignsToInsert = campaigns.map(campaign => ({
        account_id: accountId,
        campaign_id: campaign.campaignId,
        name: campaign.name,
        status: campaign.state,
        budget: campaign.dailyBudget,
        amazon_campaign_id_text: campaign.campaignId,
        amazon_profile_id_text: campaign.profileId,
        raw_data: campaign
      }));

      const { data, error } = await supabase
        .from('amazon_campaigns')
        .upsert(campaignsToInsert, { onConflict: 'campaign_id' });

      if (error) throw error;
      return data;
    } catch (error) {
      console.error('Error saving campaigns:', error);
      throw error;
    }
  },

  // Optimizasyon kuralını kaydet
  async saveOptimizationRule(ruleData) {
    try {
      const { data, error } = await supabase
        .from('optimization_rules')
        .insert(ruleData)
        .select()
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      console.error('Error saving optimization rule:', error);
      throw error;
    }
  },

  // Optimizasyon logunu kaydet
  async saveOptimizationLog(logData) {
    try {
      const { data, error } = await supabase
        .from('optimization_logs')
        .insert(logData)
        .select()
        .single();

      if (error) throw error;
      return data;
    } catch (error) {
      console.error('Error saving optimization log:', error);
      throw error;
    }
  }
};

export const amazonApi = new AmazonAdsAPI(); 