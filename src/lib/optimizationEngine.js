import { supabase } from './supabaseClient.js';
import { amazonApi } from './amazonApi.js';

export class OptimizationEngine {
  constructor() {
    this.isRunning = false;
  }

  // Tüm kullanıcılar için optimizasyon çalıştır
  async runOptimizationForAllUsers() {
    if (this.isRunning) {
      console.log('Optimization already running, skipping...');
      return;
    }

    this.isRunning = true;
    console.log('Starting optimization for all users...');

    try {
      // Tüm aktif Amazon hesaplarını al
      const { data: accounts, error } = await supabase
        .from('amazon_accounts')
        .select('*')
        .eq('status', 'connected');

      if (error) throw error;

      for (const account of accounts) {
        try {
          await this.runOptimizationForUser(account);
        } catch (error) {
          console.error(`Error optimizing for user ${account.user_id}:`, error);
          await this.logError(account.id, 'optimization_engine', error.message);
        }
      }
    } catch (error) {
      console.error('Error in optimization engine:', error);
    } finally {
      this.isRunning = false;
    }
  }

  // Belirli bir kullanıcı için optimizasyon çalıştır
  async runOptimizationForUser(account) {
    console.log(`Running optimization for user ${account.user_id}`);

    // Token'ın geçerliliğini kontrol et ve gerekirse yenile
    const validToken = await this.ensureValidToken(account);
    if (!validToken) {
      console.log(`Invalid token for user ${account.user_id}, skipping...`);
      return;
    }

    // Kullanıcının aktif kurallarını al
    const { data: rules, error: rulesError } = await supabase
      .from('optimization_rules')
      .select('*')
      .eq('user_id', account.user_id)
      .eq('enabled', true)
      .order('priority', { ascending: true });

    if (rulesError) throw rulesError;

    if (!rules || rules.length === 0) {
      console.log(`No active rules for user ${account.user_id}`);
      return;
    }

    // Her kural için optimizasyon çalıştır
    for (const rule of rules) {
      try {
        // Her kural için frekans kontrolü (settings.frequency_days veya settings.frequency_hours)
        if (!this.shouldRunRule(rule)) {
          console.log(`Skipping rule ${rule.name} - frequency not reached.`);
          continue;
        }

        await this.applyRule(account, rule);
        
        // Kuralın son çalışma zamanını güncelle
        await supabase
          .from('optimization_rules')
          .update({ last_run: new Date().toISOString() })
          .eq('id', rule.id);

      } catch (error) {
        console.error(`Error applying rule ${rule.id}:`, error);
        await this.logError(account.id, 'rule_application', error.message, { rule_id: rule.id });
      }
    }
  }

  // Bir kuralın çalıştırılıp çalıştırılmaması gerektiğini belirle
  shouldRunRule(rule) {
    const settings = rule.settings || {};

    // Öncelik: frequency_days, yoksa frequency_hours'u güne çevir
    const frequencyDays = settings.frequency_days != null
      ? Number(settings.frequency_days) || 0
      : (settings.frequency_hours != null
          ? (Number(settings.frequency_hours) || 0) / 24
          : 1); // varsayılan: günde bir

    if (frequencyDays <= 0) {
      // 0 veya negatif ise her çalıştırmada uygula
      return true;
    }

    if (!rule.last_run) {
      return true;
    }

    const lastRun = new Date(rule.last_run).getTime();
    const now = Date.now();
    const hoursSinceLastRun = (now - lastRun) / (1000 * 60 * 60);
    const requiredHours = frequencyDays * 24;

    return hoursSinceLastRun >= requiredHours;
  }

  // Token'ın geçerliliğini kontrol et ve gerekirse yenile
  async ensureValidToken(account) {
    const tokenExpiry = new Date(account.token_expires_at);
    const now = new Date();
    const timeUntilExpiry = tokenExpiry.getTime() - now.getTime();

    // Token 5 dakika içinde sona erecekse yenile
    if (timeUntilExpiry < 5 * 60 * 1000) {
      try {
        const newTokens = await amazonApi.refreshAccessToken(account.refresh_token);
        
        await supabase
          .from('amazon_accounts')
          .update({
            access_token: newTokens.access_token,
            refresh_token: newTokens.refresh_token,
            token_expires_at: new Date(Date.now() + newTokens.expires_in * 1000).toISOString(),
            updated_at: new Date().toISOString()
          })
          .eq('id', account.id);

        account.access_token = newTokens.access_token;
        account.refresh_token = newTokens.refresh_token;
        account.token_expires_at = new Date(Date.now() + newTokens.expires_in * 1000).toISOString();

        console.log(`Token refreshed for user ${account.user_id}`);
      } catch (error) {
        console.error(`Failed to refresh token for user ${account.user_id}:`, error);
        return false;
      }
    }

    return true;
  }

  // Kuralı uygula
  async applyRule(account, rule) {
    console.log(`Applying rule: ${rule.name}`);

    // Kampanyaları al
    const campaigns = await this.getCampaignsForRule(account, rule);
    if (!campaigns || campaigns.length === 0) {
      console.log(`No campaigns found for rule ${rule.name}`);
      return;
    }

    // Kampanya performans verilerini al
    const campaignIds = campaigns.map(c => c.campaignId);
    const endDate = new Date().toISOString().split('T')[0];
    // Kural bazlı lookback penceresi (kaç günlük veri analiz edilecek)
    const lookbackDays = (rule.settings && rule.settings.lookback_days)
      ? Number(rule.settings.lookback_days) || 7
      : 7;
    const startDate = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000)
      .toISOString()
      .split('T')[0];

    const performance = await amazonApi.getCampaignPerformance(
      account.access_token,
      account.amazon_profile_id,
      campaignIds,
      startDate,
      endDate
    );

    // Her kampanya için kuralı kontrol et
    for (const campaign of campaigns) {
      const campaignPerformance = performance.find(p => p.campaignId === campaign.campaignId);
      if (!campaignPerformance) continue;

      const shouldApplyAction = this.evaluateCondition(campaignPerformance, rule.settings);
      
      if (shouldApplyAction) {
        await this.executeAction(account, rule, campaign, campaignPerformance);
      }
    }
  }

  // Kural için kampanyaları al
  async getCampaignsForRule(account, rule) {
    if (rule.match_type === 'ALL') {
      // Tüm kampanyaları al
      const campaigns = await amazonApi.getCampaigns(
        account.access_token,
        account.amazon_profile_id
      );
      return campaigns;
    } else if (rule.match_type === 'SELECTED' && rule.settings.campaign_ids) {
      // Seçili kampanyaları al
      const { data: selectedCampaigns, error } = await supabase
        .from('amazon_campaigns')
        .select('campaign_id, name')
        .in('id', rule.settings.campaign_ids);

      if (error) throw error;
      return selectedCampaigns.map(c => ({ campaignId: c.campaign_id, name: c.name }));
    }

    return [];
  }

  // Koşulu değerlendir
  evaluateCondition(performance, settings) {
    const conditions = (settings.conditions && settings.conditions.length > 0)
      ? settings.conditions
      : [{ metric: settings.metric, condition: settings.condition, threshold: settings.threshold }];

    for (const cond of conditions) {
      const metricValue = this.getMetricValue(performance, cond.metric);

      switch (cond.condition) {
        case 'greater_than':
          if (!(metricValue > cond.threshold)) return false;
          break;
        case 'less_than':
          if (!(metricValue < cond.threshold)) return false;
          break;
        case 'equals':
          if (!(Math.abs(metricValue - cond.threshold) < 0.01)) return false;
          break;
        default:
          return false;
      }
    }

    return true;
  }

  // Metrik değerini al
  getMetricValue(performance, metric) {
    switch (metric) {
      case 'acos':
        return performance.acos || 0;
      case 'ctr':
        return performance.ctr || 0;
      case 'cpc':
        return performance.cpc || 0;
      case 'spend':
        return performance.cost || 0;
      case 'orders':
        return performance.orders || 0;
      default:
        return 0;
    }
  }

  // Aksiyonu uygula
  async executeAction(account, rule, campaign, performance) {
    console.log(`Executing action for campaign ${campaign.name}: ${rule.settings.action}`);

    try {
      let actionResult = null;
      let entityType = 'campaign';
      let entityId = campaign.campaignId;

      switch (rule.settings.action) {
        case 'decrease_bid':
          actionResult = await this.decreaseKeywordBids(account, campaign, rule.settings.action_value);
          entityType = 'keyword';
          break;
        
        case 'increase_bid':
          actionResult = await this.increaseKeywordBids(account, campaign, rule.settings.action_value);
          entityType = 'keyword';
          break;
        
        case 'pause_campaign':
          actionResult = await this.pauseCampaign(account, campaign);
          break;
        
        case 'adjust_budget':
          actionResult = await this.adjustCampaignBudget(account, campaign, rule.settings.action_value);
          break;
        
        default:
          console.log(`Unknown action: ${rule.settings.action}`);
          return;
      }

      // Optimizasyon logunu kaydet
      await this.saveOptimizationLog({
        user_id: account.user_id,
        rule_id: rule.id,
        amazon_account_id: account.id,
        campaign_id: campaign.campaignId,
        entity_type: entityType,
        entity_id: entityId,
        action: rule.settings.action,
        reason: (rule.settings.conditions && rule.settings.conditions.length > 0)
          ? rule.settings.conditions
              .map(c => `${c.metric} ${c.condition} ${c.threshold}`)
              .join(' AND ')
          : `${rule.settings.metric} ${rule.settings.condition} ${rule.settings.threshold}`,
        details: {
          performance,
          action_value: rule.settings.action_value,
          result: actionResult
        }
      });

      console.log(`Action executed successfully for campaign ${campaign.name}`);

    } catch (error) {
      console.error(`Error executing action for campaign ${campaign.name}:`, error);
      throw error;
    }
  }

  // Anahtar kelime tekliflerini azalt
  async decreaseKeywordBids(account, campaign, percentage) {
    const adGroups = await amazonApi.getAdGroups(
      account.access_token,
      account.amazon_profile_id,
      campaign.campaignId
    );

    const results = [];
    for (const adGroup of adGroups) {
      const keywords = await amazonApi.getKeywords(
        account.access_token,
        account.amazon_profile_id,
        adGroup.adGroupId
      );

      for (const keyword of keywords) {
        if (keyword.status === 'ENABLED') {
          const newBid = keyword.bid * (1 - percentage / 100);
          const result = await amazonApi.updateKeywordBid(
            account.access_token,
            account.amazon_profile_id,
            keyword.keywordId,
            newBid
          );
          results.push(result);
        }
      }
    }

    return results;
  }

  // Anahtar kelime tekliflerini artır
  async increaseKeywordBids(account, campaign, percentage) {
    const adGroups = await amazonApi.getAdGroups(
      account.access_token,
      account.amazon_profile_id,
      campaign.campaignId
    );

    const results = [];
    for (const adGroup of adGroups) {
      const keywords = await amazonApi.getKeywords(
        account.access_token,
        account.amazon_profile_id,
        adGroup.adGroupId
      );

      for (const keyword of keywords) {
        if (keyword.status === 'ENABLED') {
          const newBid = keyword.bid * (1 + percentage / 100);
          const result = await amazonApi.updateKeywordBid(
            account.access_token,
            account.amazon_profile_id,
            keyword.keywordId,
            newBid
          );
          results.push(result);
        }
      }
    }

    return results;
  }

  // Kampanyayı duraklat
  async pauseCampaign(account, campaign) {
    // Bu işlem için Amazon API'de kampanya durumunu güncelleme endpoint'i kullanılmalı
    // Şimdilik sadece log kaydı oluşturuyoruz
    console.log(`Would pause campaign: ${campaign.name}`);
    return { action: 'pause_campaign', campaign_id: campaign.campaignId };
  }

  // Kampanya bütçesini ayarla
  async adjustCampaignBudget(account, campaign, percentage) {
    // Mevcut bütçeyi al ve yeni bütçeyi hesapla
    const currentBudget = campaign.dailyBudget || 0;
    const newBudget = currentBudget * (1 + percentage / 100);

    const result = await amazonApi.updateCampaignBudget(
      account.access_token,
      account.amazon_profile_id,
      campaign.campaignId,
      newBudget
    );

    return result;
  }

  // Optimizasyon logunu kaydet
  async saveOptimizationLog(logData) {
    try {
      const { error } = await supabase
        .from('optimization_logs')
        .insert(logData);

      if (error) throw error;
    } catch (error) {
      console.error('Error saving optimization log:', error);
    }
  }

  // Hata logunu kaydet
  async logError(accountId, functionName, message, context = {}) {
    try {
      const { error } = await supabase
        .from('error_logs')
        .insert({
          account_id: accountId,
          function_name: functionName,
          message: message,
          context: context
        });

      if (error) throw error;
    } catch (error) {
      console.error('Error saving error log:', error);
    }
  }
}

// Singleton instance
export const optimizationEngine = new OptimizationEngine();

// Manuel optimizasyon çalıştırma fonksiyonu
export const runManualOptimization = async (userId) => {
  try {
    const { data: account, error } = await supabase
      .from('amazon_accounts')
      .select('*')
      .eq('user_id', userId)
      .single();

    if (error) throw error;
    if (!account) throw new Error('Amazon account not found');

    await optimizationEngine.runOptimizationForUser(account);
    return { success: true };
  } catch (error) {
    console.error('Manual optimization failed:', error);
    return { success: false, error: error.message };
  }
}; 