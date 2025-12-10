import { optimizationEngine } from '../src/lib/optimizationEngine.js';
import { createClient } from '@supabase/supabase-js';

// Server-side Supabase client (service role)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Cron job için ana fonksiyon
async function runDailyOptimization() {
  console.log('Starting daily optimization job...');
  
  try {
    // Tüm kullanıcılar için optimizasyon çalıştır
    await optimizationEngine.runOptimizationForAllUsers();
    
    console.log('Daily optimization job completed successfully');
    
    // Başarı logunu kaydet
    await logOptimizationJob('success', 'Daily optimization completed successfully');
    
  } catch (error) {
    console.error('Daily optimization job failed:', error);
    
    // Hata logunu kaydet
    await logOptimizationJob('error', error.message);
    
    throw error;
  }
}

// Optimizasyon job logunu kaydet
async function logOptimizationJob(status, message) {
  try {
    const { error } = await supabase
      .from('optimization_job_logs')
      .insert({
        job_type: 'daily_optimization',
        status: status,
        message: message,
        started_at: new Date().toISOString(),
        completed_at: new Date().toISOString()
      });

    if (error) {
      console.error('Error logging optimization job:', error);
    }
  } catch (error) {
    console.error('Error saving optimization job log:', error);
  }
}

// Manuel çalıştırma için
if (process.argv.includes('--manual')) {
  console.log('Running manual optimization...');
  runDailyOptimization()
    .then(() => {
      console.log('Manual optimization completed');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Manual optimization failed:', error);
      process.exit(1);
    });
}

// Export for use in other modules
export { runDailyOptimization };

// Eğer bu dosya doğrudan çalıştırılırsa
if (import.meta.url === `file://${process.argv[1]}`) {
  runDailyOptimization()
    .then(() => {
      console.log('Optimization job completed');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Optimization job failed:', error);
      process.exit(1);
    });
} 