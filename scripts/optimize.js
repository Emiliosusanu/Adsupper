// optimize.js
import { runDailyOptimization } from './optimizationCron.js';

async function runOptimizer() {
  console.log('▶️ RobotAds Optimizer started at', new Date().toISOString());

  try {
    await runDailyOptimization();
    console.log('🏁 RobotAds Optimizer finished successfully at', new Date().toISOString());
  } catch (error) {
    console.error('❌ RobotAds Optimizer failed:', error);
    throw error;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runOptimizer()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
}

export { runOptimizer };
