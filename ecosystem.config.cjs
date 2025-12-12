// ecosystem.config.cjs
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, ".env") });

module.exports = {
  apps: [
    {
      name: "robotads-sync",
      script: "./scripts/syncAmazonDataVps.js",
      cwd: __dirname,
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        SUPABASE_URL: process.env.SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
        AMAZON_CLIENT_ID: process.env.AMAZON_CLIENT_ID,
        AMAZON_CLIENT_SECRET: process.env.AMAZON_CLIENT_SECRET,
        SYNC_LOOP_MIN: "60", // Run sync every 60 minutes
        DAYS_WINDOW: "1", // Test with 1 day only for faster report generation
        STRICT_ONLY_REPORTS: "false", // Allow fallback to aggregated metrics from keywords
      },
    },
    {
      name: "robotads-optimizer",
      script: "./scripts/optimizerVps.js",
      cwd: __dirname,
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        SUPABASE_URL: process.env.SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
        AMAZON_CLIENT_ID: process.env.AMAZON_CLIENT_ID,
        AMAZON_CLIENT_SECRET: process.env.AMAZON_CLIENT_SECRET,
        CHECK_INTERVAL_MINUTES: "15", // Check rules every 15 minutes
      },
    },
    {
      name: "robotads-server",
      script: "./scripts/startOptimizationServer.js",
      cwd: __dirname,
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        SUPABASE_URL: process.env.SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
        AMAZON_CLIENT_ID: process.env.AMAZON_CLIENT_ID,
        AMAZON_CLIENT_SECRET: process.env.AMAZON_CLIENT_SECRET,
        ENABLE_MANUAL_ENDPOINT: "true",
        OPTIMIZATION_SERVER_PORT: "3001",
      },
    },
  ],
};
