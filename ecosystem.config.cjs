// ecosystem.config.cjs
require("dotenv").config();
module.exports = {
  apps: [
    {
      name: "robotads-sync",
      script: "./scripts/syncAmazonDataVps.js",
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        SUPABASE_URL: process.env.SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
        AMAZON_CLIENT_ID: process.env.AMAZON_CLIENT_ID,
        AMAZON_CLIENT_SECRET: process.env.AMAZON_CLIENT_SECRET,
        SYNC_LOOP_MIN: "60", // Run sync every 60 minutes
        STRICT_ONLY_REPORTS: "true", // Strict mode: do not aggregate from keywords if report fails
      },
    },
    {
      name: "robotads-optimizer",
      script: "./scripts/optimizerVps.js",
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
