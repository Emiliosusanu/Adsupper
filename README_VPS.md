# VPS Deployment Guide for RobotAds

## 🚀 Setup & Launch

You have successfully migrated to the new VPS-based architecture!

### 1. **Update PM2 Configuration**

The `pm2.config.js` has been updated to run two separate processes:

- `robotads-sync`: Syncs Amazon data every 60 mins.
- `robotads-optimizer`: Checks rules every 15 mins.

### 2. **Restart PM2**

Run the following command on your VPS to apply the changes:

```bash
pm2 restart pm2.config.js --update-env
```

_Or if starting fresh:_

```bash
pm2 start pm2.config.js
```

### 3. **Verify Logs**

Check that both processes are running correctly:

```bash
pm2 logs robotads-sync
pm2 logs robotads-optimizer
```

---

## 🛠 Features Enabled

### ✅ Metrics Sync Fix

- The sync script (`syncAmazonDataVps.js`) was patched.
- **Fixed**: It now handles "No Metrics" issues by falling back to keyword-level aggregation if the main Campaign Report fails.
- **Fixed**: Better error handling for GZIP reports.

### ✅ Server-Side Optimizer

- The optimizer (`optimizerVps.js`) is now fully active.
- It runs independently on the VPS.
- It respects your `frequency_days` and `last_run` schedules.
- Logs actions to `optimization_logs` in Supabase.

---

## ⚠️ Notes

- Ensure your `.env` file on the VPS has all the required variables: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `AMAZON_CLIENT_ID`, `AMAZON_CLIENT_SECRET`.
- You can adjust intervals in `pm2.config.js` (`SYNC_LOOP_MIN`, `CHECK_INTERVAL_MINUTES`).
