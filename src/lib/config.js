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

const envUrl = readEnv('VITE_OPTIMIZATION_SERVER_URL') || readEnv('OPTIMIZATION_SERVER_URL');

// Prefer same-origin proxy in production to avoid CORS. Falls back to env or localhost in dev.
let sameOriginProxy;
try {
  if (typeof window !== 'undefined' && window.location && window.location.hostname && window.location.hostname !== 'localhost') {
    sameOriginProxy = `${window.location.origin}/api/optimizer`;
  }
} catch {}

export const OPTIMIZATION_SERVER_URL = sameOriginProxy || envUrl || 'http://localhost:3001';
