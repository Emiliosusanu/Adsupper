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

export const OPTIMIZATION_SERVER_URL =
  readEnv('VITE_OPTIMIZATION_SERVER_URL') ||
  readEnv('OPTIMIZATION_SERVER_URL') ||
  'http://localhost:3001';
