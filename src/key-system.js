/**
 * key-system.js — License key system: local keys + KeyVault API client
 * Ported from Python main.py (lines 3945-4610, 5748-5946)
 *
 * Two modes:
 *  1. LOCAL — keys stored in data/keys.json (default, no API needed)
 *  2. REMOTE — keys managed via KeyVault API (configured with /keysystem)
 *
 * When KeyVault API is enabled, generate/validate/list/delete/revoke
 * operations call the remote API. State persistence (save_state/load_state)
 * is also available for surviving Railway redeploys.
 */
const fs   = require('fs');
const path = require('path');
const axios = require('axios');
const { KEYS_FILE, DATA_DIR, loadConfig, saveConfig } = require('./config');

// ── Local key storage ──────────────────────────────────────────────────
function loadKeys() {
  if (!fs.existsSync(KEYS_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(KEYS_FILE, 'utf-8'));
  } catch {
    return {};
  }
}

function saveKeys(keys) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(KEYS_FILE, JSON.stringify(keys, null, 2), 'utf-8');
}

function genKey() {
  return require('crypto').randomBytes(16).toString('hex');
}

// ── Parse duration string (e.g., "1d", "12hrs", "45min", "30days") ──────
function parseDuration(str) {
  if (!str) return 0;
  str = str.trim().toLowerCase();

  const match = str.match(/^(\d+)\s*(d|day|days|h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds|w|week|weeks)$/);
  if (!match) {
    const num = parseInt(str);
    return isNaN(num) ? 0 : num;
  }

  const val = parseInt(match[1]);
  const unit = match[2];

  if (unit.startsWith('d')) return val * 86400;
  if (unit.startsWith('h')) return val * 3600;
  if (unit.startsWith('m')) return val * 60;
  if (unit.startsWith('s')) return val;
  if (unit.startsWith('w')) return val * 604800;
  return 0;
}

// ── Duration label ──────────────────────────────────────────────────────
function durLabel(seconds) {
  if (seconds >= 86400) {
    const d = Math.floor(seconds / 86400);
    return d === 1 ? '1 Day' : `${d} Days`;
  }
  if (seconds >= 3600) {
    const h = Math.floor(seconds / 3600);
    return h === 1 ? '1 Hour' : `${h} Hours`;
  }
  if (seconds >= 60) {
    const m = Math.floor(seconds / 60);
    return m === 1 ? '1 Minute' : `${m} Minutes`;
  }
  return `${seconds} Seconds`;
}

// ── Create a key (local) ───────────────────────────────────────────────
function createKey(duration, comboLimit, maxUsers = 0) {
  const keys = loadKeys();
  const key = genKey();
  keys[key] = {
    duration: duration,
    expires: Date.now() / 1000 + duration,
    combo_limit: comboLimit,
    max_users: maxUsers,
    used_by: [],
    created_at: Date.now() / 1000,
  };
  saveKeys(keys);
  return key;
}

// ── Redeem a key (local) ───────────────────────────────────────────────
function redeemKey(key, userId) {
  const keys = loadKeys();
  const keyData = keys[key];

  if (!keyData) {
    return { success: false, message: '❌ Invalid key. Key not found.' };
  }

  const now = Date.now() / 1000;
  if (now >= keyData.expires) {
    return { success: false, message: '❌ This key has expired.' };
  }

  const usedBy = keyData.used_by || [];
  if (keyData.max_users > 0 && usedBy.length >= keyData.max_users) {
    return { success: false, message: '❌ This key has reached its maximum user limit.' };
  }

  if (usedBy.includes(userId)) {
    return { success: false, message: 'ℹ️ You have already redeemed this key.' };
  }

  usedBy.push(userId);
  keyData.used_by = usedBy;
  keys[key] = keyData;
  saveKeys(keys);

  return {
    success: true,
    message: `✅ <b>Key redeemed successfully!</b>\n\n🔑 Key: <code>${key}</code>\n⏳ Valid for: <b>${durLabel(keyData.duration)}</b>\n📦 Combo limit: <b>${keyData.combo_limit || 'Unlimited'}</b>`,
    combo_limit: keyData.combo_limit,
    key_expires: keyData.expires,
  };
}

// ── Check access (is user allowed to use the bot?) ─────────────────────
function checkAccess(userId, savedUsers) {
  const { getOwnerId, getCoownerIds } = require('./config');
  if (userId === getOwnerId() || getCoownerIds().includes(userId)) {
    return { allowed: true, reason: 'owner' };
  }

  const profile = savedUsers[String(userId)];
  if (profile?.key_expires) {
    if (Date.now() / 1000 < profile.key_expires) {
      return { allowed: true, reason: 'key' };
    }
    return { allowed: false, reason: 'expired' };
  }

  return { allowed: false, reason: 'no_key' };
}


// ════════════════════════════════════════════════════════════════════════
//  KeySystemAPI — Remote KeyVault API Client
//  Ported from Python main.py KeySystemAPI class (lines 5748-5946)
// ════════════════════════════════════════════════════════════════════════

class KeySystemAPI {
  constructor() {
    const cfg = loadConfig() || {};
    this.base_url = (cfg.keysystem_url || '').replace(/\/+$/, '');
    this.admin_secret = cfg.keysystem_admin_secret || '';
    this.enabled = !!this.base_url;
  }

  /** Build headers for API requests */
  _headers() {
    const h = { 'Content-Type': 'application/json' };
    if (this.admin_secret) {
      h['x-admin-secret'] = this.admin_secret;
    }
    return h;
  }

  /** Reload config from disk (called after /keysystem url/secret) */
  reloadConfig() {
    const cfg = loadConfig() || {};
    this.base_url = (cfg.keysystem_url || '').replace(/\/+$/, '');
    this.admin_secret = cfg.keysystem_admin_secret || '';
    this.enabled = !!this.base_url;
  }

  /**
   * Generate key(s) via the KeyVault API.
   * @param {number} durationSeconds - Key duration in seconds
   * @param {number} maxUsers - Max users who can redeem
   * @param {number} comboLimit - Combo line limit
   * @param {number} [count=1] - Number of keys to generate
   * @param {string} [tier='vip'] - 'free' | 'vip'
   * @param {string} [keyFormat='alphanum'] - 'uuid' | 'hex' | 'alphanum' | 'prefix'
   * @param {string} [label=''] - Optional key label
   * @returns {Promise<Array>} - Array of generated key objects
   */
  async generateKey(durationSeconds, maxUsers, comboLimit, count = 1, tier = 'vip', keyFormat = 'alphanum', label = '') {
    if (!this.enabled) return [];

    const { VIP_THREADS_PER_USER } = require('./config');
    const expiryDays = durationSeconds >= 86400 ? Math.max(1, Math.floor(durationSeconds / 86400)) : 0;

    const generated = [];
    for (let i = 0; i < count; i++) {
      try {
        const payload = {
          label: label || `tg-bot-${tier}`,
          tier: tier,
          format: keyFormat,
          expiryDays: expiryDays,
          rateLimit: comboLimit > 0 ? String(comboLimit) : 'unlimited',
          threads: tier === 'vip' ? VIP_THREADS_PER_USER : 2,
          maxRedemptions: maxUsers > 0 ? maxUsers : null,
        };

        const resp = await axios.post(
          `${this.base_url}/api/keys/generate`,
          payload,
          { headers: this._headers(), timeout: 15000 }
        );

        if (resp.status === 201) {
          generated.push(resp.data);
        } else {
          console.warn(`[KEYSYSTEM] Generate failed: ${resp.status} ${String(resp.data).slice(0, 200)}`);
        }
      } catch (e) {
        console.warn(`[KEYSYSTEM] Generate error: ${e.message}`);
      }
    }
    return generated;
  }

  /**
   * Validate a key via the KeyVault API.
   * @param {string} keyValue - The key string to validate
   * @returns {Promise<Object>} - API response: {valid, reason, key}
   */
  async validateKey(keyValue) {
    if (!this.enabled) return {};
    try {
      const resp = await axios.post(
        `${this.base_url}/api/keys/validate`,
        { key: keyValue },
        { headers: this._headers(), timeout: 15000 }
      );
      return resp.data;
    } catch (e) {
      console.warn(`[KEYSYSTEM] Validate error: ${e.message}`);
      return {};
    }
  }

  /**
   * List all keys from the KeyVault API.
   * @returns {Promise<Array>} - Array of key objects
   */
  async listKeys() {
    if (!this.enabled) return [];
    try {
      const resp = await axios.get(
        `${this.base_url}/api/keys/list`,
        { headers: this._headers(), timeout: 15000 }
      );
      if (resp.status === 200) return resp.data;
    } catch (e) {
      console.warn(`[KEYSYSTEM] List error: ${e.message}`);
    }
    return [];
  }

  /**
   * Delete a key by its ID via the KeyVault API.
   * @param {string} keyId - The key ID to delete
   * @returns {Promise<boolean>}
   */
  async deleteKey(keyId) {
    if (!this.enabled) return false;
    try {
      const resp = await axios.delete(
        `${this.base_url}/api/keys/delete`,
        { headers: this._headers(), data: { id: keyId }, timeout: 15000 }
      );
      return resp.status === 200;
    } catch (e) {
      console.warn(`[KEYSYSTEM] Delete error: ${e.message}`);
      return false;
    }
  }

  /**
   * Revoke a key by its ID via the KeyVault API.
   * @param {string} keyId - The key ID to revoke
   * @returns {Promise<boolean>}
   */
  async revokeKey(keyId) {
    if (!this.enabled) return false;
    try {
      const resp = await axios.post(
        `${this.base_url}/api/keys/revoke`,
        { id: keyId },
        { headers: this._headers(), timeout: 15000 }
      );
      return resp.status === 200;
    } catch (e) {
      console.warn(`[KEYSYSTEM] Revoke error: ${e.message}`);
      return false;
    }
  }

  // ── Bot state persistence (via /api/bot/state) ──────────────────────

  /**
   * Save arbitrary JSON data to KeyVault KV for persistence across redeploys.
   * @param {string} key - State key name
   * @param {*} data - Data to save (must be JSON-serializable)
   * @returns {Promise<boolean>}
   */
  async saveState(key, data) {
    if (!this.enabled) return false;
    try {
      const resp = await axios.post(
        `${this.base_url}/api/bot/state`,
        { key, data },
        { headers: this._headers(), timeout: 15000 }
      );
      return resp.status === 200;
    } catch (e) {
      console.warn(`[KEYSYSTEM] Save state error: ${e.message}`);
      return false;
    }
  }

  /**
   * Load previously saved data from KeyVault KV.
   * @param {string} key - State key name
   * @returns {Promise<*|null>}
   */
  async loadState(key) {
    if (!this.enabled) return null;
    try {
      const resp = await axios.get(
        `${this.base_url}/api/bot/state`,
        { headers: this._headers(), params: { key }, timeout: 15000 }
      );
      if (resp.status === 200) {
        return resp.data.data !== undefined ? resp.data.data : resp.data;
      }
      return null;
    } catch (e) {
      console.warn(`[KEYSYSTEM] Load state error: ${e.message}`);
      return null;
    }
  }

  /**
   * Delete saved state from KeyVault KV.
   * @param {string} key - State key name
   * @returns {Promise<boolean>}
   */
  async deleteState(key) {
    if (!this.enabled) return false;
    try {
      const resp = await axios.delete(
        `${this.base_url}/api/bot/state`,
        { headers: this._headers(), params: { key }, timeout: 15000 }
      );
      return resp.status === 200;
    } catch (e) {
      console.warn(`[KEYSYSTEM] Delete state error: ${e.message}`);
      return false;
    }
  }
}

// ── Singleton instance ─────────────────────────────────────────────────
let _keysystemApi = null;

function getKeySystemAPI() {
  if (!_keysystemApi) {
    _keysystemApi = new KeySystemAPI();
  }
  return _keysystemApi;
}

function resetKeySystemAPI() {
  _keysystemApi = null;
}


module.exports = {
  loadKeys,
  saveKeys,
  genKey,
  parseDuration,
  durLabel,
  createKey,
  redeemKey,
  checkAccess,
  KeySystemAPI,
  getKeySystemAPI,
  resetKeySystemAPI,
};
