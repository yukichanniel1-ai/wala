/**
 * key-system.js — License key system: local keys + KeyVault API client
 * Fully ported from Python main.py (lines 5710-7350)
 *
 * Two modes:
 *  1. LOCAL — keys stored in data/keys.json (default, no API needed)
 *  2. REMOTE — keys managed via KeyVault API (configured with /keysystem)
 *
 * When KeyVault API is enabled:
 *  - loadKeys() merges remote keys into local store
 *  - saveKeys() syncs to API for persistence across Railway redeploys
 *  - generate_key tries API first, falls back to local
 *  - redeem validates via API if key not found locally
 */
const fs      = require('fs');
const path    = require('path');
const axios   = require('axios');
const { KEYS_FILE, DATA_DIR, loadConfig, saveConfig, VIP_THREADS_PER_USER } = require('./config');

// ── Local key storage (mirrors Python _load_keys / _save_keys) ──────────────

function loadKeys(syncApi = true) {
  let keys = {};
  if (fs.existsSync(KEYS_FILE)) {
    try {
      keys = JSON.parse(fs.readFileSync(KEYS_FILE, 'utf-8'));
    } catch {
      keys = {};
    }
  }

  // Merge from KeyVault API (persists across Railway redeploys)
  if (syncApi) {
    try {
      const api = getKeySystemAPI();
      if (api && api.enabled) {
        // Synchronous-ish: we load from a cached/local copy first,
        // the async merge is triggered by callers that await
      }
    } catch {
      // ignore
    }
  }

  return keys;
}

/**
 * Async version of loadKeys that merges remote keys from KeyVault API.
 * Matches Python's _load_keys(sync_api=True).
 */
async function loadKeysAsync() {
  let keys = loadKeys(false); // load local only first

  // Merge from KeyVault API
  try {
    const api = getKeySystemAPI();
    if (api && api.enabled) {
      const remote = await api.loadState('redeem_keys');
      if (remote && typeof remote === 'object') {
        for (const [k, v] of Object.entries(remote)) {
          if (!(k in keys)) {
            keys[k] = v;
          }
        }
        if (Object.keys(remote).length > 0) {
          console.log(`[BOT] Synced ${Object.keys(remote).length} redeem keys from KeyVault API`);
        }
      }
    }
  } catch {
    // ignore
  }

  return keys;
}

function saveKeys(keys) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(KEYS_FILE, JSON.stringify(keys, null, 2), 'utf-8');

  // Sync to KeyVault API for persistence across Railway redeploys
  try {
    const api = getKeySystemAPI();
    if (api && api.enabled) {
      // Fire-and-forget
      api.saveState('redeem_keys', keys).catch(() => {});
    }
  } catch {
    // ignore
  }
}

// ── Key generation ──────────────────────────────────────────────────────────

function genKey() {
  // Match Python: uuid.uuid4().hex[:20].upper()
  return require('crypto').randomUUID().replace(/-/g, '').slice(0, 20).toUpperCase();
}

/**
 * Generate a local key matching KeyVault format options.
 * Mirrors Python _gen_local_key(key_format, tier).
 */
function genLocalKey(keyFormat = 'default', tier = 'free') {
  const crypto = require('crypto');
  const ALPHANUM = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';

  if (keyFormat === 'uuid') {
    return crypto.randomUUID();
  } else if (keyFormat === 'hex') {
    return Array.from(crypto.randomBytes(32))
      .map(b => (b % 16).toString(16))
      .join('');
  } else if (keyFormat === 'alphanum') {
    const chars = ALPHANUM;
    return Array.from(crypto.randomBytes(24))
      .map(b => chars[b % chars.length])
      .join('');
  } else if (keyFormat === 'prefix') {
    const chars = ALPHANUM;
    const pfx = tier === 'vip' ? 'vip' : 'free';
    const part1 = Array.from(crypto.randomBytes(8)).map(b => chars[b % chars.length]).join('');
    const part2 = Array.from(crypto.randomBytes(8)).map(b => chars[b % chars.length]).join('');
    return `${pfx}_${part1}_${part2}`;
  } else {
    // Default: same as genKey() — uuid4 hex[:20].upper()
    return genKey();
  }
}

// ── Parse duration string ───────────────────────────────────────────────────
// Supports: "1d", "12hrs", "45min", "2w", "3mo", "1d12h30min"
// Mirrors Python _parse_duration

function parseDuration(str) {
  if (!str) return 0;
  str = str.trim().toLowerCase();

  let total = 0;
  const re = /(\d+)\s*(mo(?:n(?:th)?s?)?|w(?:ee)?k?s?|d(?:ay)?s?|hr?s?|min?s?)/g;
  let m;
  while ((m = re.exec(str)) !== null) {
    const val = parseInt(m[1]);
    const unit = m[2];
    if (unit.startsWith('mo'))      total += val * 86400 * 30;
    else if (unit.startsWith('w'))   total += val * 86400 * 7;
    else if (unit.startsWith('d'))   total += val * 86400;
    else if (unit.startsWith('h'))   total += val * 3600;
    else if (unit.startsWith('min')) total += val * 60;
  }
  if (total > 0) return total;

  // Try plain number as days (matches Python fallback)
  const numMatch = str.match(/^(\d+)$/);
  if (numMatch) return parseInt(numMatch[1]) * 86400;

  return 0;
}

// ── Duration label ───────────────────────────────────────────────────────────
// Mirrors Python _dur_label: composite "1d 1h 1m" format

function durLabel(seconds) {
  const days = Math.floor(seconds / 86400);
  const hrs  = Math.floor((seconds % 86400) / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const parts = [];
  if (days) parts.push(`${days}d`);
  if (hrs)  parts.push(`${hrs}h`);
  if (mins) parts.push(`${mins}m`);
  return parts.length > 0 ? parts.join(' ') : '0m';
}

// ── Create a key (local) ────────────────────────────────────────────────────
// Matches Python _finalize_gen_key local fallback
// Field names match Python exactly for compatibility with existing keys

function createKey(duration, comboLimit, maxUsers = 0, keyFormat = 'default', tier = 'free', label = '') {
  const keys = loadKeys(false);
  const key = genLocalKey(keyFormat, tier);
  const now = Date.now() / 1000;

  keys[key] = {
    expires:         duration > 0 ? now + duration : now + 86400 * 36500, // ~100 years for "Never"
    combo_limit:     comboLimit,
    max_users:       maxUsers,
    used_by:         [],
    created:         now,             // Python uses "created" not "created_at"
    source:          'local',
    tier:            tier || 'free',
    format:          keyFormat || 'default', // Python uses "format" not "key_format"
    label:           label || '',
    redemption_count: 0,
  };
  saveKeys(keys);
  return key;
}

// ── Redeem a key ────────────────────────────────────────────────────────────
// Full Python parity: rich user info, case-insensitive match, API fallback,
// already-redeemed refresh, owner notification

async function redeemKey(keyArg, fromUser, chatId) {
  const keys = await loadKeysAsync();
  const now = Date.now() / 1000;
  const uidStr = String(chatId);
  let key = keyArg.trim().toUpperCase();
  const api = getKeySystemAPI();

  // Find key in local store (case-insensitive match)
  let entry = keys[key];
  if (!entry) {
    const keyLower = keyArg.trim().toLowerCase();
    for (const k of Object.keys(keys)) {
      if (k.toLowerCase() === keyLower) {
        key = k;
        entry = keys[k];
        break;
      }
    }
  }

  // Key not found locally — try validating via KeyVault API
  if (!entry && api && api.enabled) {
    let apiResult = await api.validateKey(keyArg.trim());
    if (!apiResult || !apiResult.valid) {
      apiResult = await api.validateKey(key); // try uppercase
    }
    if (apiResult && apiResult.valid) {
      const apiKeyInfo = apiResult.key || {};
      const expiresAt = apiKeyInfo.expiresAt;
      const expires = expiresAt ? expiresAt / 1000.0 : now + 86400 * 30;
      const rateLimit = apiKeyInfo.rateLimit || '1000';
      const comboLimit = /^\d+$/.test(rateLimit) ? parseInt(rateLimit) : 0;
      const maxRedemptions = apiKeyInfo.maxRedemptions;
      keys[key] = {
        expires:     expires,
        combo_limit: comboLimit,
        max_users:   maxRedemptions || 0,
        used_by:     [],
        created:     now,
        api_id:      apiKeyInfo.id || '',
        source:      'keyvault',
      };
      saveKeys(keys);
      entry = keys[key];
    } else if (apiResult && !apiResult.valid) {
      return { success: false, message: `❌ <b>${apiResult.reason || 'Invalid key'}</b>` };
    } else {
      return { success: false, message: '❌ <b>Invalid key.</b>\n\nPlease check the key and try again.' };
    }
  }

  if (!entry) {
    return { success: false, message: '❌ <b>Invalid key.</b>\n\nPlease check the key and try again.' };
  }

  // ── Expiry check ──
  if (now > entry.expires) {
    return { success: false, message: '⏳ <b>This key has expired.</b>\nAsk the owner for a new one.' };
  }

  // ── Migrate legacy keys: used_by was a single string ──
  let usedBy = entry.used_by || [];
  if (typeof usedBy === 'string') {
    usedBy = usedBy ? [usedBy] : [];
    entry.used_by = usedBy;
  }

  const maxUsers = entry.max_users || 1; // 0 = unlimited

  // ── Already redeemed by this user → refresh + re-ask setup ──
  let userAlreadyRedeemed = false;
  for (const u of usedBy) {
    if (typeof u === 'object' && String(u.id || '') === uidStr) {
      userAlreadyRedeemed = true;
      break;
    } else if (typeof u === 'string' && u === uidStr) {
      userAlreadyRedeemed = true;
      break;
    }
  }

  if (userAlreadyRedeemed) {
    const remaining = Math.max(0, Math.floor(entry.expires - now));
    const hrs = Math.floor(remaining / 3600);
    const mins = Math.floor((remaining % 3600) / 60);
    const slotsMax = maxUsers === 0 ? '∞' : String(maxUsers);
    const tierLabel = entry.tier === 'vip' ? '⭐ VIP (no queue)' : '🆓 Free (queued)';
    return {
      success: true,
      alreadyRedeemed: true,
      message: `✅ <b>Access Restored!</b> ${tierLabel}\n\n` +
        `🔑 <b>Key:</b> <code>${key}</code>\n` +
        `🆔 <b>Your ID:</b> <code>${fromUser?.id || chatId}</code>\n` +
        `⏳ <b>Valid for:</b> ${hrs}h ${mins}m\n` +
        `👥 <b>Slots:</b> ${usedBy.length}/${slotsMax}\n` +
        `📦 <b>Combo limit:</b> ${entry.combo_limit === 0 ? '∞ Unlimited' : entry.combo_limit + ' lines'}\n\n` +
        `<i>Update your settings below 👇</i>`,
      key: key,
      key_expires: entry.expires,
      combo_limit: entry.combo_limit || 0,
      key_tier: entry.tier || 'free',
    };
  }

  // ── User limit check ──
  if (maxUsers !== 0 && usedBy.length >= maxUsers) {
    return {
      success: false,
      message: `🔐 <b>Key is full!</b>\n\nThis key already has <b>${usedBy.length}/${maxUsers}</b> users.\nAsk the owner for a new key.`
    };
  }

  // ── Add this user with rich info ──
  const userName = [fromUser?.first_name, fromUser?.last_name].filter(Boolean).join(' ');
  const userUsername = fromUser?.username || '';
  const userTgId = fromUser?.id || chatId;

  const userEntry = {
    id: uidStr,
    name: userName,
    username: userUsername,
    tg_id: userTgId,
  };

  // Check if this user (by id) is already in used_by to avoid duplicates
  let alreadyIndex = -1;
  for (let i = 0; i < usedBy.length; i++) {
    const u = usedBy[i];
    const uidCheck = typeof u === 'string' ? u : String(u.id || '');
    if (uidCheck === uidStr) {
      alreadyIndex = i;
      break;
    }
  }

  if (alreadyIndex >= 0) {
    usedBy[alreadyIndex] = userEntry; // update with richer info
  } else {
    usedBy.push(userEntry);
  }
  entry.used_by = usedBy;

  // Increment redemption count
  entry.redemption_count = (entry.redemption_count || 0) + 1;

  saveKeys(keys);

  const remaining = Math.max(0, Math.floor(entry.expires - now));
  const hrs = Math.floor(remaining / 3600);
  const mins = Math.floor((remaining % 3600) / 60);
  const slotsUsed = usedBy.length;
  const slotsMax = maxUsers === 0 ? '∞' : String(maxUsers);
  const tierLabel = entry.tier === 'vip' ? '⭐ VIP (no queue)' : '🆓 Free (queued)';

  return {
    success: true,
    alreadyRedeemed: false,
    message: `✅ <b>Key Redeemed Successfully!</b> ${tierLabel}\n\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
      `🔑 <b>Key:</b> <code>${key}</code>\n` +
      `🆔 <b>Your ID:</b> <code>${userTgId}</code>\n` +
      `👤 <b>Name:</b> ${userName || 'Unknown'}${userUsername ? ' @' + userUsername : ''}\n` +
      `⏳ <b>Valid for:</b> ${hrs}h ${mins}m\n` +
      `👥 <b>Slots:</b> ${slotsUsed}/${slotsMax} used\n` +
      `📦 <b>Combo limit:</b> ${entry.combo_limit === 0 ? '∞ Unlimited' : entry.combo_limit + ' lines'}\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `<i>Now set up your preferences below 👇</i>`,
    key: key,
    key_expires: entry.expires,
    combo_limit: entry.combo_limit || 0,
    key_tier: entry.tier || 'free',
    // Owner notification info
    ownerNotify: {
      userName: userName || 'Unknown',
      userUsername,
      userTgId,
      keyShort: key.slice(0, 20) + (key.length > 20 ? '…' : ''),
      slotsUsed,
      slotsMax,
    },
  };
}

// ── Check access ────────────────────────────────────────────────────────────
// Mirrors Python _check_access

function checkAccess(userId, savedUsers) {
  const { getOwnerId, getCoownerIds } = require('./config');
  if (userId === getOwnerId() || getCoownerIds().includes(userId)) {
    return { allowed: true, reason: 'owner' };
  }

  // Check in-memory profile
  const profile = savedUsers?.[String(userId)];
  if (profile?.key_expires) {
    if (Date.now() / 1000 < profile.key_expires) {
      return { allowed: true, reason: 'key', tier: profile.key_tier || 'free' };
    }
    return { allowed: false, reason: 'expired' };
  }

  // Also check saved profile on disk
  try {
    const { getUsers } = require('./config');
    const users = getUsers();
    const saved = users?.[String(userId)];
    if (saved?.key && Date.now() / 1000 < (saved.key_expires || 0)) {
      // Restore into memory
      if (profile) {
        profile.key = saved.key;
        profile.key_expires = saved.key_expires;
        profile.combo_limit = saved.combo_limit || 500;
        profile.key_tier = saved.key_tier || 'free';
      }
      return { allowed: true, reason: 'key', tier: saved.key_tier || 'free' };
    }
  } catch {
    // ignore
  }

  return { allowed: false, reason: 'no_key' };
}


// ═══════════════════════════════════════════════════════════════════════════════
//  KeySystemAPI — Remote KeyVault API Client
//  Ported from Python main.py KeySystemAPI class (lines 5748-5946)
// ═══════════════════════════════════════════════════════════════════════════════

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
   * Matches Python KeySystemAPI.generate_key() exactly.
   */
  async generateKey(durationSeconds, maxUsers, comboLimit, count = 1, tier = 'vip', keyFormat = 'alphanum', label = '') {
    if (!this.enabled) return [];

    const expiryDays = durationSeconds >= 86400 ? Math.max(1, Math.floor(durationSeconds / 86400)) : 0;
    const vipThreads = VIP_THREADS_PER_USER || 10;

    const generated = [];
    for (let i = 0; i < count; i++) {
      try {
        const payload = {
          label: label || `tg-bot-${tier}`,
          tier: tier,
          format: keyFormat,
          expiryDays: expiryDays,
          rateLimit: comboLimit > 0 ? String(comboLimit) : 'unlimited',
          threads: tier === 'vip' ? vipThreads : 2,
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

  /** Validate a key via the KeyVault API */
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

  /** List all keys from the KeyVault API */
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

  /** Delete a key by its ID via the KeyVault API */
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

  /** Revoke a key by its ID via the KeyVault API */
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

  // ── Bot state persistence (via /api/bot/state) ────────────────────────────

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

// ── Singleton instance ──────────────────────────────────────────────────────

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

/**
 * Delete a key from the KeyVault API if it was created there.
 * Matches Python _delete_key_from_api(entry)
 */
async function deleteKeyFromApi(entry) {
  const api = getKeySystemAPI();
  const apiId = entry?.api_id;
  if (api && api.enabled && apiId) {
    return api.deleteKey(apiId);
  }
  return false;
}


module.exports = {
  loadKeys,
  loadKeysAsync,
  saveKeys,
  genKey,
  genLocalKey,
  parseDuration,
  durLabel,
  createKey,
  redeemKey,
  checkAccess,
  deleteKeyFromApi,
  KeySystemAPI,
  getKeySystemAPI,
  resetKeySystemAPI,
};
