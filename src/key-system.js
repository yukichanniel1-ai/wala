/**
 * key-system.js — License key system: load/save keys, generate, parse duration, redeem
 * Ported from Python main.py (lines 3945-4610)
 */
const fs   = require('fs');
const path = require('path');
const { randomBytes } = require('crypto');
const { KEYS_FILE, DATA_DIR } = require('./config');

// ── Load keys ────────────────────────────────────────────────────────
function loadKeys() {
  if (!fs.existsSync(KEYS_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(KEYS_FILE, 'utf-8'));
  } catch {
    return {};
  }
}

// ── Save keys ────────────────────────────────────────────────────────
function saveKeys(keys) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(KEYS_FILE, JSON.stringify(keys, null, 2), 'utf-8');
}

// ── Generate a unique key ────────────────────────────────────────────
function genKey() {
  return randomBytes(16).toString('hex');
}

// ── Parse duration string (e.g., "1d", "12hrs", "45min", "30days") ──
function parseDuration(str) {
  if (!str) return 0;
  str = str.trim().toLowerCase();

  const match = str.match(/^(\d+)\s*(d|day|days|h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds|w|week|weeks)$/);
  if (!match) {
    // Try pure number (assume seconds)
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

// ── Duration label ───────────────────────────────────────────────────
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

// ── Create a key ─────────────────────────────────────────────────────
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

// ── Redeem a key ─────────────────────────────────────────────────────
// Returns { success, message, combo_limit, key_expires } or { success: false, message }
function redeemKey(key, userId) {
  const keys = loadKeys();
  const keyData = keys[key];

  if (!keyData) {
    return { success: false, message: '❌ Invalid key. Key not found.' };
  }

  // Check expiry
  const now = Date.now() / 1000;
  if (now >= keyData.expires) {
    return { success: false, message: '❌ This key has expired.' };
  }

  // Check max users
  const usedBy = keyData.used_by || [];
  if (keyData.max_users > 0 && usedBy.length >= keyData.max_users) {
    return { success: false, message: '❌ This key has reached its maximum user limit.' };
  }

  // Check if already used by this user
  if (usedBy.includes(userId)) {
    return { success: false, message: 'ℹ️ You have already redeemed this key.' };
  }

  // Redeem
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

// ── Check access (is user allowed to use the bot?) ───────────────────
function checkAccess(userId, savedUsers) {
  // Owner/co-owner always has access
  const { getOwnerId, getCoownerIds } = require('./config');
  if (userId === getOwnerId() || getCoownerIds().includes(userId)) {
    return { allowed: true, reason: 'owner' };
  }

  // Check if user has a valid key
  const profile = savedUsers[String(userId)];
  if (profile?.key_expires) {
    if (Date.now() / 1000 < profile.key_expires) {
      return { allowed: true, reason: 'key' };
    }
    return { allowed: false, reason: 'expired' };
  }

  return { allowed: false, reason: 'no_key' };
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
};
