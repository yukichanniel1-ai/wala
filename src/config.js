/**
 * config.js — Bot configuration, constants, owner/coowner management
 * Ported from Python main.py config loading/saving sections
 * Enhanced with: env var fallbacks, keysystem config, OWNER_USERNAME,
 *                SAVED_PROXIES_DIR, setup wizard
 */
const fs   = require('fs');
const path = require('path');

// ── Paths ──────────────────────────────────────────────────────────────
const DATA_DIR          = path.join(__dirname, '..', 'data');
const CONFIG_FILE       = path.join(__dirname, '..', 'config.json');
const KEYS_FILE         = path.join(DATA_DIR, 'keys.json');
const USERS_FILE        = path.join(DATA_DIR, 'saved_users.json');
const PROXY_DIR         = path.join(__dirname, '..', 'proxies');
const RESULTS_DIR       = path.join(DATA_DIR, 'results');
const SAVED_PROXIES_DIR = path.join(__dirname, '..', 'saved_proxies');

// ── Thread / concurrency constants ─────────────────────────────────────
const MAX_GLOBAL_THREADS    = 10;
const MAX_THREADS_PER_USER  = 5;
const MAX_CONCURRENT_USERS  = 4;
const VIP_THREADS_PER_USER  = 5;
const COMBO_LINE_LIMIT      = 15000;

// ── Garena constants ───────────────────────────────────────────────────
const GARENA_APP_ID     = 10100;
const GARENA_CLIENT_ID  = 100082;
const CODM_PACKAGE      = 'com.garena.game.codm';

// ── Ensure directories exist ───────────────────────────────────────────
function ensureDirs() {
  for (const d of [DATA_DIR, PROXY_DIR, RESULTS_DIR, SAVED_PROXIES_DIR]) {
    if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
  }
}

// ── Config state ───────────────────────────────────────────────────────
let BOT_TOKEN           = '';
let OWNER_ID            = 0;
let OWNER_USERNAME      = '';
let COOWNER_IDS         = [];
let KEYSYSTEM_URL       = '';
let KEYSYSTEM_ADMIN_SECRET = '';

/**
 * Build a config object from environment variables (if set).
 * Environment variables take priority over config.json.
 */
function envConfig() {
  const cfg = {};
  if (process.env.BOT_TOKEN) {
    cfg.bot_token = process.env.BOT_TOKEN.trim();
  }
  if (process.env.OWNER_ID) {
    const parsed = parseInt(process.env.OWNER_ID.trim());
    if (!isNaN(parsed)) cfg.owner_id = parsed;
  }
  if (process.env.OWNER_USERNAME) {
    cfg.owner_username = process.env.OWNER_USERNAME.trim().replace(/^@+/, '');
  }
  if (process.env.COOWNER_IDS) {
    try {
      cfg.coowner_ids = process.env.COOWNER_IDS
        .split(',')
        .map(x => parseInt(x.trim()))
        .filter(x => !isNaN(x));
    } catch { /* ignore */ }
  }
  if (process.env.KEYSYSTEM_URL) {
    cfg.keysystem_url = process.env.KEYSYSTEM_URL.trim();
  }
  if (process.env.KEYSYSTEM_ADMIN_SECRET) {
    cfg.keysystem_admin_secret = process.env.KEYSYSTEM_ADMIN_SECRET.trim();
  }
  return cfg;
}

/**
 * Detect if we're running on Railway/cloud (no TTY).
 */
function isRailway() {
  return !!process.env.RAILWAY_SERVICE_ID ||
         !!process.env.RAILWAY_STATIC_URL ||
         !!process.env.RENDER ||
         !!process.env.RENDER_SERVICE_ID;
}

/**
 * Validate a Telegram bot token by calling getMe.
 */
async function validateToken(token) {
  try {
    const axios = require('axios');
    const resp = await axios.get(`https://api.telegram.org/bot${token}/getMe`, { timeout: 10000 });
    return resp.data?.ok === true ? resp.data.result : null;
  } catch {
    return null;
  }
}

/**
 * Load config.json, then overlay with env vars (they take priority),
 * then try KeyVault API as fallback if critical fields are missing.
 * Returns the config object.
 */
function loadConfig() {
  let cfg = {};

  // 1. Try config.json on disk
  if (fs.existsSync(CONFIG_FILE)) {
    try {
      cfg = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf-8'));
    } catch (e) {
      console.error('[CONFIG] Failed to load config.json:', e.message);
    }
  }

  // 2. Overlay with environment variables (they take priority)
  const env = envConfig();
  if (Object.keys(env).length > 0) {
    cfg = { ...cfg, ...env };
  }

  // 3. If still missing critical fields, try KeyVault API (survives redeploy)
  //    Note: This is synchronous in the initial load; KeyVault fallback is
  //    handled lazily via the setup wizard / wait loop if needed.
  //    The async version is in the startup logic of index.js.

  // Apply to module-level variables
  BOT_TOKEN              = cfg.bot_token              || '';
  OWNER_ID               = cfg.owner_id               || 0;
  OWNER_USERNAME         = (cfg.owner_username        || '').toLowerCase();
  COOWNER_IDS            = cfg.coowner_ids            || [];
  KEYSYSTEM_URL          = cfg.keysystem_url          || '';
  KEYSYSTEM_ADMIN_SECRET = cfg.keysystem_admin_secret || '';

  return cfg;
}

/**
 * Save config.json AND sync to KeyVault API for Railway persistence.
 */
function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2), 'utf-8');

  BOT_TOKEN              = cfg.bot_token              || '';
  OWNER_ID               = cfg.owner_id               || 0;
  OWNER_USERNAME         = (cfg.owner_username        || '').toLowerCase();
  COOWNER_IDS            = cfg.coowner_ids            || [];
  KEYSYSTEM_URL          = cfg.keysystem_url          || '';
  KEYSYSTEM_ADMIN_SECRET = cfg.keysystem_admin_secret || '';

  // Also sync to KeyVault API so config survives Railway redeploy
  try {
    const { getKeySystemAPI } = require('./key-system');
    const api = getKeySystemAPI();
    if (api && api.enabled) {
      api.saveState('bot_config', cfg).catch(() => {});
    }
  } catch { /* ignore — may not be loaded yet */ }
}

function getBotToken()              { return BOT_TOKEN; }
function getOwnerId()               { return OWNER_ID; }
function getOwnerUsername()         { return OWNER_USERNAME; }
function getKeySystemUrl()          { return KEYSYSTEM_URL; }
function getKeySystemAdminSecret()  { return KEYSYSTEM_ADMIN_SECRET; }
function getCoownerIds()            { return COOWNER_IDS; }

function isOwner(fromUser) {
  const uid = fromUser?.id || 0;
  const uname = (fromUser?.username || '').toLowerCase();
  return uid === OWNER_ID ||
         COOWNER_IDS.includes(uid) ||
         (OWNER_USERNAME && uname === OWNER_USERNAME);
}

function isPrimaryOwner(fromUser) {
  const uid = fromUser?.id || 0;
  return uid === OWNER_ID;
}

function addCoowner(uid) {
  if (!COOWNER_IDS.includes(uid)) {
    COOWNER_IDS.push(uid);
    const cfg = loadConfig() || {};
    cfg.coowner_ids = COOWNER_IDS;
    saveConfig(cfg);
  }
}

function removeCoowner(uid) {
  COOWNER_IDS = COOWNER_IDS.filter(id => id !== uid);
  const cfg = loadConfig() || {};
  cfg.coowner_ids = COOWNER_IDS;
  saveConfig(cfg);
}

// ── Proxy files ────────────────────────────────────────────────────────
function getProxyFiles() {
  if (!fs.existsSync(PROXY_DIR)) return [];
  return fs.readdirSync(PROXY_DIR)
    .filter(f => f.endsWith('.txt'))
    .map(f => path.join(PROXY_DIR, f));
}

// ── Shutdown event ─────────────────────────────────────────────────────
const shutdownEvent = {
  _set: false,
  set()    { this._set = true; },
  clear()  { this._set = false; },
  isSet()  { return this._set; },
};

module.exports = {
  DATA_DIR, CONFIG_FILE, KEYS_FILE, USERS_FILE, PROXY_DIR, RESULTS_DIR,
  SAVED_PROXIES_DIR,
  MAX_GLOBAL_THREADS, MAX_THREADS_PER_USER, MAX_CONCURRENT_USERS,
  VIP_THREADS_PER_USER, COMBO_LINE_LIMIT,
  GARENA_APP_ID, GARENA_CLIENT_ID, CODM_PACKAGE,
  ensureDirs, loadConfig, saveConfig,
  envConfig, isRailway, validateToken,
  getBotToken, getOwnerId, getOwnerUsername,
  getKeySystemUrl, getKeySystemAdminSecret, getCoownerIds,
  isOwner, isPrimaryOwner, addCoowner, removeCoowner,
  getProxyFiles, shutdownEvent,
};
