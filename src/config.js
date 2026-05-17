/**
 * config.js — Bot configuration, constants, owner/coowner management
 * Ported from Python main.py config loading/saving sections
 */
const fs   = require('fs');
const path = require('path');

// ── Paths ──────────────────────────────────────────────────────────────
const DATA_DIR     = path.join(__dirname, '..', 'data');
const CONFIG_FILE  = path.join(__dirname, '..', 'config.json');
const KEYS_FILE    = path.join(DATA_DIR, 'keys.json');
const USERS_FILE   = path.join(DATA_DIR, 'saved_users.json');
const PROXY_DIR    = path.join(__dirname, '..', 'proxies');
const RESULTS_DIR  = path.join(DATA_DIR, 'results');

// ── Thread / concurrency constants ─────────────────────────────────────
const MAX_GLOBAL_THREADS    = 10;   // Max concurrent checks across ALL users (free plan optimized)
const MAX_THREADS_PER_USER  = 5;    // 5 threads per user ✅
const MAX_CONCURRENT_USERS  = 4;    // Max users checking at same time
const VIP_THREADS_PER_USER  = 5;    // Owner also gets 5 (same, fair on free plan)
const COMBO_LINE_LIMIT      = 15000; // Reduced for 512MB RAM

// ── Garena constants ──────────────────────────────────────────────────
const GARENA_APP_ID     = 10100;
const GARENA_CLIENT_ID  = 100082;
const CODM_PACKAGE      = 'com.garena.game.codm';

// ── Ensure directories exist ──────────────────────────────────────────
function ensureDirs() {
  for (const d of [DATA_DIR, PROXY_DIR, RESULTS_DIR]) {
    if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
  }
}

// ── Config state ──────────────────────────────────────────────────────
let BOT_TOKEN    = '';
let OWNER_ID     = 0;
let COOWNER_IDS  = [];

/**
 * Load config.json. Returns the config object.
 */
function loadConfig() {
  if (!fs.existsSync(CONFIG_FILE)) {
    return null;
  }
  try {
    const raw = fs.readFileSync(CONFIG_FILE, 'utf-8');
    const cfg = JSON.parse(raw);
    BOT_TOKEN   = cfg.bot_token   || '';
    OWNER_ID    = cfg.owner_id    || 0;
    COOWNER_IDS = cfg.coowner_ids || [];
    return cfg;
  } catch (e) {
    console.error('[CONFIG] Failed to load config.json:', e.message);
    return null;
  }
}

/**
 * Save config.json.
 */
function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2), 'utf-8');
  BOT_TOKEN   = cfg.bot_token   || '';
  OWNER_ID    = cfg.owner_id    || 0;
  COOWNER_IDS = cfg.coowner_ids || [];
}

function getBotToken()   { return BOT_TOKEN; }
function getOwnerId()    { return OWNER_ID; }
function getCoownerIds() { return COOWNER_IDS; }

function isOwner(fromUser) {
  const uid = fromUser?.id || 0;
  return uid === OWNER_ID || COOWNER_IDS.includes(uid);
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

// ── Proxy files ──────────────────────────────────────────────────────
function getProxyFiles() {
  if (!fs.existsSync(PROXY_DIR)) return [];
  return fs.readdirSync(PROXY_DIR)
    .filter(f => f.endsWith('.txt'))
    .map(f => path.join(PROXY_DIR, f));
}

// ── Shutdown event ───────────────────────────────────────────────────
const shutdownEvent = {
  _set: false,
  set()    { this._set = true; },
  clear()  { this._set = false; },
  isSet()  { return this._set; },
};

module.exports = {
  DATA_DIR, CONFIG_FILE, KEYS_FILE, USERS_FILE, PROXY_DIR, RESULTS_DIR,
  MAX_GLOBAL_THREADS, MAX_THREADS_PER_USER, MAX_CONCURRENT_USERS,
  VIP_THREADS_PER_USER, COMBO_LINE_LIMIT,
  GARENA_APP_ID, GARENA_CLIENT_ID, CODM_PACKAGE,
  ensureDirs, loadConfig, saveConfig,
  getBotToken, getOwnerId, getCoownerIds,
  isOwner, isPrimaryOwner, addCoowner, removeCoowner,
  getProxyFiles, shutdownEvent,
};
