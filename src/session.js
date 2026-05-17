/**
 * session.js — User session state management, save/load profiles, bot state
 * Ported from Python main.py (lines 3066-3660)
 */
const fs   = require('fs');
const path = require('path');
const { USERS_FILE, DATA_DIR, COMBO_LINE_LIMIT } = require('./config');

// ── In-memory state ──────────────────────────────────────────────────
const botState    = {};   // chatId -> state string
const userData    = {};   // chatId -> user data object
const savedUsers  = {};   // keyId -> profile object (persisted)
const stopEvents  = {};   // chatId -> { _set: bool, set(), isSet() }
const activeBars  = {};   // chatId -> { done, total, live_stats }

// ── Genkey wizard state ──────────────────────────────────────────────
const genkeyWizard = {};  // chatId -> { step, duration, max_users, combo_limit }

// ── Delete key selection state ───────────────────────────────────────
const deleteKeySelection = {};  // chatId -> Set of key names

// ── Proxy accumulator state ─────────────────────────────────────────
const proxyAccumulator = {};  // chatId -> [lines]
const proxyMsgIds      = {};  // chatId -> [message_ids]

// ── Load saved users from disk ───────────────────────────────────────
function loadSavedUsers() {
  if (!fs.existsSync(USERS_FILE)) return;
  try {
    const data = JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8'));
    Object.assign(savedUsers, data);
  } catch (e) {
    console.error('[SESSION] Failed to load saved users:', e.message);
  }
}

// ── Save users to disk ───────────────────────────────────────────────
function saveUsersToDisk() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(USERS_FILE, JSON.stringify(savedUsers, null, 2), 'utf-8');
  } catch (e) {
    console.error('[SESSION] Failed to save users:', e.message);
  }
}

// ── Get or create user data for a chat ──────────────────────────────
function udata(chatId) {
  if (!userData[chatId]) {
    userData[chatId] = {
      hits_id: chatId,
      username: '',
      level: [1],
      clean_filter: 'both',
      key: null,
      key_expires: 0,
      combo_limit: COMBO_LINE_LIMIT,
    };
  }
  return userData[chatId];
}

// ── Get saved profile ────────────────────────────────────────────────
function getSavedProfile(keyId) {
  return savedUsers[keyId] || null;
}

// ── Save profile ─────────────────────────────────────────────────────
function saveProfile(chatId, data) {
  const keyId = String(data.hits_id || chatId);
  savedUsers[keyId] = {
    hits_id: data.hits_id || chatId,
    username: data.username || '',
    level: data.level || [1],
    clean_filter: data.clean_filter || 'both',
    key: data.key || null,
    key_expires: data.key_expires || 0,
    combo_limit: data.combo_limit || COMBO_LINE_LIMIT,
  };
  if (data.username) {
    savedUsers[data.username.toLowerCase().replace('@', '')] = savedUsers[keyId];
  }
  saveUsersToDisk();
}

// ── Stop event management ────────────────────────────────────────────
function getStopEvent(chatId) {
  if (!stopEvents[chatId]) {
    stopEvents[chatId] = { _set: false, set() { this._set = true; }, clear() { this._set = false; }, isSet() { return this._set; } };
  }
  return stopEvents[chatId];
}

function setStopEvent(chatId) {
  const evt = getStopEvent(chatId);
  evt.set();
}

function isStopRequested(chatId) {
  return stopEvents[chatId]?.isSet() || false;
}

function clearStopEvent(chatId) {
  if (stopEvents[chatId]) stopEvents[chatId].clear();
}

function getAllStopEvents() {
  return stopEvents;
}

// ── Active bars ──────────────────────────────────────────────────────
function setActiveBar(chatId, data) {
  activeBars[chatId] = data;
}

function getActiveBar(chatId) {
  return activeBars[chatId] || null;
}

function removeActiveBar(chatId) {
  delete activeBars[chatId];
}

module.exports = {
  botState, userData, savedUsers, stopEvents, activeBars,
  genkeyWizard, deleteKeySelection,
  proxyAccumulator, proxyMsgIds,
  loadSavedUsers, saveUsersToDisk, udata,
  getSavedProfile, saveProfile,
  getStopEvent, setStopEvent, isStopRequested, clearStopEvent, getAllStopEvents,
  setActiveBar, getActiveBar, removeActiveBar,
};
