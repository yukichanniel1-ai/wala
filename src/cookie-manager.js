/**
 * cookie-manager.js — Banned cookies, auto-trim, get_valid_cookies, save_cookie
 * Ported from Python CookieManager class (lines 567-636)
 */
const fs   = require('fs');
const path = require('path');
const { DATA_DIR } = require('./config');

const BANNED_COOKIES_FILE = path.join(DATA_DIR, 'banned_cookies.txt');
const FRESH_COOKIE_FILE   = path.join(path.dirname(__dirname), 'fresh_cookie.txt');

class CookieManager {
  constructor() {
    this.bannedCookies = new Set();
    this._loadBanned();
  }

  _loadBanned() {
    if (fs.existsSync(BANNED_COOKIES_FILE)) {
      try {
        const lines = fs.readFileSync(BANNED_COOKIES_FILE, 'utf-8').split('\n');
        for (const line of lines) {
          const trimmed = line.trim();
          if (trimmed) this.bannedCookies.add(trimmed);
        }
      } catch (e) {
        console.error('[COOKIE] Failed to load banned cookies:', e.message);
      }
    }
  }

  /**
   * Get valid cookies from fresh_cookie.txt, filtering out banned ones.
   * Auto-trims the file to 1000 lines.
   */
  getValidCookies() {
    if (!fs.existsSync(FRESH_COOKIE_FILE)) return [];
    try {
      let lines = fs.readFileSync(FRESH_COOKIE_FILE, 'utf-8').split('\n').filter(l => l.trim());
      
      // Auto-trim at 1000 lines
      if (lines.length > 1000) {
        lines = lines.slice(lines.length - 1000);
        fs.writeFileSync(FRESH_COOKIE_FILE, lines.join('\n'), 'utf-8');
      }

      return lines.filter(l => !this.bannedCookies.has(l.trim()));
    } catch (e) {
      console.error('[COOKIE] Failed to read fresh_cookie.txt:', e.message);
      return [];
    }
  }

  /**
   * Ban a cookie — add to banned set and file.
   */
  banCookie(cookie) {
    const trimmed = cookie.trim();
    if (!trimmed) return;
    this.bannedCookies.add(trimmed);
    try {
      fs.appendFileSync(BANNED_COOKIES_FILE, trimmed + '\n', 'utf-8');
    } catch (e) {
      console.error('[COOKIE] Failed to save banned cookie:', e.message);
    }
  }

  /**
   * Save a cookie to fresh_cookie.txt.
   */
  saveCookie(cookie) {
    const trimmed = cookie.trim();
    if (!trimmed) return;
    try {
      fs.appendFileSync(FRESH_COOKIE_FILE, trimmed + '\n', 'utf-8');
    } catch (e) {
      console.error('[COOKIE] Failed to save cookie:', e.message);
    }
  }

  isBanned(cookie) {
    return this.bannedCookies.has(cookie.trim());
  }
}

module.exports = CookieManager;
