/**
 * datadome-manager.js — Set/get/extract/clear datadome cookies, handle_403
 * Ported from Python DataDomeManager class (lines 637-714)
 */
const { PROXY_DIR } = require('./config');

class DataDomeManager {
  constructor() {
    /** Map<proxyString, datadomeCookieValue> */
    this.store = new Map();
  }

  /**
   * Set a datadome cookie for a given proxy.
   */
  set(proxy, cookie) {
    if (proxy && cookie) {
      this.store.set(proxy, cookie);
    }
  }

  /**
   * Get a datadome cookie for a given proxy.
   */
  get(proxy) {
    return this.store.get(proxy) || null;
  }

  /**
   * Extract datadome cookie value from a Set-Cookie header or cookie string.
   */
  static extract(cookieHeader) {
    if (!cookieHeader) return null;
    // Look for datadome=... in Set-Cookie header
    const match = cookieHeader.match(/datadome=([^;]+)/i);
    return match ? match[1] : null;
  }

  /**
   * Clear datadome cookie for a given proxy.
   */
  clear(proxy) {
    this.store.delete(proxy);
  }

  /**
   * Clear all datadome cookies.
   */
  clearAll() {
    this.store.clear();
  }

  /**
   * Handle a 403 response — clear datadome for the proxy and rotate.
   * Returns the new proxy from the geoRotator.
   */
  handle403(proxy, geoRotator) {
    this.clear(proxy);
    if (geoRotator) {
      return geoRotator.smartRotate();
    }
    return proxy;
  }
}

module.exports = DataDomeManager;
