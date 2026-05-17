/**
 * geo-rotator.js — Proxy rotation with normalize, load files, get/rotate/remove
 * Ported from Python main.py GeoRotator class (lines 167-496)
 */
const fs   = require('fs');
const path = require('path');
const { PROXY_DIR, getProxyFiles } = require('./config');

class GeoRotator {
  constructor() {
    this.proxies       = [];       // Array of normalized proxy strings
    this.blockedSet    = new Set();
    this.currentProxy  = null;
    this.currentIndex  = 0;
    this.total         = 0;
    this._loadAllFiles();
    if (this.proxies.length > 0) {
      this.currentProxy = this.proxies[0];
    }
  }

  /**
   * Normalize a proxy line into a standard format.
   * Supports: host:port, host:port:user:pass, user:pass@host:port,
   * and protocol://host:port (http/https/socks4/socks5)
   */
  static normalizeProxy(line) {
    line = line.trim();
    if (!line || line.startsWith('#')) return null;

    // Already has protocol prefix
    const protoMatch = line.match(/^(https?|socks[45]):\/\/(.+)$/i);
    if (protoMatch) {
      const proto = protoMatch[1].toLowerCase();
      const rest  = protoMatch[2];
      // proto://user:pass@host:port
      const authMatch = rest.match(/^(.+?):(.+?)@(.+?):(\d+)$/);
      if (authMatch) {
        return `${proto}://${authMatch[1]}:${authMatch[2]}@${authMatch[3]}:${authMatch[4]}`;
      }
      // proto://host:port
      const hostPort = rest.match(/^(.+?):(\d+)$/);
      if (hostPort) {
        return `${proto}://${hostPort[1]}:${hostPort[2]}`;
      }
      return null;
    }

    // user:pass@host:port
    const atMatch = line.match(/^(.+?):(.+?)@(.+?):(\d+)$/);
    if (atMatch) {
      return `http://${atMatch[1]}:${atMatch[2]}@${atMatch[3]}:${atMatch[4]}`;
    }

    // host:port:user:pass
    const fourPart = line.match(/^(.+?):(\d+):(.+?):(.+)$/);
    if (fourPart) {
      return `http://${fourPart[3]}:${fourPart[4]}@${fourPart[1]}:${fourPart[2]}`;
    }

    // host:port (no auth)
    const simple = line.match(/^(.+?):(\d+)$/);
    if (simple) {
      return `http://${simple[1]}:${simple[2]}`;
    }

    return null;
  }

  _loadAllFiles() {
    const files = getProxyFiles();
    const seen  = new Set();
    this.proxies = [];

    for (const fp of files) {
      try {
        const lines = fs.readFileSync(fp, 'utf-8').split('\n');
        for (const raw of lines) {
          const p = GeoRotator.normalizeProxy(raw);
          if (p && !seen.has(p)) {
            seen.add(p);
            this.proxies.push(p);
          }
        }
      } catch (e) {
        console.error(`[GEO] Failed to read ${fp}: ${e.message}`);
      }
    }

    this.total = this.proxies.length;
  }

  reload() {
    const oldCurrent = this.currentProxy;
    this._loadAllFiles();
    if (oldCurrent && this.proxies.includes(oldCurrent)) {
      this.currentIndex = this.proxies.indexOf(oldCurrent);
    } else {
      this.currentIndex = 0;
    }
    this.currentProxy = this.proxies[this.currentIndex] || null;
  }

  /**
   * Get the current proxy for a given "thread" (just returns current).
   */
  getProxies() {
    return this.proxies.filter(p => !this.blockedSet.has(p));
  }

  getCurrentProxy() {
    return this.currentProxy;
  }

  removeBlockedProxy(proxy) {
    this.blockedSet.add(proxy);
    if (this.currentProxy === proxy) {
      this.forceRotate();
    }
  }

  /**
   * Force rotate to the next available (non-blocked) proxy.
   */
  forceRotate() {
    const available = this.getProxies();
    if (available.length === 0) {
      this.currentProxy = null;
      return null;
    }
    // Move to next index
    this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
    let attempts = 0;
    while (this.blockedSet.has(this.proxies[this.currentIndex]) && attempts < this.proxies.length) {
      this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
      attempts++;
    }
    this.currentProxy = this.proxies[this.currentIndex] || null;
    return this.currentProxy;
  }

  /**
   * Smart rotate: rotate and return new proxy. If no proxies available,
   * try reloading from files.
   */
  smartRotate() {
    const p = this.forceRotate();
    if (!p) {
      this.blockedSet.clear();
      this.reload();
      return this.currentProxy;
    }
    return p;
  }
}

module.exports = GeoRotator;
