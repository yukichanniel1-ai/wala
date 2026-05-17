/**
 * geo-rotator.js — Proxy rotation with normalize, load files, get/rotate/remove
 * Ported from Python main.py GeoRotator class (lines 167-496)
 * Enhanced with: SOCKS5 auto-detection by port, socks5h support,
 *                no-proxy notification support
 */
const fs   = require('fs');
const path = require('path');
const { PROXY_DIR, getProxyFiles } = require('./config');

// ── SOCKS5 auto-detection: well-known SOCKS5 ports ─────────────────────
const SOCKS5_PORTS = new Set([1080, 1081, 4145, 4146, 9050, 9051, 9052, 9053, 10800, 10801, 28100]);

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
   * Enhanced with: SOCKS5 auto-detection by port number, socks5h support
   *
   * Supported formats:
   *   1. http://host:port
   *   2. https://host:port
   *   3. socks5://host:port / socks5h://host:port
   *   4. http://user:pass@host:port
   *   5. host:port → auto-detect: SOCKS5 if known port, else http
   *   6. user:pass@host:port
   *   7. ip:port:username:password
   */
  static normalizeProxy(line) {
    line = line.trim();
    if (!line || line.startsWith('#')) return null;

    // Detect and strip explicit scheme
    let scheme = 'http'; // default
    const low = line.toLowerCase();

    if (low.startsWith('socks5h://')) {
      scheme = 'socks5h';
      line = line.slice(10);
    } else if (low.startsWith('socks5://')) {
      scheme = 'socks5h'; // upgrade to socks5h for remote DNS resolution
      line = line.slice(9);
    } else if (low.startsWith('socks4://')) {
      scheme = 'socks5h'; // upgrade socks4 to socks5h
      line = line.slice(9);
    } else if (low.startsWith('https://')) {
      scheme = 'https';
      line = line.slice(8);
    } else if (low.startsWith('http://')) {
      scheme = 'http';
      line = line.slice(7);
    }

    // user:pass@host:port format (already has @)
    if (line.includes('@')) {
      const atIndex = line.lastIndexOf('@');
      const creds = line.slice(0, atIndex);
      const hostport = line.slice(atIndex + 1);
      const parts = hostport.split(':');
      if (parts.length >= 2) {
        const portStr = parts[parts.length - 1];
        if (/^\d+$/.test(portStr)) {
          // Auto-detect SOCKS5 by port even with auth
          if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
            scheme = 'socks5h';
          }
          return `${scheme}://${creds}@${hostport}`;
        }
      }
      return null;
    }

    // Split by ':' to detect format
    const colonParts = line.split(':');

    if (colonParts.length === 2) {
      // host:port
      const [host, portStr] = colonParts;
      if (host && /^\d+$/.test(portStr)) {
        // Auto-detect SOCKS5 by well-known ports
        if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
          scheme = 'socks5h';
        }
        return `${scheme}://${host}:${portStr}`;
      }
    } else if (colonParts.length === 4) {
      // ip:port:username:password
      const [ip, portStr, username, password] = colonParts;
      if (ip && /^\d+$/.test(portStr)) {
        // Auto-detect SOCKS5 by port
        if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
          scheme = 'socks5h';
        }
        return `${scheme}://${username}:${password}@${ip}:${portStr}`;
      }
    } else if (colonParts.length === 3) {
      // Ambiguous — treat as host:port (ignore third segment)
      const [host, portStr, extra] = colonParts;
      if (host && /^\d+$/.test(portStr)) {
        if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
          scheme = 'socks5h';
        }
        return `${scheme}://${host}:${portStr}`;
      }
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
   * Check if there are any available (non-blocked) proxies.
   */
  hasProxies() {
    return this.getProxies().length > 0;
  }

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

  forceRotate() {
    const available = this.getProxies();
    if (available.length === 0) {
      this.currentProxy = null;
      return null;
    }
    this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
    let attempts = 0;
    while (this.blockedSet.has(this.proxies[this.currentIndex]) && attempts < this.proxies.length) {
      this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
      attempts++;
    }
    this.currentProxy = this.proxies[this.currentIndex] || null;
    return this.currentProxy;
  }

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
