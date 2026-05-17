/**
 * proxy-fetcher.js — Auto-fetch proxies from raw API URLs
 * Ported from Python main.py (lines 820-1060)
 *
 * Periodically fetches proxies from configured URL sources,
 * deduplicates against the current pool, and saves new ones.
 * Supports JSON format (with proxy type info) and plain text.
 * Auto-resumes proxy-paused users when proxies become available.
 */

const fs   = require('fs');
const path = require('path');
const axios = require('axios');
const { PROXY_DIR, shutdownEvent } = require('./config');

// ── Configuration ──────────────────────────────────────────────────────
const RAW_PROXY_SOURCES = [
  // (url, default_scheme) — scheme is used for bare ip:port lines from that source
  ['https://worker-production-a615.up.railway.app/?format=json', 'http'],
];

const RAW_PROXY_FETCH_INTERVAL = 30; // seconds
const RAW_PROXY_SAVE_FILE = path.join(PROXY_DIR, 'raw_fetched_proxies.txt');

// ── SOCKS5 port auto-detection ─────────────────────────────────────────
const SOCKS5_PORTS = new Set([1080, 1081, 4145, 4146, 9050, 9051, 9052, 9053, 10800, 10801, 28100]);

/**
 * Normalize a proxy line using the correct scheme for its source.
 * Handles: scheme://host:port, bare ip:port, ip:port:user:pass, etc.
 * Auto-detects SOCKS5 by port number.
 * Upgrades socks5:// → socks5h:// for remote DNS resolution.
 */
function normalizeWithScheme(line, defaultScheme = 'http') {
  line = line.trim();
  if (!line || line.startsWith('#')) return null;

  // Detect and strip explicit scheme
  let scheme = defaultScheme;
  const low = line.toLowerCase();

  if (low.startsWith('socks5h://')) {
    scheme = 'socks5h';
    line = line.slice(10);
  } else if (low.startsWith('socks5://')) {
    scheme = 'socks5h'; // upgrade to socks5h for remote DNS
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
  } else {
    // No scheme prefix — will apply defaultScheme with SOCKS5 auto-detect
    scheme = defaultScheme;
  }

  // user:pass@host:port format
  if (line.includes('@')) {
    const atIndex = line.lastIndexOf('@');
    const creds = line.slice(0, atIndex);
    const hostport = line.slice(atIndex + 1);
    const parts = hostport.rsplit ? hostport.split(':') : hostport.split(':');
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
      if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
        scheme = 'socks5h';
      }
      return `${scheme}://${host}:${portStr}`;
    }
  } else if (colonParts.length === 4) {
    // ip:port:username:password
    const [ip, portStr, username, password] = colonParts;
    if (ip && /^\d+$/.test(portStr)) {
      if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
        scheme = 'socks5h';
      }
      return `${scheme}://${username}:${password}@${ip}:${portStr}`;
    }
  }

  return null;
}


/**
 * Start the auto-fetch proxy background worker.
 * Returns a stop function that can be called to terminate the loop.
 *
 * @param {object} geoRotator - The GeoRotator instance
 * @param {function} onProxiesAvailable - Callback when new proxies are fetched (for auto-resume)
 * @param {function} touchLiveness - Callback to touch liveness tracker
 * @param {function} persistProxies - Callback to persist proxies to saved_proxies/ + KeyVault
 * @returns {function} stop function
 */
function startProxyFetcher(geoRotator, { onProxiesAvailable, touchLiveness, persistProxies } = {}) {
  let running = true;

  // Ensure proxy folder and save file exist
  try {
    if (!fs.existsSync(PROXY_DIR)) {
      fs.mkdirSync(PROXY_DIR, { recursive: true });
    }
    if (!fs.existsSync(RAW_PROXY_SAVE_FILE)) {
      fs.writeFileSync(RAW_PROXY_SAVE_FILE, '# Auto-fetched proxies — do not edit manually\n', 'utf-8');
      console.log('[RAW-PROXY] Created ' + RAW_PROXY_SAVE_FILE);
    }
  } catch (e) {
    console.error('[RAW-PROXY] Failed to create proxy folder/file:', e.message);
  }

  console.log(
    `[RAW-PROXY] Auto-fetch started — ${RAW_PROXY_SOURCES.length} source(s), ` +
    `interval ${RAW_PROXY_FETCH_INTERVAL}s`
  );

  async function fetchCycle() {
    while (running && !shutdownEvent.isSet()) {
      // Touch liveness at the start of every fetch cycle
      if (touchLiveness) touchLiveness();

      try {
        let totalNew = 0;
        let totalFetched = 0;
        let totalDupes = 0;

        // Collect ALL proxies from ALL sources first, then write once
        const allNormalized = []; // ordered list of unique normalized proxies
        const seen = new Set();

        for (const sourceEntry of RAW_PROXY_SOURCES) {
          let url, scheme;
          if (Array.isArray(sourceEntry)) {
            [url, scheme] = sourceEntry;
          } else {
            url = sourceEntry;
            scheme = 'http';
          }

          let resp;
          try {
            resp = await axios.get(url, { timeout: 30000 });
          } catch (e) {
            console.debug(`[RAW-PROXY] Failed to fetch ${url}: ${e.message}`);
            continue;
          }

          // Try JSON format first (worker returns type info per proxy)
          try {
            const data = resp.data;
            const proxyItems = data?.proxies;
            if (Array.isArray(proxyItems) && proxyItems.length > 0 && typeof proxyItems[0] === 'object') {
              // JSON format with type info — use it to apply correct scheme
              for (const item of proxyItems) {
                const rawProxy = (item.proxy || '').trim();
                const proxyType = (item.type || '').toLowerCase().trim();
                if (!rawProxy || !proxyType) continue;
                totalFetched++;

                // Determine scheme from the type field
                let lineScheme;
                if (proxyType === 'socks5' || proxyType === 'socks5h') {
                  lineScheme = 'socks5h';
                } else if (proxyType === 'socks4') {
                  lineScheme = 'socks5h';
                } else {
                  lineScheme = 'http';
                }

                let normalized;
                try {
                  normalized = normalizeWithScheme(rawProxy, lineScheme);
                } catch { continue; }

                if (!normalized) continue;
                if (seen.has(normalized)) { totalDupes++; continue; }
                seen.add(normalized);
                allNormalized.push(normalized);
                totalNew++;
              }
              continue; // skip plain-text parsing below
            }
          } catch { /* Not JSON or malformed — fall through to plain-text */ }

          // Fallback: plain-text parsing (bare ip:port lines)
          const lines = resp.data?.toString?.()?.split?.('\n') ||
                        (typeof resp.data === 'string' ? resp.data.split('\n') : []);
          const cleanLines = lines
            .map(l => l.trim())
            .filter(l => l && !l.startsWith('#'));
          totalFetched += cleanLines.length;

          for (const rawLine of cleanLines) {
            let normalized;
            try {
              normalized = normalizeWithScheme(rawLine, scheme);
            } catch { continue; }
            if (!normalized) continue;
            if (seen.has(normalized)) { totalDupes++; continue; }
            seen.add(normalized);
            allNormalized.push(normalized);
            totalNew++;
          }
        }

        // Merge new proxies with existing ones in the save file
        if (allNormalized.length > 0 || fs.existsSync(RAW_PROXY_SAVE_FILE)) {
          try {
            if (!fs.existsSync(PROXY_DIR)) fs.mkdirSync(PROXY_DIR, { recursive: true });

            // Load existing proxies from the save file
            const existingProxies = new Set();
            const existingOrder = [];

            if (fs.existsSync(RAW_PROXY_SAVE_FILE)) {
              try {
                const content = fs.readFileSync(RAW_PROXY_SAVE_FILE, 'utf-8');
                for (const line of content.split('\n')) {
                  const trimmed = line.trim();
                  if (trimmed && !trimmed.startsWith('#')) {
                    existingProxies.add(trimmed);
                    existingOrder.push(trimmed);
                  }
                }
              } catch { /* ignore read errors */ }
            }

            // Merge: new proxies are added; existing ones are kept
            const merged = [...existingOrder];
            let newAdded = 0;
            for (const p of allNormalized) {
              if (!existingProxies.has(p)) {
                existingProxies.add(p);
                merged.push(p);
                newAdded++;
              }
            }

            // Write the merged set back
            fs.writeFileSync(
              RAW_PROXY_SAVE_FILE,
              '# Auto-fetched proxies — do not edit manually\n' +
              merged.join('\n') + '\n',
              'utf-8'
            );
            console.log(`[RAW-PROXY] Merged: ${merged.length} total proxies (+${newAdded} new)`);
          } catch (e) {
            console.error(`[RAW-PROXY] Failed to write ${RAW_PROXY_SAVE_FILE}:`, e.message);
          }
        }

        // Reload the proxy pool and trigger callbacks
        if (totalNew > 0) {
          try {
            geoRotator.reload();
            if (touchLiveness) touchLiveness();

            const poolNow = geoRotator.total;
            if (poolNow > 0) {
              console.log(`[RAW-PROXY] Pool reloaded: ${poolNow} proxies`);
            } else {
              console.warn('[RAW-PROXY] Pool still 0 after reload');
            }
          } catch (e) {
            console.error('[RAW-PROXY] Failed to reload proxy pool:', e.message);
          }

          // Auto-resume proxy-paused users if proxies are now available
          if (onProxiesAvailable) {
            try { onProxiesAvailable(); } catch { /* ignore */ }
          }

          // Persist fetched proxies to saved_proxies/ + KeyVault API
          if (persistProxies) {
            try { persistProxies(); } catch { /* ignore */ }
          }

          console.log(
            `[RAW-PROXY] +${totalNew} new proxies ` +
            `(fetched ${totalFetched}, ${totalDupes} dupes) | pool: ${geoRotator.total}`
          );
        } else {
          // Only log at debug level if no new proxies
          if (totalFetched > 0) {
            console.debug(`[RAW-PROXY] No new proxies (fetched ${totalFetched}, ${totalDupes} dupes)`);
          }
        }
      } catch (e) {
        console.error('[RAW-PROXY] Error in fetch cycle:', e.message);
      }

      // Wait for next cycle (interruptible)
      await new Promise(resolve => {
        const timer = setTimeout(resolve, RAW_PROXY_FETCH_INTERVAL * 1000);
        // Allow early termination
        const check = setInterval(() => {
          if (!running || shutdownEvent.isSet()) {
            clearTimeout(timer);
            clearInterval(check);
            resolve();
          }
        }, 1000);
      });
    }

    console.log('[RAW-PROXY] Auto-fetch stopped');
  }

  // Start the fetch loop (non-blocking)
  fetchCycle().catch(e => {
    console.error('[RAW-PROXY] Fatal error:', e.message);
  });

  // Return stop function
  return function stop() {
    running = false;
  };
}


module.exports = {
  startProxyFetcher,
  normalizeWithScheme,
  RAW_PROXY_SAVE_FILE,
  SOCKS5_PORTS,
};
