/**
 * proxy-upload.js — Normalize proxy lines, preprocess text, save proxies,
 *                   persistent proxy storage (saved_proxies/ + KeyVault sync)
 * Ported from Python main.py (lines 137-316, 4646-5320)
 */
const fs   = require('fs');
const path = require('path');
const { PROXY_DIR, SAVED_PROXIES_DIR } = require('./config');

/**
 * Normalize a single proxy line into a standard format.
 * Handles: host:port, host:port:user:pass, user:pass@host:port,
 * protocol://host:port (http/https/socks4/socks5/socks5h),
 * and various other formats. Includes SOCKS5 auto-detection by port.
 */
function normalizeProxyLine(line) {
  line = line.trim();
  if (!line || line.startsWith('#')) return null;

  // Remove surrounding quotes/brackets
  line = line.replace(/^["'[\]()]+|["'[\]()]+$/g, '').trim();
  if (!line) return null;

  // SOCKS5 auto-detection ports
  const SOCKS5_PORTS = new Set([1080, 1081, 4145, 4146, 9050, 9051, 9052, 9053, 10800, 10801, 28100]);

  // Detect and strip explicit scheme
  let scheme = 'http';
  const low = line.toLowerCase();

  if (low.startsWith('socks5h://')) {
    scheme = 'socks5h';
    line = line.slice(10);
  } else if (low.startsWith('socks5://')) {
    scheme = 'socks5h';
    line = line.slice(9);
  } else if (low.startsWith('socks4://')) {
    scheme = 'socks5h';
    line = line.slice(9);
  } else if (low.startsWith('https://')) {
    scheme = 'https';
    line = line.slice(8);
  } else if (low.startsWith('http://')) {
    scheme = 'http';
    line = line.slice(7);
  }

  // user:pass@host:port
  if (line.includes('@')) {
    const atIndex = line.lastIndexOf('@');
    const creds = line.slice(0, atIndex);
    const hostport = line.slice(atIndex + 1);
    const parts = hostport.split(':');
    if (parts.length >= 2) {
      const portStr = parts[parts.length - 1];
      if (/^\d+$/.test(portStr)) {
        if (scheme === 'http' && SOCKS5_PORTS.has(parseInt(portStr))) {
          scheme = 'socks5h';
        }
        return `${scheme}://${creds}@${hostport}`;
      }
    }
    return null;
  }

  // host:port:user:pass
  const fourPart = line.match(/^(.+?):(\d+):(.+?):(.+)$/);
  if (fourPart) {
    const portNum = parseInt(fourPart[2]);
    if (scheme === 'http' && SOCKS5_PORTS.has(portNum)) {
      scheme = 'socks5h';
    }
    return `${scheme}://${fourPart[3]}:${fourPart[4]}@${fourPart[1]}:${fourPart[2]}`;
  }

  // host:port (no auth)
  const simple = line.match(/^([a-zA-Z0-9.\-]+):(\d+)$/);
  if (simple) {
    const portNum = parseInt(simple[2]);
    if (scheme === 'http' && SOCKS5_PORTS.has(portNum)) {
      scheme = 'socks5h';
    }
    return `${scheme}://${simple[1]}:${simple[2]}`;
  }

  return null;
}

/**
 * Preprocess proxy text: rejoin lines that Telegram may have wrapped.
 */
function preprocessProxyText(text) {
  if (!text) return [];

  let lines = text.split(/\r?\n/);
  const rejoined = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const isProxyStart = /^(https?|socks[45]h?):\/\/|^[^:@\s]+:[0-9]/.test(trimmed) ||
                         /^[a-zA-Z0-9].*:\d+/.test(trimmed);

    if (!isProxyStart && rejoined.length > 0) {
      rejoined[rejoined.length - 1] += trimmed;
    } else {
      rejoined.push(trimmed);
    }
  }

  return rejoined;
}

/**
 * Save proxy lines to a unique file in the proxy directory.
 * Returns the path to the saved file.
 */
function saveProxiesFromLines(lines) {
  if (!lines || !lines.length) return null;

  const normalized = [];
  const seen = new Set();

  for (const raw of lines) {
    const proxy = normalizeProxyLine(raw);
    if (proxy && !seen.has(proxy)) {
      seen.add(proxy);
      normalized.push(proxy);
    }
  }

  if (!normalized.length) return null;

  const timestamp = Date.now();
  const filename  = `proxies_${timestamp}.txt`;
  const filePath  = path.join(PROXY_DIR, filename);

  fs.mkdirSync(PROXY_DIR, { recursive: true });
  fs.writeFileSync(filePath, normalized.join('\n'), 'utf-8');

  return filePath;
}

function uniqueProxyPath() {
  return path.join(PROXY_DIR, `proxies_${Date.now()}.txt`);
}

function flushProxyAccumulator(chatId, proxyAccumulator, proxyMsgIds) {
  const lines = proxyAccumulator[chatId];
  if (!lines || !lines.length) return null;

  const result = saveProxiesFromLines(lines);
  delete proxyAccumulator[chatId];
  delete proxyMsgIds[chatId];

  return { count: lines.length, filePath: result };
}


// ════════════════════════════════════════════════════════════════════════
//  PERSISTENT PROXY STORAGE — survives Railway redeploys
//  proxy/        → ephemeral (used by GeoRotator, may be cleaned)
//  saved_proxies/→ persistent (never cleaned, backed up to KeyVault API)
// ════════════════════════════════════════════════════════════════════════

/**
 * Copy all proxy/*.txt files to saved_proxies/ for persistence across restarts.
 * Called after any proxy change (upload, paste, fetch) to keep the backup current.
 */
function syncProxiesToSaved() {
  try {
    if (!fs.existsSync(SAVED_PROXIES_DIR)) {
      fs.mkdirSync(SAVED_PROXIES_DIR, { recursive: true });
    }
    if (!fs.existsSync(PROXY_DIR)) return;

    for (const fname of fs.readdirSync(PROXY_DIR)) {
      if (!fname.endsWith('.txt')) continue;
      const src = path.join(PROXY_DIR, fname);
      const dst = path.join(SAVED_PROXIES_DIR, fname);
      if (!fs.statSync(src).isFile()) continue;
      try {
        fs.copyFileSync(src, dst);
      } catch (e) {
        console.debug(`[PROXY-PERSIST] Failed to sync ${fname}: ${e.message}`);
      }
    }
    console.debug('[PROXY-PERSIST] Synced proxy files to saved_proxies/');
  } catch (e) {
    console.debug('[PROXY-PERSIST] Sync failed:', e.message);
  }
}

/**
 * Restore proxy files from saved_proxies/ to proxy/ on startup.
 * Called before GeoRotator init so the proxy pool is populated from persistent storage.
 * Skips files that already exist in proxy/ (doesn't overwrite newer uploads).
 */
function restoreProxiesFromSaved() {
  try {
    if (!fs.existsSync(SAVED_PROXIES_DIR)) return 0;
    if (!fs.existsSync(PROXY_DIR)) fs.mkdirSync(PROXY_DIR, { recursive: true });

    let restored = 0;
    for (const fname of fs.readdirSync(SAVED_PROXIES_DIR)) {
      if (!fname.endsWith('.txt')) continue;
      const src = path.join(SAVED_PROXIES_DIR, fname);
      const dst = path.join(PROXY_DIR, fname);
      if (!fs.statSync(src).isFile()) continue;

      if (fs.existsSync(dst)) {
        // Only overwrite if saved version is newer or proxy/ version is empty
        try {
          const srcSize = fs.statSync(src).size;
          const dstSize = fs.statSync(dst).size;
          if (dstSize > 0 && dstSize >= srcSize) continue;
        } catch { continue; }
      }

      try {
        fs.copyFileSync(src, dst);
        restored++;
        console.log(`[PROXY-PERSIST] Restored ${fname} from saved_proxies/`);
      } catch (e) {
        console.debug(`[PROXY-PERSIST] Failed to restore ${fname}: ${e.message}`);
      }
    }

    if (restored > 0) {
      console.log(`[PROXY-PERSIST] Restored ${restored} proxy file(s) from saved_proxies/`);
    }
    return restored;
  } catch (e) {
    console.debug('[PROXY-PERSIST] Restore failed:', e.message);
    return 0;
  }
}

/**
 * Save all proxy file contents to KeyVault API for persistence across Railway redeploys.
 * Each file is stored as a separate key: proxy_file_<filename>.
 */
async function syncProxiesToKeyvault() {
  try {
    const { getKeySystemAPI } = require('./key-system');
    const api = getKeySystemAPI();
    if (!api.enabled) return;

    const { getProxyFiles } = require('./config');
    const files = getProxyFiles();
    const manifest = [];

    for (const fpath of files) {
      const fname = path.basename(fpath);
      try {
        const fileContent = fs.readFileSync(fpath, 'utf-8');
        const key = `proxy_file_${fname}`;
        await api.saveState(key, { content: fileContent, filename: fname });
        manifest.push(fname);
      } catch (e) {
        console.debug(`[PROXY-PERSIST] KeyVault sync failed for ${fname}: ${e.message}`);
      }
    }

    // Save manifest
    await api.saveState('proxy_manifest', { files: manifest });
    console.debug(`[PROXY-PERSIST] Synced ${manifest.length} proxy file(s) to KeyVault API`);
  } catch (e) {
    console.debug('[PROXY-PERSIST] KeyVault sync failed:', e.message);
  }
}

/**
 * Load proxy file contents from KeyVault API and write to proxy/ folder.
 * Called on startup after restoreProxiesFromSaved() as a fallback.
 */
async function restoreProxiesFromKeyvault() {
  try {
    const { getKeySystemAPI } = require('./key-system');
    const api = getKeySystemAPI();
    if (!api.enabled) return 0;

    // Load manifest
    const manifestData = await api.loadState('proxy_manifest');
    if (!manifestData || typeof manifestData !== 'object') return 0;
    const manifest = manifestData.files || manifestData;
    if (!Array.isArray(manifest)) return 0;

    if (!fs.existsSync(PROXY_DIR)) fs.mkdirSync(PROXY_DIR, { recursive: true });
    let restored = 0;

    for (const fname of manifest) {
      const key = `proxy_file_${fname}`;
      try {
        const data = await api.loadState(key);
        if (!data || typeof data !== 'object') continue;
        const content = data.content || '';
        if (!content || !content.trim()) continue;

        const dst = path.join(PROXY_DIR, fname);
        // Don't overwrite if file already exists with content
        if (fs.existsSync(dst)) {
          try {
            const existing = fs.readFileSync(dst, 'utf-8');
            if (existing.trim()) continue; // keep local version
          } catch { /* overwrite */ }
        }

        fs.writeFileSync(dst, content, 'utf-8');
        restored++;
        console.log(`[PROXY-PERSIST] Restored ${fname} from KeyVault API`);
      } catch (e) {
        console.debug(`[PROXY-PERSIST] KeyVault restore failed for ${fname}: ${e.message}`);
      }
    }

    if (restored > 0) {
      console.log(`[PROXY-PERSIST] Restored ${restored} proxy file(s) from KeyVault API`);
    }
    return restored;
  } catch (e) {
    console.debug('[PROXY-PERSIST] KeyVault restore failed:', e.message);
    return 0;
  }
}

/**
 * Sync proxy files to both saved_proxies/ and KeyVault API.
 * Call this after any proxy change (upload, paste, fetch).
 */
function persistProxies() {
  syncProxiesToSaved();
  // KeyVault sync is async but we fire-and-forget
  syncProxiesToKeyvault().catch(() => {});
}

/**
 * Restore proxy files from all persistent sources (saved_proxies/ + KeyVault API).
 * Called once at startup before GeoRotator init.
 */
async function restoreAllProxies() {
  const restoredLocal = restoreProxiesFromSaved();
  const restoredApi = await restoreProxiesFromKeyvault();
  const total = restoredLocal + restoredApi;
  if (total > 0) {
    console.log(`[PROXY-PERSIST] ✅ Total ${total} proxy file(s) restored from persistent storage`);
  }
}


module.exports = {
  normalizeProxyLine,
  preprocessProxyText,
  saveProxiesFromLines,
  uniqueProxyPath,
  flushProxyAccumulator,
  // Persistent proxy storage
  syncProxiesToSaved,
  restoreProxiesFromSaved,
  syncProxiesToKeyvault,
  restoreProxiesFromKeyvault,
  persistProxies,
  restoreAllProxies,
};
