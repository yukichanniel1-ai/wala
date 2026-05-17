/**
 * proxy-upload.js — Normalize proxy lines, preprocess text, save proxies
 * Ported from Python main.py (lines 4646-5320)
 */
const fs   = require('fs');
const path = require('path');
const { PROXY_DIR } = require('./config');

/**
 * Normalize a single proxy line into a standard format.
 * Handles: host:port, host:port:user:pass, user:pass@host:port,
 * protocol://host:port (http/https/socks4/socks5),
 * and various other formats.
 */
function normalizeProxyLine(line) {
  line = line.trim();
  if (!line || line.startsWith('#')) return null;

  // Remove surrounding quotes/brackets
  line = line.replace(/^["'\[\]()]+|["'\[\]()]+$/g, '').trim();
  if (!line) return null;

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
  const simple = line.match(/^([a-zA-Z0-9.\-]+):(\d+)$/);
  if (simple) {
    return `http://${simple[1]}:${simple[2]}`;
  }

  return null;
}

/**
 * Preprocess proxy text: rejoin lines that Telegram may have wrapped.
 * Telegram sometimes inserts line breaks in long proxy strings.
 */
function preprocessProxyText(text) {
  if (!text) return [];

  // Split into lines
  let lines = text.split(/\r?\n/);

  // Rejoin wrapped lines: if a line doesn't look like a proxy start
  // (no digits, no protocol, no @), it's a continuation of the previous line
  const rejoined = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // Does it look like the start of a proxy?
    const isProxyStart = /^(https?|socks[45]):\/\/|^[^:@\s]+:[0-9]/.test(trimmed) ||
                         /^[a-zA-Z0-9].*:\d+/.test(trimmed);

    if (!isProxyStart && rejoined.length > 0) {
      // Append to previous line
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

  // Generate unique filename
  const timestamp = Date.now();
  const filename  = `proxies_${timestamp}.txt`;
  const filePath  = path.join(PROXY_DIR, filename);

  fs.mkdirSync(PROXY_DIR, { recursive: true });
  fs.writeFileSync(filePath, normalized.join('\n'), 'utf-8');

  return filePath;
}

/**
 * Get a unique proxy file path.
 */
function uniqueProxyPath() {
  return path.join(PROXY_DIR, `proxies_${Date.now()}.txt`);
}

/**
 * Flush proxy accumulator for a chat — save all accumulated lines.
 * Returns { count, filePath } or null.
 */
function flushProxyAccumulator(chatId, proxyAccumulator, proxyMsgIds) {
  const lines = proxyAccumulator[chatId];
  if (!lines || !lines.length) return null;

  const result = saveProxiesFromLines(lines);
  delete proxyAccumulator[chatId];
  delete proxyMsgIds[chatId];

  return { count: lines.length, filePath: result };
}

module.exports = {
  normalizeProxyLine,
  preprocessProxyText,
  saveProxiesFromLines,
  uniqueProxyPath,
  flushProxyAccumulator,
};
