/**
 * combo-parser.js — Validate Garena credentials, parse combo lines
 * Ported from Python main.py (lines 2921-3065)
 */

/**
 * Check if a string looks like a Garena credential (email, phone, or username).
 */
function isGarenaCredential(str) {
  if (!str || typeof str !== 'string') return false;
  str = str.trim();
  if (!str) return false;

  // Email pattern
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(str)) return true;

  // Phone pattern (digits, may start with +)
  if (/^\+?\d{7,15}$/.test(str)) return true;

  // Username pattern (alphanumeric with underscores, 4-30 chars)
  if (/^[a-zA-Z][a-zA-Z0-9_]{3,29}$/.test(str)) return true;

  return false;
}

/**
 * Parse combo lines from text content.
 * Supports formats:
 *   email:password
 *   email----password
 *   email|password
 *   email;password
 *   email password (space separated - least reliable)
 *
 * Returns array of [account, password] tuples.
 * Auto-detects the separator used.
 */
function parseComboLines(text) {
  if (!text) return [];
  const lines = text.split(/\r?\n/);
  const results = [];

  for (let raw of lines) {
    raw = raw.trim();
    if (!raw) continue;

    let account = null, password = null;

    // Try different separators in order of reliability
    // ---- (4 dashes) first as it's most specific
    if (raw.includes('----')) {
      const parts = raw.split('----');
      if (parts.length >= 2) {
        account  = parts[0].trim();
        password = parts.slice(1).join('----').trim();
      }
    }
    // : (colon) — most common, but be careful with emails
    else if (raw.includes(':')) {
      // If first part contains @, split on first :
      const firstColon = raw.indexOf(':');
      const firstPart  = raw.substring(0, firstColon).trim();
      if (firstPart.includes('@') || /^\+?\d{7,15}$/.test(firstPart) || /^[a-zA-Z][a-zA-Z0-9_]{3,29}$/.test(firstPart)) {
        account  = firstPart;
        password = raw.substring(firstColon + 1).trim();
      } else {
        // Try splitting on last :
        const lastColon = raw.lastIndexOf(':');
        account  = raw.substring(0, lastColon).trim();
        password = raw.substring(lastColon + 1).trim();
      }
    }
    // | (pipe)
    else if (raw.includes('|')) {
      const parts = raw.split('|');
      if (parts.length >= 2) {
        account  = parts[0].trim();
        password = parts.slice(1).join('|').trim();
      }
    }
    // ; (semicolon)
    else if (raw.includes(';')) {
      const parts = raw.split(';');
      if (parts.length >= 2) {
        account  = parts[0].trim();
        password = parts.slice(1).join(';').trim();
      }
    }
    // Space (least reliable, only if exactly 2 parts)
    else if (raw.includes(' ')) {
      const parts = raw.split(/\s+/);
      if (parts.length === 2) {
        account  = parts[0].trim();
        password = parts[1].trim();
      }
    }

    if (account && password && isGarenaCredential(account)) {
      results.push([account, password]);
    }
  }

  return results;
}

/**
 * Remove duplicate combos (by account name).
 */
function removeDuplicates(combos) {
  const seen = new Set();
  return combos.filter(([account]) => {
    if (seen.has(account.toLowerCase())) return false;
    seen.add(account.toLowerCase());
    return true;
  });
}

module.exports = {
  isGarenaCredential,
  parseComboLines,
  removeDuplicates,
};
