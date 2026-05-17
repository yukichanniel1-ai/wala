/**
 * live-stats.js — Real-time statistics with progress bars, fancy telegram display
 * Ported from Python LiveStats class (lines 715-1022)
 */

class LiveStats {
  /**
   * @param {number} total - Total accounts to check
   * @param {number[]} levelThresholds - e.g. [100] or [1] for all
   * @param {string} cleanFilter - 'clean' | 'notclean' | 'both'
   */
  constructor(total, levelThresholds = [1], cleanFilter = 'both') {
    this.total         = total;
    this.levelThresholds = levelThresholds;
    this.cleanFilter   = cleanFilter;

    // Counters
    this.checked       = 0;
    this.valid         = 0;   // hits (total valid accounts)
    this.invalid       = 0;   // bad
    this.twoFa         = 0;   // 2fa accounts
    this.errors        = 0;
    this.clean         = 0;
    this.notClean      = 0;
    this.hasCodm       = 0;
    this.noCodm        = 0;
    this.skipped       = 0;

    // Level distribution
    this.levelDist     = {};
    // Server distribution
    this.serverDist    = {};

    // Speed tracking
    this.startTime     = Date.now();
    this.lastChecked   = 0;
    this.lastTime      = Date.now();
    this.speed         = 0;   // accounts per second
  }

  /**
   * Record a valid (hit) account.
   */
  recordHit(accountInfo) {
    this.valid++;
    this.checked++;
    this._updateSpeed();

    if (accountInfo) {
      if (accountInfo.is_clean) {
        this.clean++;
      } else {
        this.notClean++;
      }
      if (accountInfo.has_codm) {
        this.hasCodm++;
      } else {
        this.noCodm++;
      }

      // Level distribution
      const lvl = accountInfo.level || 0;
      const bucket = this._levelBucket(lvl);
      this.levelDist[bucket] = (this.levelDist[bucket] || 0) + 1;

      // Server distribution
      if (accountInfo.server) {
        this.serverDist[accountInfo.server] = (this.serverDist[accountInfo.server] || 0) + 1;
      }
    }
  }

  /**
   * Record a bad (invalid) account.
   */
  recordBad() {
    this.invalid++;
    this.checked++;
    this._updateSpeed();
  }

  /**
   * Record a 2FA account.
   */
  record2Fa() {
    this.twoFa++;
    this.checked++;
    this._updateSpeed();
  }

  /**
   * Record an error.
   */
  recordError() {
    this.errors++;
    this.checked++;
    this._updateSpeed();
  }

  /**
   * Record a skipped account.
   */
  recordSkip() {
    this.skipped++;
    this.checked++;
    this._updateSpeed();
  }

  _updateSpeed() {
    const now = Date.now();
    const dt  = (now - this.lastTime) / 1000;
    if (dt >= 1) {
      const dc = this.checked - this.lastChecked;
      this.speed       = dc / dt;
      this.lastChecked = this.checked;
      this.lastTime    = now;
    }
  }

  _levelBucket(lvl) {
    if (lvl >= 400) return '400+';
    if (lvl >= 300) return '300-399';
    if (lvl >= 200) return '200-299';
    if (lvl >= 100) return '100-199';
    if (lvl >= 50)  return '50-99';
    if (lvl >= 1)   return '1-49';
    return '0';
  }

  /**
   * Get elapsed time string.
   */
  getElapsed() {
    const secs = Math.floor((Date.now() - this.startTime) / 1000);
    return this._formatDuration(secs);
  }

  /**
   * Get ETA string.
   */
  getEta() {
    if (this.speed <= 0 || this.checked >= this.total) return '—';
    const remaining = this.total - this.checked;
    const etaSecs   = remaining / this.speed;
    return this._formatDuration(Math.floor(etaSecs));
  }

  _formatDuration(secs) {
    if (secs < 0) secs = 0;
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = secs % 60;
    if (h > 0) return `${h}h${m}m${s}s`;
    if (m > 0) return `${m}m${s}s`;
    return `${s}s`;
  }

  /**
   * Build a progress bar string.
   */
  _makeBar(pct, width = 20) {
    const filled = Math.round(pct / 100 * width);
    const empty  = width - filled;
    return '█'.repeat(filled) + '░'.repeat(empty);
  }

  /**
   * Get Telegram-friendly progress text.
   */
  getTelegramProgress() {
    const pct = this.total > 0 ? (this.checked / this.total * 100).toFixed(1) : '0.0';
    const bar = this._makeBar(parseFloat(pct));
    return (
      `📊 ${bar} ${pct}%\n` +
      `✅ Valid: <b>${this.valid}</b> | ❌ Bad: <b>${this.invalid}</b> | 🔐 2FA: <b>${this.twoFa}</b>\n` +
      `⚡ Speed: <b>${this.speed.toFixed(1)}/s</b> | ⏱ ETA: <b>${this.getEta()}</b>`
    );
  }

  /**
   * Get fancy Telegram progress with level/server distribution.
   */
  getFancyTelegramProgress() {
    let text = this.getTelegramProgress();

    // Level distribution
    const levelKeys = Object.keys(this.levelDist).sort();
    if (levelKeys.length > 0) {
      text += '\n\n🎮 <b>Level Distribution:</b>';
      for (const k of levelKeys) {
        text += `\n  Level ${k}: <b>${this.levelDist[k]}</b>`;
      }
    }

    // Server distribution (top 5)
    const serverKeys = Object.entries(this.serverDist)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);
    if (serverKeys.length > 0) {
      text += '\n\n🌍 <b>Top Servers:</b>';
      for (const [k, v] of serverKeys) {
        text += `\n  ${k}: <b>${v}</b>`;
      }
    }

    // Clean/Not Clean
    text += `\n\n✅ Clean: <b>${this.clean}</b> | ❌ Not Clean: <b>${this.notClean}</b>`;
    text += `\n🎮 CODM: <b>${this.hasCodm}</b> | 🚫 No CODM: <b>${this.noCodm}</b>`;

    return text;
  }

  /**
   * Serialize for save_progress.
   */
  toJSON() {
    return {
      total: this.total,
      checked: this.checked,
      valid: this.valid,
      invalid: this.invalid,
      twoFa: this.twoFa,
      errors: this.errors,
      clean: this.clean,
      notClean: this.notClean,
      hasCodm: this.hasCodm,
      noCodm: this.noCodm,
      skipped: this.skipped,
      levelDist: this.levelDist,
      serverDist: this.serverDist,
      startTime: this.startTime,
    };
  }

  /**
   * Restore from saved progress.
   */
  static fromJSON(data) {
    const ls = new LiveStats(data.total, data.levelThresholds || [1], data.cleanFilter || 'both');
    Object.assign(ls, {
      checked:    data.checked    || 0,
      valid:      data.valid      || 0,
      invalid:    data.invalid    || 0,
      twoFa:      data.twoFa      || 0,
      errors:     data.errors     || 0,
      clean:      data.clean      || 0,
      notClean:   data.notClean   || 0,
      hasCodm:    data.hasCodm    || 0,
      noCodm:     data.noCodm     || 0,
      skipped:    data.skipped    || 0,
      levelDist:  data.levelDist  || {},
      serverDist: data.serverDist || {},
      startTime:  data.startTime  || Date.now(),
    });
    return ls;
  }
}

module.exports = LiveStats;
