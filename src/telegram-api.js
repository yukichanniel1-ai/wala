/**
 * telegram-api.js — Telegram Bot API helpers
 * Ported from Python main.py (lines 2706-3065)
 */
const axios     = require('axios');
const fs        = require('fs');
const path      = require('path');
const FormData  = require('form-data');
const archiver  = require('archiver');

// ── Core API call with retry on 429 ──────────────────────────────────
async function tgApi(token, method, payload = {}, retries = 3) {
  const url = `https://api.telegram.org/bot${token}/${method}`;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await axios.post(url, payload, {
        timeout: 30000,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      });
      return response.data;
    } catch (e) {
      if (e.response?.status === 429) {
        const retryAfter = parseInt(e.response.headers['retry-after']) || 10;
        console.warn(`[TG] Rate limited on ${method} — sleeping ${retryAfter}s`);
        await new Promise(r => setTimeout(r, retryAfter * 1000));
        continue;
      }
      if (attempt < retries - 1) {
        await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
        continue;
      }
      console.error(`[TG] Error calling ${method}:`, e.message);
      return null;
    }
  }
  return null;
}

// ── Send message (auto-split >4096 chars) ────────────────────────────
async function tgSend(token, chatId, text, extra = {}) {
  if (!text) return null;

  // Auto-split long messages
  const chunks = [];
  let remaining = text;
  while (remaining.length > 4096) {
    let splitAt = remaining.lastIndexOf('\n', 4096);
    if (splitAt < 2048) splitAt = remaining.lastIndexOf(' ', 4096);
    if (splitAt < 2048) splitAt = 4096;
    chunks.push(remaining.slice(0, splitAt));
    remaining = remaining.slice(splitAt).trimStart();
  }
  if (remaining) chunks.push(remaining);

  let lastResult = null;
  for (const chunk of chunks) {
    const payload = {
      chat_id: chatId,
      text: chunk,
      parse_mode: 'HTML',
      ...extra,
    };
    lastResult = await tgApi(token, 'sendMessage', payload);
  }
  return lastResult;
}

// ── Send message with inline keyboard ────────────────────────────────
async function tgSendButtons(token, chatId, text, keyboard) {
  const payload = {
    chat_id: chatId,
    text: text,
    parse_mode: 'HTML',
    reply_markup: JSON.stringify({ inline_keyboard: keyboard }),
  };
  return await tgApi(token, 'sendMessage', payload);
}

// ── Answer callback query ────────────────────────────────────────────
async function tgAnswerCallback(token, callbackQueryId, text = '') {
  return await tgApi(token, 'answerCallbackQuery', {
    callback_query_id: callbackQueryId,
    text: text,
  });
}

// ── Edit message ─────────────────────────────────────────────────────
async function tgEditMessage(token, chatId, messageId, text, keyboard = null) {
  const payload = {
    chat_id: chatId,
    message_id: messageId,
    text: text,
    parse_mode: 'HTML',
  };
  if (keyboard !== null) {
    payload.reply_markup = JSON.stringify({ inline_keyboard: keyboard });
  }
  return await tgApi(token, 'editMessageText', payload);
}

// ── Delete message ───────────────────────────────────────────────────
async function tgDeleteMessage(token, chatId, messageId) {
  return await tgApi(token, 'deleteMessage', {
    chat_id: chatId,
    message_id: messageId,
  });
}

// ── Delete messages bulk ─────────────────────────────────────────────
async function tgDeleteMessagesBulk(token, chatId, messageIds) {
  if (!messageIds || !messageIds.length) return;
  // Telegram doesn't have a bulk delete for non-group chats
  // Delete one by one
  for (const mid of messageIds) {
    try {
      await tgDeleteMessage(token, chatId, mid);
    } catch {}
    await new Promise(r => setTimeout(r, 50));
  }
}

// ── Send document ────────────────────────────────────────────────────
async function tgSendDocument(token, chatId, filePath, caption = '') {
  if (!fs.existsSync(filePath)) return null;

  const form = new FormData();
  form.append('chat_id', String(chatId));
  form.append('document', fs.createReadStream(filePath));
  if (caption) form.append('caption', caption);
  form.append('parse_mode', 'HTML');

  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${token}/sendDocument`,
      form,
      { headers: form.getHeaders(), timeout: 120000 }
    );
    return response.data;
  } catch (e) {
    console.error(`[TG] Error sending document:`, e.message);
    return null;
  }
}

// ── Get file URL ─────────────────────────────────────────────────────
async function tgGetFileUrl(token, fileId) {
  const result = await tgApi(token, 'getFile', { file_id: fileId });
  if (result?.ok && result.result?.file_path) {
    return `https://api.telegram.org/file/bot${token}/${result.result.file_path}`;
  }
  return null;
}

// ── Download file from Telegram ──────────────────────────────────────
async function tgDownloadFile(token, fileId, destPath) {
  const url = await tgGetFileUrl(token, fileId);
  if (!url) return null;

  try {
    const response = await axios.get(url, { responseType: 'stream', timeout: 60000 });
    const writer = fs.createWriteStream(destPath);
    response.data.pipe(writer);
    return new Promise((resolve, reject) => {
      writer.on('finish', () => resolve(destPath));
      writer.on('error', reject);
    });
  } catch (e) {
    console.error(`[TG] Error downloading file:`, e.message);
    return null;
  }
}

// ── Set bot commands ─────────────────────────────────────────────────
async function tgSetCommands(token) {
  const commands = [
    { command: 'start', description: 'Start the bot' },
    { command: 'help', description: 'Show help menu' },
    { command: 'stop', description: 'Stop running checker' },
    { command: 'reset', description: 'Reset your settings' },
    { command: 'redeem', description: 'Redeem a key' },
    { command: 'generate_key', description: 'Generate a key (owner)' },
    { command: 'statuskey', description: 'Check key status (owner)' },
    { command: 'deletekey', description: 'Delete keys (owner)' },
    { command: 'serverstatus', description: 'Server status (owner)' },
    { command: 'upload_proxy', description: 'Upload proxies (owner)' },
    { command: 'proxy_done', description: 'Finish proxy upload (owner)' },
    { command: 'proxystatus', description: 'Proxy status (owner)' },
    { command: 'add_coowner', description: 'Add co-owner (owner)' },
    { command: 'remove_coowner', description: 'Remove co-owner (owner)' },
    { command: 'stopall', description: 'Stop all checkers (owner)' },
    { command: 'resetconfig', description: 'Reset config (owner)' },
  ];
  return await tgApi(token, 'setMyCommands', { commands });
}

// ── Send results as zip ──────────────────────────────────────────────
async function sendResultsZip(token, chatId, resultFolder) {
  if (!fs.existsSync(resultFolder)) return null;

  const zipPath = path.join(resultFolder, 'results.zip');

  return new Promise((resolve) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', async () => {
      const result = await tgSendDocument(token, chatId, zipPath, '📊 <b>Checker Results</b>');
      // Clean up zip
      try { fs.unlinkSync(zipPath); } catch {}
      resolve(result);
    });

    archive.on('error', (err) => {
      console.error('[ZIP] Error:', err.message);
      resolve(null);
    });

    archive.pipe(output);
    archive.directory(resultFolder, false, (entry) => {
      // Skip the zip file itself
      if (entry.name.endsWith('.zip')) return false;
      return entry;
    });
    archive.finalize();
  });
}

module.exports = {
  tgApi,
  tgSend,
  tgSendButtons,
  tgAnswerCallback,
  tgEditMessage,
  tgDeleteMessage,
  tgDeleteMessagesBulk,
  tgSendDocument,
  tgGetFileUrl,
  tgDownloadFile,
  tgSetCommands,
  sendResultsZip,
};
