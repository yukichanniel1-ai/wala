# Node.js Rewrite Todo

## Phase 1: Core Modules
- [ ] src/crypto.js — AES-ECB encode, MD5, SHA-256, hash_password
- [ ] src/config.js — load/save config, constants, owner/coowner management
- [ ] src/geo-rotator.js — proxy rotation with normalize, load files, get/rotate/remove
- [ ] src/cookie-manager.js — banned cookies, auto-trim, get_valid_cookies, save_cookie
- [ ] src/datadome-manager.js — set/get/extract/clear datadome, handle_403

## Phase 2: Stats & Garena API
- [ ] src/live-stats.js — real-time statistics with progress bars, fancy telegram display
- [ ] src/garena.js — prelogin, login, get_datadome_cookie, applyck, CODM functions, processaccount, save accounts

## Phase 3: Telegram & Bot Logic
- [ ] src/telegram-api.js — _tg_api, _tg_send, _tg_send_buttons, answer_callback, delete/edit messages
- [ ] src/key-system.js — load/save keys, generate key, parse duration, redeem
- [ ] src/session.js — user session state management, save/load profiles, bot state
- [ ] src/combo-parser.js — _is_garena_credential, _parse_combo_lines
- [ ] src/proxy-upload.js — normalize proxy lines, preprocess text, save proxies

## Phase 4: Main Entry & Integration
- [ ] src/healthcheck.js — HTTP server on PORT for Railway healthcheck
- [ ] src/index.js — Main entry: signal handling, polling, watchdog, heartbeat, crash guard
- [ ] Update Procfile to `worker: node src/index.js`
- [ ] Test bot starts and responds to /start
- [ ] Push to GitHub on feature/nodejs-rewrite branch
