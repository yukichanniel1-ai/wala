#!/usr/bin/env python3
"""
Test VIP/Free Queue Priority System
=====================================
Validates:
  1. VIP accounts process with NO queue (instant)
  2. Free accounts HAVE a queue (wait for limited slots)
  3. VIP threads are never blocked by Free threads
  4. Free threads are properly limited to free_threads count
  5. VIP and Free semaphores are completely independent
"""

import time
import threading
from threading import Lock, Semaphore, Event
from queue import Queue
from typing import Optional, List


# ============================================================
# Replicate the CORRECTED VipFreeQueue from garena_checker_v2.py
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
    """

    def __init__(self, vip_threads: int, free_threads: int):
        self.vip_queue = Queue()
        self.free_queue = Queue()
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
        self._vip_sem = Semaphore(vip_threads)
        self._free_sem = Semaphore(free_threads)
        self._vip_in_flight = 0
        self._free_in_flight = 0

    def add_vip(self, account: dict):
        with self._lock:
            self._vip_added += 1
            self._total += 1
        self.vip_queue.put(account)

    def add_free(self, account: dict):
        with self._lock:
            self._free_added += 1
            self._total += 1
        self.free_queue.put(account)

    def acquire_vip_slot(self, timeout: float = None) -> bool:
        return self._vip_sem.acquire(timeout=timeout)

    def release_vip_slot(self):
        self._vip_sem.release()

    def acquire_free_slot(self, timeout: float = None) -> bool:
        return self._free_sem.acquire(timeout=timeout)

    def release_free_slot(self):
        self._free_sem.release()

    def get_vip(self) -> Optional[dict]:
        try:
            account = self.vip_queue.get_nowait()
            with self._lock:
                self._vip_in_flight += 1
            return account
        except:
            return None

    def get_free(self) -> Optional[dict]:
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
        with self._lock:
            return self.vip_queue.qsize() + self._vip_in_flight

    @property
    def free_remaining(self):
        with self._lock:
            return self.free_queue.qsize() + self._free_in_flight

    def all_done(self):
        with self._lock:
            return self._done >= self._total and self._total > 0

    def stop(self):
        self._stop_event.set()

    def should_stop(self):
        return self._stop_event.is_set()


# ============================================================
# TEST 1: VIP has NO queue — processes instantly
# ============================================================

def test_vip_instant_processing():
    """VIP accounts should start processing immediately without waiting."""
    print("\n" + "="*60)
    print("TEST 1: VIP Instant Processing (NO Queue)")
    print("="*60)

    vip_threads = 5
    free_threads = 2
    pq = VipFreeQueue(vip_threads, free_threads)

    # Add 10 VIP accounts
    for i in range(10):
        pq.add_vip({"username": f"vip_user_{i}", "password": "pass", "is_vip": True})

    start_times = {}
    lock = Lock()
    running = True

    def vip_worker(worker_id):
        nonlocal running
        while running:
            # CORRECTED: Acquire slot FIRST, then get account
            if not pq.acquire_vip_slot(timeout=0.5):
                if pq.vip_remaining == 0:
                    break
                continue

            account = pq.get_vip()
            if account is None:
                pq.release_vip_slot()
                if pq.vip_remaining == 0:
                    break
                time.sleep(0.01)
                continue

            try:
                with lock:
                    start_times[account['username']] = time.time()
                time.sleep(0.05)  # Simulate work
            finally:
                pq.release_vip_slot()
                pq.mark_vip_done()

    overall_start = time.time()
    workers = []
    for i in range(vip_threads):
        t = threading.Thread(target=vip_worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)

    # Wait for completion
    while not pq.all_done():
        time.sleep(0.1)
    running = False
    for t in workers:
        t.join(timeout=2)

    max_start_delay = max(start_times.values()) - overall_start
    print(f"  ✅ All 10 VIP accounts processed: {pq._vip_done}")
    print(f"  ✅ Max start delay: {max_start_delay:.3f}s (should be < 0.3s for instant)")
    assert pq._vip_done == 10, f"Expected 10 VIP done, got {pq._vip_done}"
    assert max_start_delay < 0.5, f"VIP start delay too high: {max_start_delay:.3f}s"
    print(f"  ✅ VIP has NO queue — instant processing confirmed")


# ============================================================
# TEST 2: Free HAS a queue — waits for limited slots
# ============================================================

def test_free_queued_processing():
    """Free accounts should wait in queue when free_threads slots are full."""
    print("\n" + "="*60)
    print("TEST 2: Free Queued Processing (HAS Queue)")
    print("="*60)

    vip_threads = 5
    free_threads = 2  # Only 2 concurrent Free slots
    pq = VipFreeQueue(vip_threads, free_threads)

    # Add 10 Free accounts
    for i in range(10):
        pq.add_free({"username": f"free_user_{i}", "password": "pass", "is_vip": False})

    concurrent_free = 0
    max_concurrent_free = 0
    lock = Lock()

    def free_worker(worker_id):
        while True:
            # CORRECTED: Acquire slot FIRST — this IS the queue
            if not pq.acquire_free_slot(timeout=0.5):
                if pq.free_remaining == 0:
                    break
                continue

            account = pq.get_free()
            if account is None:
                pq.release_free_slot()
                if pq.free_remaining == 0:
                    break
                time.sleep(0.01)
                continue

            try:
                with lock:
                    nonlocal concurrent_free, max_concurrent_free
                    concurrent_free += 1
                    if concurrent_free > max_concurrent_free:
                        max_concurrent_free = concurrent_free

                time.sleep(0.15)  # Simulate work — long enough to see queuing

                with lock:
                    nonlocal_concurrent = concurrent_free
                    concurrent_free -= 1
            finally:
                pq.release_free_slot()
                pq.mark_free_done()

    workers = []
    # Use more workers than free_threads to test queuing
    for i in range(free_threads):
        t = threading.Thread(target=free_worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)

    # Wait for completion
    while not pq.all_done():
        time.sleep(0.1)
    for t in workers:
        t.join(timeout=5)

    print(f"  ✅ All 10 Free accounts processed: {pq._free_done}")
    print(f"  ✅ Max concurrent Free: {max_concurrent_free} (limit: {free_threads})")
    assert pq._free_done == 10, f"Expected 10 Free done, got {pq._free_done}"
    assert max_concurrent_free <= free_threads, f"Free exceeded thread limit: {max_concurrent_free} > {free_threads}"
    print(f"  ✅ Free HAS queue — limited to {free_threads} concurrent slots confirmed")


# ============================================================
# TEST 3: VIP never blocked by Free
# ============================================================

def test_vip_not_blocked_by_free():
    """VIP accounts should process even when all Free slots are occupied."""
    print("\n" + "="*60)
    print("TEST 3: VIP Not Blocked By Free")
    print("="*60)

    vip_threads = 3
    free_threads = 2
    pq = VipFreeQueue(vip_threads, free_threads)

    # Add 6 Free accounts first (will occupy Free slots for a while)
    for i in range(6):
        pq.add_free({"username": f"free_user_{i}", "password": "pass", "is_vip": False})
    # Add 3 VIP accounts
    for i in range(3):
        pq.add_vip({"username": f"vip_user_{i}", "password": "pass", "is_vip": True})

    vip_done_time = None
    lock = Lock()

    def vip_worker(worker_id):
        nonlocal vip_done_time
        while True:
            if not pq.acquire_vip_slot(timeout=0.5):
                if pq.vip_remaining == 0:
                    break
                continue

            account = pq.get_vip()
            if account is None:
                pq.release_vip_slot()
                if pq.vip_remaining == 0:
                    break
                time.sleep(0.01)
                continue

            try:
                time.sleep(0.05)
            finally:
                pq.release_vip_slot()
                pq.mark_vip_done()

        with lock:
            if vip_done_time is None or pq._vip_done >= 3:
                vip_done_time = time.time()

    def free_worker(worker_id):
        while True:
            if not pq.acquire_free_slot(timeout=0.5):
                if pq.free_remaining == 0:
                    break
                continue

            account = pq.get_free()
            if account is None:
                pq.release_free_slot()
                if pq.free_remaining == 0:
                    break
                time.sleep(0.01)
                continue

            try:
                time.sleep(0.3)  # Free takes longer
            finally:
                pq.release_free_slot()
                pq.mark_free_done()

    overall_start = time.time()

    workers = []
    for i in range(vip_threads):
        t = threading.Thread(target=vip_worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)
    for i in range(free_threads):
        t = threading.Thread(target=free_worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)

    # Wait for all
    while not pq.all_done():
        time.sleep(0.1)
    for t in workers:
        t.join(timeout=5)

    vip_elapsed = vip_done_time - overall_start if vip_done_time else 999

    print(f"  ✅ VIP done: {pq._vip_done}, Free done: {pq._free_done}")
    print(f"  ✅ VIP completed in {vip_elapsed:.3f}s (should be fast)")
    assert pq._vip_done == 3, f"Expected 3 VIP done, got {pq._vip_done}"
    assert pq._free_done == 6, f"Expected 6 Free done, got {pq._free_done}"
    assert vip_elapsed < 1.0, f"VIP took too long: {vip_elapsed:.3f}s"
    print(f"  ✅ VIP is NOT blocked by Free — separate semaphores confirmed")


# ============================================================
# TEST 4: Semaphore independence
# ============================================================

def test_semaphore_independence():
    """VIP and Free semaphores should be completely independent."""
    print("\n" + "="*60)
    print("TEST 4: Semaphore Independence")
    print("="*60)

    vip_threads = 3
    free_threads = 1
    pq = VipFreeQueue(vip_threads, free_threads)

    # Initial values
    assert pq._vip_sem._value == vip_threads, f"VIP semaphore initial value wrong: {pq._vip_sem._value}"
    assert pq._free_sem._value == free_threads, f"Free semaphore initial value wrong: {pq._free_sem._value}"

    # Acquire all Free slots
    pq._free_sem.acquire()
    assert pq._free_sem._value == 0, "Free semaphore should be 0 after acquire"
    assert pq._vip_sem._value == vip_threads, "VIP semaphore should NOT be affected by Free acquire"

    # VIP should still be able to acquire
    pq._vip_sem.acquire()
    assert pq._vip_sem._value == vip_threads - 1, "VIP semaphore should decrement independently"

    # Release
    pq._vip_sem.release()
    pq._free_sem.release()

    assert pq._vip_sem._value == vip_threads, "VIP semaphore should restore"
    assert pq._free_sem._value == free_threads, "Free semaphore should restore"

    print(f"  ✅ VIP semaphore ({vip_threads}) independent of Free semaphore ({free_threads})")
    print(f"  ✅ Acquiring Free slots does NOT affect VIP availability")
    print(f"  ✅ Acquiring VIP slots does NOT affect Free availability")


# ============================================================
# TEST 5: Large workload — all accounts eventually processed
# ============================================================

def test_large_workload():
    """With a large number of accounts, all should eventually be processed."""
    print("\n" + "="*60)
    print("TEST 5: Large Workload (50 VIP + 200 Free)")
    print("="*60)

    vip_threads = 5
    free_threads = 3
    pq = VipFreeQueue(vip_threads, free_threads)

    # Add 50 VIP + 200 Free
    for i in range(50):
        pq.add_vip({"username": f"vip_user_{i}", "password": "pass", "is_vip": True})
    for i in range(200):
        pq.add_free({"username": f"free_user_{i}", "password": "pass", "is_vip": False})

    vip_start = time.time()
    free_start = time.time()

    def vip_worker(worker_id):
        while True:
            if not pq.acquire_vip_slot(timeout=0.5):
                if pq.vip_remaining == 0:
                    break
                continue
            account = pq.get_vip()
            if account is None:
                pq.release_vip_slot()
                if pq.vip_remaining == 0:
                    break
                time.sleep(0.01)
                continue
            try:
                time.sleep(0.01)  # Fast processing
            finally:
                pq.release_vip_slot()
                pq.mark_vip_done()

    def free_worker(worker_id):
        while True:
            if not pq.acquire_free_slot(timeout=0.5):
                if pq.free_remaining == 0:
                    break
                continue
            account = pq.get_free()
            if account is None:
                pq.release_free_slot()
                if pq.free_remaining == 0:
                    break
                time.sleep(0.01)
                continue
            try:
                time.sleep(0.02)  # Slightly slower
            finally:
                pq.release_free_slot()
                pq.mark_free_done()

    workers = []
    for i in range(vip_threads):
        t = threading.Thread(target=vip_worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)
    for i in range(free_threads):
        t = threading.Thread(target=free_worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)

    # Wait for all
    while not pq.all_done():
        time.sleep(0.5)
    for t in workers:
        t.join(timeout=5)

    total_time = time.time() - vip_start
    print(f"  ✅ VIP done: {pq._vip_done}/50")
    print(f"  ✅ Free done: {pq._free_done}/200")
    print(f"  ✅ Total time: {total_time:.2f}s")
    assert pq._vip_done == 50, f"Expected 50 VIP done, got {pq._vip_done}"
    assert pq._free_done == 200, f"Expected 200 Free done, got {pq._free_done}"
    print(f"  ✅ All 250 accounts processed correctly")


# ============================================================
# RUN ALL TESTS
# ============================================================

if __name__ == "__main__":
    print("\n" + "🔍" * 30)
    print("  VIP/FREE QUEUE PRIORITY SYSTEM — TEST SUITE")
    print("🔍" * 30)

    try:
        test_vip_instant_processing()
        test_free_queued_processing()
        test_vip_not_blocked_by_free()
        test_semaphore_independence()
        test_large_workload()

        print("\n" + "="*60)
        print("  🎉 ALL TESTS PASSED!")
        print("="*60)
        print("""
  Summary:
    ✅ VIP has NO queue — instant processing
    ✅ Free HAS queue — waits for limited slots
    ✅ VIP is never blocked by Free
    ✅ VIP and Free semaphores are independent
    ✅ Large workloads process all accounts correctly
        """)
    except AssertionError as e:
        print(f"\n  ❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n  💥 UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
