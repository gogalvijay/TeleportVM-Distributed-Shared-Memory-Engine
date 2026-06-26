#!/usr/bin/env python3
"""
coherence_test_driver.py

Launches two teleport_dsm nodes in --coherence-test mode, forces a
real write/write/read conflict on the SAME page index from BOTH
nodes, and checks that the data actually converges to a single
consistent value instead of just "didn't crash".

This is the test to run to prove cross-node MSI coherence is real,
not two independent local benchmarks that happen to both print
SUCCESS.

Usage:
    python3 coherence_test_driver.py [path_to_teleport_dsm]

Run from the directory containing the teleport_dsm binary, or pass
its path explicitly. Requires Linux with unprivileged userfaultfd
enabled (check: cat /proc/sys/vm/unprivileged_userfaultfd -> should
be 1, or run as root / with CAP_SYS_PTRACE).
"""

import subprocess
import sys
import time
import re
import os
import threading

BIN = sys.argv[1] if len(sys.argv) > 1 else "./teleport_dsm"
COORD_PORT = "3490"
WORKER_PORT = "3491"
PAGE = 0

RESULT_RE = re.compile(r"RESULT (\w+) page=(\d+) value=(\d+)")


def start_node(args, label):
    proc = subprocess.Popen(
        [BIN] + args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    proc.label = label
    proc.lines = []
    proc.result_queue = []
    proc._lock = threading.Lock()

    def pump():
        for line in proc.stdout:
            line = line.rstrip("\n")
            with proc._lock:
                proc.lines.append(line)
            print(f"   [{label}] {line}")
            m = RESULT_RE.search(line)
            if m:
                with proc._lock:
                    proc.result_queue.append((m.group(1), int(m.group(2)), int(m.group(3))))
        with proc._lock:
            proc.lines.append(f"<<{label} stdout closed, process likely exited>>")
        print(f"   [{label}] <<stdout closed>>")

    t = threading.Thread(target=pump, daemon=True)
    t.start()
    proc._pump_thread = t
    return proc


def send(proc, line):
    if proc.poll() is not None:
        print(f"ERROR: {proc.label} already exited with code {proc.returncode} "
              f"before we could send: {line!r}")
        print(f"Full output from {proc.label}:")
        for l in proc.lines:
            print(f"   [{proc.label}] {l}")
        raise RuntimeError(f"{proc.label} is dead")
    try:
        proc.stdin.write(line + "\n")
        proc.stdin.flush()
    except BrokenPipeError:
        print(f"ERROR: {proc.label} pipe broke while sending {line!r} "
              f"(exit code: {proc.poll()})")
        print(f"Full output from {proc.label}:")
        for l in proc.lines:
            print(f"   [{proc.label}] {l}")
        raise


def wait_for_result(proc, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with proc._lock:
            if proc.result_queue:
                return proc.result_queue.pop(0)
        if proc.poll() is not None:
            print(f"ERROR: {proc.label} exited (code {proc.returncode}) "
                  f"while waiting for a result.")
            return None
        time.sleep(0.05)
    print(f"TIMEOUT waiting for result from {proc.label}")
    return None


def main():
    if not os.path.exists(BIN):
        print(f"ERROR: binary not found at {BIN}. Build it first with `make`.")
        sys.exit(1)

    print("=== Starting Node 1 (coordinator) ===")
    node1 = start_node(["1", COORD_PORT, "--coherence-test"], "Node1")
    time.sleep(0.5)

    print("=== Starting Node 2 (worker) ===")
    node2 = start_node(["2", WORKER_PORT, "127.0.0.1", COORD_PORT, "--coherence-test"], "Node2")
    time.sleep(0.7)  # let the join handshake complete

    try:
        print(f"\n=== Step 1: Node 2 writes page {PAGE} = 111 (acquires MODIFIED) ===")
        send(node2, f"WRITE {PAGE} 111")
        r1 = wait_for_result(node2)
        assert r1 == ("WRITE", PAGE, 111), f"Node2 WRITE failed: {r1}"

        time.sleep(0.3)

        print(f"\n=== Step 2: Node 1 reads page {PAGE} (must fetch from Node 2) ===")
        send(node1, f"READ {PAGE}")
        r2 = wait_for_result(node1)
        assert r2 is not None and r2[0] == "READ" and r2[2] == 111, \
            f"FAIL: Node1 read {r2}, expected 111 (stale/missing cross-node fetch)"
        print(f"   -> Node 1 correctly sees Node 2's write: {r2[2]}")

        time.sleep(0.3)

        print(f"\n=== Step 3: Node 1 writes page {PAGE} = 222 (takes MODIFIED back, invalidates Node 2) ===")
        send(node1, f"WRITE {PAGE} 222")
        r3 = wait_for_result(node1)
        assert r3 == ("WRITE", PAGE, 222), f"Node1 WRITE failed: {r3}"

        time.sleep(0.3)

        print(f"\n=== Step 4: Node 2 reads page {PAGE} (must fetch Node 1's new value, not its stale 111) ===")
        send(node2, f"READ {PAGE}")
        r4 = wait_for_result(node2)
        assert r4 is not None and r4[0] == "READ" and r4[2] == 222, \
            f"FAIL: Node2 read {r4}, expected 222 (stale data == coherence broken)"
        print(f"   -> Node 2 correctly sees Node 1's new write: {r4[2]}")

        print("\n=========================================")
        print("PASS: cross-node MSI coherence verified.")
        print("Data round-tripped Node2 -> Node1 -> Node2")
        print("with no staleness at either step.")
        print("=========================================")

    except (AssertionError, RuntimeError) as e:
        print("\n=========================================")
        print(f"FAIL: {e}")
        print("=========================================")
        sys.exit(1)

    finally:
        for n in (node1, node2):
            try:
                send(n, "DONE")
            except Exception:
                pass
        time.sleep(0.3)
        for n in (node1, node2):
            n.terminate()


if __name__ == "__main__":
    main()
