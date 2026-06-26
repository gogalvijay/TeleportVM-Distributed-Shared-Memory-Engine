# TeleportVM — Userspace Distributed Shared Memory Engine

A high-performance **Distributed Shared Memory (DSM)** system written in pure C that transparently unifies the virtual address spaces of multiple networked Linux machines. Application code running on any node can read and write shared data through ordinary pointer dereferences — no sockets, no serialization, no special API. The networking, page fetching, and cache coherence happen entirely underneath, invisible to the application.

---

## What It Does

Normally, each machine has its own private RAM. TeleportVM creates the illusion that all machines share one large block of memory. When Node A writes a value to address `0x1000`, Node B can read that same address and see the updated value — across a real TCP network — as if they were sharing local memory.

The core trick is Linux's `userfaultfd` API. Instead of crashing when a thread accesses a page that isn't locally present, the kernel suspends that thread and notifies a userspace handler. TeleportVM's handler fetches the missing page from whichever node currently owns it, injects it via `UFFDIO_COPY`, and the suspended thread resumes — all transparently.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Virtual Shared Address Space (16 KB)            │
│         Page 0      Page 1      Page 2      Page 3          │
└──────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────────┐          ┌──────────────────────┐
│  Node 1 (Coord)     │          │  Node 2 (Worker)     │
│                     │          │                      │
│  mmap region        │          │  mmap region         │
│  Global Page Dir    │◄──TCP───►│  Fault Handler       │
│  Coord Sequencer    │          │  Network Engine      │
│  Fault Handler      │          │  Eviction Ring       │
│  Network Engine     │          │  Page Queue          │
└─────────────────────┘          └──────────────────────┘
```

Every node runs the **same binary**. The coordinator is distinguished only by being started without a `--coordinator-host` argument. All coherence authority lives in the coordinator's Global Page Directory.

---

## Core Components

### Global Page Directory (GPD) — `gpd.c`
The coordinator's authoritative record of every page's state across the entire cluster. Each entry tracks:
- **MSI state** — `MODIFIED`, `SHARED`, or `INVALID`
- **Owner node** — which node holds the exclusive write copy
- **Shared bitmap** — 64-bit bitmask of nodes holding read-only copies
- **Per-page mutex** — ensures transitions are atomic

### MSI Cache Coherence Protocol
Borrowed from CPU cache coherence hardware, applied over a network:

| State | Meaning | Who can have it |
|---|---|---|
| `MODIFIED` | One node has the only valid, writable copy | Exactly one node |
| `SHARED` | Multiple nodes have read-only copies | Any number of nodes |
| `INVALID` | No local copy — must fetch before access | Any node |

**Read fault flow:** `INVALID → SHARED` — coordinator fetches data from the current owner (if any) and grants a read copy. All existing copies remain valid.

**Write fault flow:** `* → MODIFIED` — coordinator sends `MSG_INVALIDATE` to every node currently holding a copy, waits for all acknowledgments, then grants exclusive write access. Every other copy is dropped.

### Fault Handler — `fault_handler.c`
A dedicated thread that sits in an `epoll` loop watching the `userfaultfd` file descriptor. When any thread in the process accesses a page that is missing or write-protected, the kernel suspends that thread and wakes this handler. The handler reads the `uffd_msg`, extracts the faulting page index and whether it was a read or write, marks the page `PAGE_PENDING`, and pushes the request into the shared queue. The faulting thread stays suspended until the page is resolved.

### Network Engine — `network_engine.c`
A single-threaded, non-blocking event loop built on `epoll`. It watches three categories of events simultaneously:
- **Listening socket** — accepts new node connections
- **eventfd from the queue** — wakes up when the fault handler pushes a new request
- **All peer TCP sockets** — receives incoming DSM packets

Outgoing requests go to the coordinator. Incoming packets are dispatched by type: `MSG_REQUEST_PAGE_READ`, `MSG_REQUEST_PAGE_WRITE`, `MSG_SEND_PAGE`, `MSG_INVALIDATE`, `MSG_INVALIDATE_ACK`, `MSG_NODE_JOIN`.

### Wire Protocol — `network.h`
All communication uses a single fixed-size binary packet structure:

```c
struct __attribute__((packed)) dsm_packet {
    uint8_t  type;            // message type
    uint64_t page_index;      // which page
    uint32_t sender_node_id;  // who sent it
    uint8_t  payload[4096];   // raw page data (only for MSG_SEND_PAGE)
};
```

Fixed-size packets eliminate framing complexity. The network engine accumulates bytes into a buffer and only processes a packet once all `sizeof(dsm_packet)` bytes have arrived — correctly handling TCP's stream-oriented delivery.

### Eviction Ring — `eviction.c`
Each node has a fixed local memory budget. When memory pressure builds, the background eviction thread uses the **Clock (Second-Chance) algorithm** to find a victim page:

1. Sweep through pages in circular order
2. If `access_bit == 1`: clear it (second chance) and skip
3. If `access_bit == 0`: this page is the victim
4. If victim is `MODIFIED`: write 4 KB back to coordinator (`MSG_SEND_PAGE`), wait for ack
5. Call `madvise(MADV_DONTNEED)` to release physical RAM
6. Mark page `INVALID` in GPD

The access bit is set via `dsm_mark_accessed()` every time a page is successfully resolved.

### Page Request Queue — `queue.c`
A thread-safe ring buffer connecting the fault handler thread to the network engine thread. Uses a Linux `eventfd` as the notification mechanism — the fault handler writes `1` to the eventfd after pushing a request, which wakes the network engine's `epoll_wait` without requiring a separate condition variable or polling loop.

---

## Building

**Requirements:**
- Linux kernel 4.11+ (userfaultfd support)
- `gcc` with C11 support
- `pthread` library
- Unprivileged userfaultfd enabled:
  ```bash
  cat /proc/sys/vm/unprivileged_userfaultfd   # should be 1
  # if 0, either run as root or:
  sudo sysctl -w vm.unprivileged_userfaultfd=1
  ```

**Build:**
```bash
make
```

This produces the `teleport_dsm` binary. All nodes run this same binary.

---

## Running

### Two nodes on the same machine (localhost)

**Terminal 1 — Node 1 (coordinator):**
```bash
./teleport_dsm 1 3490
```

**Terminal 2 — Node 2 (worker):**
```bash
./teleport_dsm 2 3491 127.0.0.1 3490
```

### Two nodes on separate machines

**Machine A (coordinator):**
```bash
./teleport_dsm 1 3490
```

**Machine B (worker):**
```bash
./teleport_dsm 2 3491 <Machine-A-IP> 3490
```

**Arguments:**
```
./teleport_dsm <node_id> <listen_port> [coordinator_host coordinator_port] [--coherence-test]

  node_id         Integer 1–63. Node 0 is reserved (means "unowned" in the GPD bitmap).
  listen_port     TCP port this node listens on. Each node needs its own if on one machine.
  coordinator_host/port  Omit for the coordinator node. Provide for all worker nodes.
  --coherence-test  Skip the benchmark; read WRITE/READ/SLEEP/DONE commands from stdin instead.
```

---

## Modes

### Benchmark Mode (default)
Runs a parallel 64×64 matrix transformation across 4 threads, measuring:
- Total execution time
- Average page fault resolution latency
- Validates all matrix values for correctness at the end

Output:
```
METRIC_TOTAL_DURATION_NS:142857142
METRIC_AVG_FAULT_LATENCY_NS:8500000
VALIDATION_STATUS:SUCCESS
```

### Coherence Test Mode (`--coherence-test`)
The node reads commands from stdin and reports results on stdout. Designed to be driven by `coherence_test_driver.py`.

Supported commands:
```
WRITE <page_index> <value>    Write a uint32_t to the start of the given page
READ  <page_index>            Read and print the uint32_t at the start of the given page
SLEEP <milliseconds>          Sleep for the given duration
DONE                          Shut down cleanly
```

### Running the Coherence Test
```bash
python3 coherence_test_driver.py ./teleport_dsm
```

This script:
1. Starts Node 1 and Node 2 with `--coherence-test`
2. Forces Node 2 to write page 0 = `111`
3. Verifies Node 1 reads `111` (cross-node fetch)
4. Forces Node 1 to write page 0 = `222` (invalidates Node 2)
5. Verifies Node 2 reads `222` and not stale `111`
6. Prints `PASS` or `FAIL` with the exact step that broke

This test proves real MSI coherence — not two independent processes that happen to both print SUCCESS, but actual data consistency verified at every step.

Expected output on success:
```
=========================================
PASS: cross-node MSI coherence verified.
Data round-tripped Node2 -> Node1 -> Node2
with no staleness at either step.
=========================================
```

---

## Design Decisions

**Why userfaultfd instead of SIGSEGV?**
Signal handlers can only call async-signal-safe functions — no mutexes, no malloc, no socket I/O. `userfaultfd` delivers fault notifications as readable events on a file descriptor, so the handler thread can do anything a normal thread can do.

**Why a fixed-size wire packet?**
Variable-length packets require a length header and two-phase reads. Fixed-size packets let the network engine accumulate bytes into a statically allocated buffer and process exactly one packet per `sizeof(dsm_packet)` bytes received — simpler, no dynamic allocation on the hot path.

**Why a clock algorithm for eviction?**
True LRU requires updating a timestamp or moving an entry in a list on every access — expensive under load. The clock algorithm approximates LRU with a single bit per page, set on access and cleared on eviction sweep. One bit updated atomically per access is nearly free.

**Why one coordinator?**
Distributed consensus (Raft, Paxos) is significantly more complex to implement correctly and adds latency. A single coordinator gives strong serialization guarantees — only one write transaction per page can be in flight at a time — at the cost of a single point of failure. Acceptable for a learning/research system, not for production.

**Why node IDs limited to 1–63?**
The shared nodes bitmap in `gpd_entry_t` is a `uint64_t`. Each bit represents one node. Bit 0 is reserved to mean "unowned." This caps the cluster at 63 nodes.

---

## File Structure

```
teleport_dsm/
├── main.c                    Entry point — wires all components together
├── dsm_core.c / .h           userfaultfd setup, page table, shared globals
├── gpd.c / .h                Global Page Directory — MSI state per page
├── fault_handler.c / .h      userfaultfd listener thread
├── network_engine.c / .h     epoll-driven TCP event loop, packet dispatch
├── coordinator_sequencer.c   Coordinator transaction serializer (see known issues)
├── coordinator_sequencer.h
├── eviction.c                Clock algorithm page eviction
├── queue.c                   Thread-safe ring buffer with eventfd notification
├── benchmark.c               Parallel matrix benchmark workload
├── coherence_test.c          stdin-driven coherence test command loop
├── coherence_test_driver.py  Python harness for automated coherence testing
└── schedule.txt              15-day implementation plan
```

---

## Known Limitations and Known Issues

This project was built as a self-directed learning exercise to understand OS internals,
cache coherence protocols, and distributed systems in one integrated system rather than
in isolation. It is not production-ready.

The current implementation has a hardcoded 16 KB shared region across 4 pages (demo
scale), a single coordinator with no fault tolerance, and no clean shutdown path. There
are also a few known race conditions in edge cases — specifically around concurrent faults
on the same page and the eviction thread interacting with in-flight page requests — that
are identified but not yet resolved. Some minor inconsistencies exist between parallel
implementations of the coordinator logic across files. These are documented and understood,
and represent clear next steps for improving the system toward production readiness.

---

## What This Project Demonstrates

Despite the limitations above, this project correctly implements and proves the following:

- Linux `userfaultfd` page fault interception in userspace
- MSI cache coherence protocol with correct state transitions under concurrent access
- Non-blocking, event-driven networking with `epoll` handling multiple connections on a single thread
- Custom binary wire protocol with correct stream-oriented TCP framing
- Clock algorithm page eviction with modified-page writeback
- Cross-node coherence verified under forced write-write-read conflict with zero staleness

The coherence test driver provides automated, repeatable proof that the system maintains memory consistency under real cross-node contention — not just a benchmark that passes in isolation.

---

## References

- [Linux userfaultfd documentation](https://www.kernel.org/doc/html/latest/admin-guide/mm/userfaultfd.html)
- [userfaultfd kernel selftest](https://github.com/torvalds/linux/blob/master/tools/testing/selftests/mm/userfaultfd.c)
- Hennessy & Patterson — *Computer Architecture: A Quantitative Approach* (MSI coherence protocol)
- Silberschatz, Galvin & Gagne — *Operating System Concepts* (clock page replacement algorithm)
- `man 2 userfaultfd`, `man 2 madvise`, `man 7 epoll`
