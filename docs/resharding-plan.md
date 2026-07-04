# Resharding: research & implementation plan

*Drafted 2026-07-04. Based on a code audit of `any-sync-node` + `any-sync@v0.11.20` and an external design consult. Status: proposal.*

## 1. Current state (what the code actually does)

Placement is a **pure function** of `spaceId` and the network configuration:

- `nodeconf` builds a consistent-hash ring (`go-chash`) over **tree nodes only**: `PartitionCount = 3000`, `ReplicationFactor = 3` (`any-sync/nodeconf/service.go:22`). Hash key is `ReplKey(spaceId)` — the suffix after the last `.` — so derived spaces co-locate (`nodeconf/nodeconf.go:126`).
- Everyone (nodes *and* clients) evaluates the same function locally. Config comes from the coordinator (`NetworkConfiguration(currentId)`), identified by an **opaque `ConfigurationId`** — no ordering/epoch semantics. Nodes poll every ~10 min (`nodeconf/service.go:112-150`) and atomically rebuild the chash on change; **no hook/event fires** on a config change.
- `IsResponsible` is enforced only against **client** peers (`nodespace/checks.go:14`); node→node requests are never rejected.
- Storage: one anystore/SQLite DB per space at `<anyStorePath>/<spaceId>/store.db`; a per-node index DB holds `{spaceId → heads hash, SpaceStatus}` (`nodestorage/indexstorage.go`); `nodehead` maintains per-partition `ldiff` trees for anti-entropy.
- **Archive tier**: spaces not accessed for `ArchiveAfterDays` (default **7**) are backed up, gzipped, uploaded to S3 (`<keyPrefix>/<spaceId>`, per-node bucket/prefix config), marked `SpaceStatusArchived` (index keeps last heads hash + sizes), and the **local DB dir is deleted** (`archive/archive.go:94`). Restore is lazy: any open of an archived space — including `DumpStorage`, the coldsync server path — downloads from S3, sets status Ok, and **deletes the S3 object** (`storageservice.go:278`, `archive/archive.go:190`). Archived spaces **remain in the partition ldiff** with their archive-time heads (`indexstorage.go:125` — `ReadHashes` includes Ok + Archived). Each replica archives independently on its own last-access clock, so a dormant space typically exists as up to 3 separate gz objects. Given mostly-dormant user data, **the majority of spaces on a production node are expected to be Archived**.
- Data movement is a **repair loop only** (`nodesync/`): on start + every N hours, for each of the 3000 partitions where the node is a member, run ldiff against the other members; missing spaces → `coldsync` (full SQLite backup stream, refuses if space exists locally), diverged spaces → `hotsync` (normal tree sync, ~300 concurrent).

**The gaps that make topology changes unsafe today:**

1. Nothing reacts to a config change — sync is timer-driven, there is no old-vs-new membership diff.
2. Nothing ever **removes or hands off** spaces a node lost responsibility for. Orphaned copies accumulate (years' worth already). The only tool is the manual per-space `debug/spacechecker` `Fix`, which parks the dir under `notresponsible/` **without verifying new owners have the data**.
3. A new owner serves (rejects nothing from nodes, and for clients returns errors) before it has synced; an old owner keeps accepting stale-client writes; RF is silently violated in both directions during the transition window.
4. `coldsync` can't overwrite/merge into an existing stale local copy and has no resume for GB-size DBs.
5. Node replacement with the same identity but an empty disk is a **read black hole**: chash considers it fully operational immediately.
6. Anti-entropy can **resurrect deleted spaces**: a peer that lacks a space (because it processed the deletion log) will happily receive it again via coldsync from a node that still holds an orphaned copy.
7. **Archive amplification**: any naive migration of a mostly-archived partition becomes a restore storm — each coldsync of an archived space forces the source to S3-GET + unpack + disk-spike + S3-DELETE, stream the SQLite DB, and re-archive (S3 PUT) days later; a draining node would restore nearly its whole archived population just to push and then delete it. Node replacement also leaks: a rebuilt node's old S3 prefix keeps millions of orphaned gz objects nothing will ever clean.

## 2. Design direction

Given client-evaluated placement, weakly-synchronized config distribution, and CRDT space data, a strongly-consistent coordinator-driven migration state machine (Ceph/CockroachDB style) fights the architecture. The right shape is a **decentralized, epoch-aware handoff** (Dynamo/Cassandra bootstrap+drain style), leaning on CRDT convergence:

> Placement stays a pure function. The coordinator versions configs with a monotonic **epoch** and enforces safety invariants before publishing. Each node, on observing an epoch change, locally diffs the old and new rings and runs a per-partition **Bootstrapping** (gained) or **Draining** (lost) state machine. The deletion invariant is **transport-agnostic**: a draining node may delete a space only after its state is **durably held by ≥2 current owners** — *how* the data got there doesn't matter. Bulk data movement is the **S3 data plane** ("force-archive + rename", §2.5): archived snapshots copied server-side between node prefixes plus a pointer-handoff RPC; the CRDT tree sync converges any delta. Deletions are protected by **tombstones** with a coordinator fallback.

**Scope: resharding is a production-network feature.** Archiving is optional and most self-hosted networks run without S3 — but they don't need resharding either: with RF=3, any network of ≤3 tree nodes has every node owning every partition, so there is nothing to reshard; topology changes at that scale are handled by today's lazy `nodesync` (which the epoch/safety rails below still make safer). The migration machinery activates only when the network has a shared archive store configured (self-hosters who grow past 3 nodes can opt in with any S3-compatible store, e.g. MinIO). No node-to-node bulk-streaming transport is built for resharding — that was evaluated and dropped as serving a user that doesn't exist.

### 2.1 Protocol/config changes

- **Epoch**: add a monotonically increasing integer to `nodeconf.Configuration`; coordinator increments on every topology change. Keep `ConfigurationId` for compatibility.
- **Config history**: nodes persist the last K configs locally (tiny); coordinator additionally serves history by epoch. A node that adopts epoch N can always compute the N-1 ring (whom to pull from / push to), even after a restart mid-migration.
- **Coordinator guardrail (hard rule)**: before publishing epoch N+1, simulate the new ring and verify **replica overlap ≥1 (target 2) surviving member per partition** vs epoch N. Reject configs that replace all replicas of any partition at once. This is what makes "pull from current members" always possible and removes any need to serialize epochs — a new epoch may be published mid-migration; nodes simply retarget.
- **DiskGenID (replacement safety)**: a tree node initializing an empty storage dir generates a UUID and registers it with the coordinator. If a node restarts with a new/missing DiskGenID, it enters global Bootstrapping for **all** its partitions: refuse client reads for unsynced spaces (transient error → client retries another replica) until anti-entropy completes. Closes the black-hole-on-replacement trap.

### 2.2 Node-local state machine

On observing epoch N → N+1, diff the rings per partition:

**Draining (lost partition P) — the sender drives the migration:**
1. Adopt the new ring: client writes for P are now rejected (`IsResponsible == false`); stragglers via legacy paths are still covered by step 3.
2. Per space S, **metadata first**: ask the current owners whether they hold S's heads (their index answers for live *and* archived copies — archived heads are frozen at archive time and stay in the index/ldiff). If ≥2 owners give a *durable ACK* (§2.5), skip to step 5 with **zero data transfer** — the common case, since RF=3 means the survivors already hold dormant spaces.
3. On divergence: `hotsync`-**push** latest local state to ≥2 surviving replicas and wait for their ACKs ("peer needs nothing after my push" *is* the superset proof; no new DAG-traversal RPC). For an archived local copy this needs a restore first, but a dormant space can't have diverged — rare path.
4. Hand the space to the new owner via the S3 data plane (§2.5): if archived — `CopyObject` own gz → target prefix, then `AdoptArchive` RPC; if live — `ForceArchive` (snapshot, DB stays live) → `CopyObject` → `AdoptArchive(eager=true)`.
5. **Delete locally only after ≥2 current owners durably hold the state** (metadata ACK, push ACK, or adopted archive). Overlap guarantee means ≥1 owner already had the data; 2 ACKs keep quorum durability even if one new owner dies immediately. "Delete locally" for an archived space means the index entry **and** the own-prefix S3 object.
6. If the network moves to epoch N+2 mid-drain, retarget to the newest owners; the rule is always "≥2 from *current* owners".

**Bootstrapping (gained partition P) — mostly passive:**
- Normal case: the drainer pushes `AdoptArchive` pointers; the node verifies the object (S3 `HEAD`), registers `Archived` + heads in its index, and restores lazily on first access — or eagerly (background queue) when flagged, so hot spaces don't pay S3-GET latency on the next client request. After an eager restore, hotsync against peers converges the post-snapshot delta.
- Pull mode (drainer dead / unplanned loss): drive the same engine from the receiver — ask a surviving replica to `ForceArchive`/`CopyObject` + `AdoptArchive`. (The existing lazy `nodesync` coldsync also still converges missing spaces, as today.)
- Client cache-miss on a not-yet-adopted space → enqueue high-priority pull + return the existing transient error so the client retries a warm replica. Post-MVP: synchronous pull-on-demand for small spaces.

### 2.3 Deletion tombstones (stop resurrection)

- On processing a deletion-log record, do not just remove the DB: write a tombstone `{spaceId, status=Deleted, expiresAt = now + 30d}` in the index; GC tombstones after TTL.
- `coldsync`/handoff prep-request: receiver checks its index; if tombstoned → reply `ErrSpaceDeleted`; sender applies the tombstone locally and treats a drain handoff as **successful**.
- Fallback for expired tombstones / fresh nodes: on an incoming coldsync of an unknown *orphan-age* space, the receiver may ask the coordinator "is S in the deletion log?" before accepting. Keeps local tombstone state bounded; the coordinator's deletion log stays the global authority.

### 2.4 Legacy orphan sweeper (years of accumulated copies)

Low-priority, rate-limited background worker; for each local space with `IsResponsible == false`:

1. Deleted? (local tombstone, else coordinator deletion-log check) → yes: delete local DB, done.
2. Integrity: DB fails to open/verify → move to `<root>/quarantine/<spaceId>`, never sync corrupted data.
3. Resolve current 3 owners from the *current* ring.
4. Push & verify with the drain logic (hotsync push, need ≥2 ACKs) → delete local DB.
5. <2 ACKs (owners unreachable, disk full, timeouts) → **park**, retry next sweep cycle. Never delete unverified.

This also subsumes and retires the manual `spacechecker` flow.

**Archived orphans** (index entry + S3 object, no local DB): same flow, but the verify step is the metadata check (§2.5); on success delete both the index entry and the S3 object; if owners lack the heads, transfer the archive object (`ColdsyncArchive`) instead of restore+push.

### 2.5 The S3 data plane ("force-archive + rename")

Resharding never streams space data node-to-node on the main network: **S3 is the universal migration channel**, reusing the battle-tested archive machinery. This is the Cassandra snapshot-ship model adapted to CRDTs — the key property making it safe: *restoring from ANY snapshot (even stale) + tree-sync always converges; a snapshot is never wrong, only old.*

Primitives:

- **`ForceArchive(spaceId)`**: like `Archive()` but leaves the local DB intact and the status `Ok` — a pure snapshot-to-S3 (Backup + gzip + PUT). Used to ship live spaces; the post-snapshot delta converges via normal tree sync with the surviving replicas.
- **S3 rename**: server-side `CopyObject` from the source node's prefix to the target node's prefix (+ later delete of the source object). No bytes flow through node NICs; millions of copies cost dollars. A shared bucket across tree nodes is a **prerequisite** of the migration machinery (see scope note in §2); networks without one simply don't run it.
- **`AdoptArchive(spaceId, s3Key, headsHash, eager)` RPC** (control plane): tells the new owner "your copy is parked at this key". Receiver `HEAD`-verifies the object, registers `Archived` + heads in its index; `eager=true` queues an immediate background restore + hotsync (for spaces that were live on the drainer).
- **Durable ACK**: when an owner is asked "do you have heads H of space S?" and its index says `Archived`, it must S3-`HEAD` the object before ACKing (milliseconds, near-free). On a miss it repairs its index and NACKs, forcing a real transfer. Closes the "index says archived but the object is gone" hole — indexes are self-reported; disks and buckets are not trusted.
- **Per-node prefixes stay** (shared content-addressed namespace with cross-node dedup was considered and **rejected for now**): decentralized GC of shared objects is the classic data-loss vector — one buggy sweeper deleting a shared gz destroys the archive for *all* replicas at once, whereas per-node prefixes isolate the blast radius to one replica. If the ~3x archive storage cost ever becomes a problem, dedup can be added later with a strictly **centralized** GC that cross-references all node indexes.
- **Restore becomes copy, not move**: stop deleting the S3 object inside `Restore`; delete on the next successful re-archive instead. Idempotent restores, and a parked snapshot survives a botched restore. Optional hardening: content-addressed keys within the node prefix (`<prefix>/<spaceId>/<headsHash>.gz`) making objects immutable.
- **S3 prefix sweeper**: low-priority job listing the node's own prefix, deleting objects not matching the local index. Without it, node replacement (DiskGenID bootstrap-from-scratch) leaks the old prefix's millions of gz objects forever.
- **Tombstones delete archives too**: deletion-log processing for an `Archived` space issues the S3 DELETE directly (today's path would pointlessly restore the space just to delete it).
- **Throttling**: the bottleneck moves from node NIC/disk IOPS to S3 API limits. Token-bucket the migration workers' S3 calls (PUT/COPY) cluster-wide (e.g. hundreds/sec); a 100k-space partition handoff is 100k cheap index checks + a bounded stream of copies, with PUTs only for the live minority.

Failure semantics are the big win: a node dying mid-migration leaves data safely parked in S3; the target just resumes pointer adoption. Source and target never need to be online simultaneously.

### 2.6 Client-facing behavior

**MVP requires zero client protocol changes.** Existing behavior suffices:

- Stale client → draining node: gets `ErrPeerIsNotResponsible` (existing), refetches config, routes to new owners.
- Fresh client → bootstrapping node that lacks the space: node returns the existing transient error; client retries other replicas (2 of 3 are warm).

Accepted MVP risks: a ~10-min stale-client-to-stale-node write window (safe — the drain push hands those writes off; convergence merely delayed); +1 RTT for clients hitting a cold replica; the rare "cold replica + both warm replicas down" outage until background sync completes.

Post-MVP client protocol (in priority order):
1. **Pull-on-demand** on bootstrap nodes (node-side only, biggest UX win).
2. **Epoch in client RPCs** + `ErrConfigStale(latestConfig)` fast-bounce: instant client convergence, no waiting on polls. Node treats `clientEpoch > nodeEpoch` as a single-flighted, rate-limited (≥5s) trigger to refetch from the coordinator, returning a transient error meanwhile; never trust the claimed epoch value itself (fetch, then compare against the coordinator's real latest).

## 3. Failure modes explicitly covered

- **Node dies mid-reshard / epoch published mid-migration**: no global "reshard in progress" state to corrupt; nodes recompute diffs against the newest ring and retarget. Config history makes restart-safe.
- **All replicas replaced at once**: rejected by the coordinator overlap guardrail.
- **Replaced hardware, same identity**: DiskGenID forces bootstrap mode instead of serving empty.
- **Offline old owner reconnecting with un-handed-off writes**: drain verification happens *after* pushing local heads, so late writes are pushed before deletion; CRDT merge handles concurrency.
- **Deleted-space resurrection via anti-entropy**: tombstones + coordinator fallback.
- **ReplKey co-location**: locks for on-demand pulls must be **per-spaceId**, not per-partition/replKey, so one giant space doesn't block siblings.
- **Corrupted local copy**: quarantined, never propagated.
- **Archived-but-missing S3 object**: durable ACK (`HEAD` before ACK) detects it; index gets repaired and the drainer keeps its copy.
- **Hot space migrated via snapshot**: writes between snapshot (T0) and write-cutoff (T1) are hotsync-pushed to the 2 surviving replicas before local delete; the adopter restores the T0 snapshot and converges the T0→T1 delta (and anything newer) from the survivors. No lost-delta window.
- **Stale gz vs newer live DB**: `AdoptArchive` carries the heads hash and only the copy's owner initiates handoff from its own index state; restore-as-copy + (optional) content-addressed keys close the rest.
- **Buggy sweeper / rogue node**: per-node S3 prefixes confine damage to that replica's archive; RF=3 peers unaffected (this is why shared-namespace dedup was rejected).
- **S3 storage leaks**: per-node S3 sweeper (replacement scenario) + tombstone-time S3 delete (deletion scenario).

## 4. Observability & "done" definition

Per-node Prometheus metrics:

- `partitions_bootstrapping_total`, `partitions_draining_total`
- `spaces_orphaned_total`, `spaces_parked_total`, `spaces_quarantined_total`
- handoff throughput (bytes/sec), ACK counts, sweep progress

A reshard for epoch N is **complete** when `sum(bootstrapping) == 0 && sum(draining) == 0` across the fleet. This is an operational signal, not a coordinator precondition — epochs never block on it.

## 5. Phased plan

**Phase 1 — Epochs & safety rails** *(coordinator + nodeconf changes)*
- Epoch field + config history (coordinator API + local persistence in `nodeconfstore`).
- Coordinator overlap guardrail on config publish.
- DiskGenID registration + global-bootstrap mode on mismatch.
- Config-change hook in `nodeconf.Service` (`setLastConfiguration` currently swaps silently — add a subscription).

**Phase 2 — The S3 engine** *(data plane)*
- `ForceArchive` (snapshot without deleting/flipping the live DB).
- S3 `Transfer(src, dst)`: server-side `CopyObject` between node prefixes in the shared bucket (gated on the migration-machinery prerequisite; no streaming fallback is built).
- `AdoptArchive(spaceId, s3Key, headsHash, eager)` RPC + `HEAD` verification + eager-restore queue.
- Restore-as-copy (move S3 delete from `Restore` to re-archive; consider content-addressed keys here since it changes the key scheme).
- Cluster-wide S3-call token bucket for migration workers.
- Metrics.

**Phase 3 — The state machine** *(bootstrap & drain, makes add/remove/replace safe)*
- Ring-diff on epoch change → per-partition Bootstrapping/Draining (today `nodesync/nodesync.go` `getRelatePartitions` only looks at the current ring, timer-driven).
- Drain sequence: metadata verification (durable ACK incl. S3 `HEAD`) → hotsync-push on divergence → ForceArchive/CopyObject/AdoptArchive handoff → ≥2-owner rule → local delete of DB/index/S3 object (reuse `SpaceStatus` transitions in `nodestorage/indexstorage.go`).
- Bootstrap: adopt pointers, eager-restore hot spaces, pull mode for dead-drainer recovery.
- Deletion tombstones + `ErrSpaceDeleted` handshake in handoff prep; tombstone execution deletes the S3 object for archived spaces directly.

**Phase 4 — Cleanup & polish**
- Legacy orphan sweeper (with quarantine + park + archived branch), retiring the manual `spacechecker` flow — same logic as drain, run lazily.
- Per-node S3 prefix sweeper (orphaned gz objects after replacements/migrations).
- Tombstone TTL GC + coordinator deletion-log fallback.
- Pull-on-demand on bootstrap nodes.
- (Later) client epochs + `ErrConfigStale`; capacity weights in chash (lib already supports them — weight changes are just topology changes to this machinery); RF increase uses the same bootstrap path.

Ordering rationale: the S3 engine is deliberately built *before* the state machine — it's small, independently testable (ForceArchive + CopyObject + AdoptArchive can be exercised manually on single spaces), and every later phase rides on it. What this plan **no longer contains** (killed by the S3 data plane): the `ColdsyncArchive` streaming RPC, coldsync overwrite/merge/resume changes, node-to-node streaming queues and their throttling.

## 6. Open items to settle before implementation

- Where epoch lives in the coordinator's data model and how existing `ConfigurationId` consumers migrate.
- Exact ACK semantics in the hotsync push session (today sync is symmetric; need an explicit "peer needs nothing" signal surfaced to the drain controller).
- Throttle defaults (bytes/sec per node, sweep rate) — derive from production disk/network headroom.
- Exact gating of the migration machinery: config flag vs auto-detect "all tree nodes share an archive bucket" via nodeconf metadata. Safety rails (epochs, guardrail, DiskGenID, tombstones) ship everywhere regardless; only drain/bootstrap data movement is gated.
- **Verify production S3 layout**: the design assumes per-node bucket/prefix. If any replicas currently share a prefix, today's restore-deletes-object behavior (`archive/archive.go:199`) is a live bug independent of resharding — one replica's restore breaks the others' archived state.
- Whether `DumpStorage`-triggered restores should be blocked once the S3 data plane exists (a node→node request for an archived space should go through `AdoptArchive`/`Transfer`, never force a restore on the source).
- Content-addressed S3 keys require a migration for existing objects (or dual-scheme lookup during a transition window).
- How a node detects "shared S3 bucket" for the `Transfer` fast path (nodeconf metadata vs static node config), and the IAM policy allowing cross-prefix `CopyObject` within the bucket.
- Sizing: measure the live-vs-archived ratio per partition in production to predict PUT volume (`ForceArchive` of the live minority) and migration wall-clock for a typical node decommission.

## 7. Implementation status (2026-07-04, branch `resharding`)

Implemented across three branches (any-sync, any-sync-coordinator, any-sync-node — all named `resharding`):

- **Phase 1**: `Configuration.Epoch` + proto field; `nodeconf.Service.ObserveChanges`; `nodeconf.HistoryStore` (last 100 epochs on disk); coordinator epoch assignment + partition-overlap guardrail in `nodeconfsource.Add` (`confapply -force` escape hatch); `.diskgen` fresh-storage marker.
- **Phase 2**: `archivestore.Exists/Key/CopyFrom/List` + `shared` config flag; `archive.ForceArchive` (snapshot, DB stays live); restore-as-copy; `AdoptArchive` RPC + adopter component (HEAD-verified server-side copy, head-aware AlreadyHave responses, node-only); eager-restore queue.
- **Phase 3**: `resharder` component — drain cycle on config change + periodic; metadata-first verification, ≥2-current-owner ACK rule, heads recheck before deletion, hotsync convergence for diverged owners, `SpaceStatusMoved`; spacedeleter deletes S3 objects directly for archived spaces.
- **Phase 4**: archive prefix sweeper (daily, 7-day age guard) + unindexed-dir reconciliation.
- **Tests**: unit coverage per package + in-process integration test (`resharder/integration_test.go`) driving drain → adopt → eager-restore over two real node stacks and a shared in-memory bucket. Manual full-network runbook: `docs/resharding-e2e.md`.

Deviations from the plan, deliberate:
- DiskGenID is a local marker only (no coordinator registration): a wiped disk simply looks like an empty node — reads of missing spaces already error (clients retry replicas), nodesync repopulates, and the guardrail prevents multi-replica replacement. Registration can be added later if operators want alerting.
- Drain verification uses AdoptArchive responses (Ok / AlreadyHaveSame / Diverged by index heads) instead of a separate heads-query RPC — one round-trip does both handoff and verification. Divergence resolves through hotsync (space load syncs with current owners) rather than an explicit push session.
- Bootstrapping is push-driven by drainers (plus existing lazy nodesync as pull-mode repair); no separate bootstrap state machine was needed.
- Quarantine of corrupted DBs is not automated (failed handoffs park and are visible via `node_resharder_parked`; `spacechecker` remains the operator tool).

Rollout order: release any-sync → bump + release coordinator (guardrail is active immediately; epochs appear on the next `confapply`) → bump + roll nodes (machinery stays dormant until `s3Store.shared: true`).
