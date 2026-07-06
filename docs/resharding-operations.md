# Resharding: operator guide

How to change the set of tree nodes of a running network. Two supported
operations: **adding node(s)** and **removing node(s)**. Everything else
(hardware replacement) is a combination of the two.

Covered by tests: `resharder/scenarios_test.go` (`TestScenario_AddNode`,
`TestScenario_RemoveNode`) runs both flows in-process over real chash rings;
`resharder/integration_test.go` runs the handoff against a real S3 bucket when
`ARCHIVE_TEST_S3_*` env vars are set.

## Prerequisites

Resharding is gated on a **shared archive store**. Every tree node (including
new ones) needs, in `any-sync-node.yml`:

```yaml
s3Store:
  enabled: true
  bucket: <one bucket shared by all tree nodes>
  keyPrefix: <unique per node, e.g. the peerId>
  shared: true            # gates the resharding machinery
  # endpoint/credentials as usual (GCS interop: endpoint https://storage.googleapis.com,
  #   region us-east-1, forcePathStyle: true)
archive:
  enabled: true
```

Without `shared: true` nodes never drain — topology changes then behave as
before this feature (data stays on the old nodes; only anti-entropy fills the
new ones).

Network configurations are published with `confapply` (coordinator repo):

```
confapply -c coordinator.yml -n network.yml -e
# prints: id: <configurationId> epoch: <N>
```

Every publish gets a monotonically increasing **epoch**. `confapply` refuses
configurations where any chash partition would lose *all* of its current
replicas (`unsafe configuration: ...`); `-force` overrides this check —
emergencies only, it can produce partitions with no live source.

## First deployment (one-time preparations)

1. **Merge/release order**: any-sync → tag → bump the dependency in
   coordinator and node → release both. Old node builds are compatible with
   new ones (an old receiver answers `AdoptArchive` with "unknown rpc"; the
   drainer parks and retries), so binaries can roll gradually — but publish
   no topology change until the whole tree fleet runs the new build.
2. **Audit the S3 layout before enabling anything**:
   - every tree node must use the **same bucket** (server-side copy does not
     cross buckets) and a **unique keyPrefix** — verify no two nodes share a
     prefix (a shared prefix was already subtly broken before this feature);
   - node credentials must allow `GetObject` bucket-wide (they become
     CopyObject *sources* for keys other nodes hand them) plus write/list on
     their own prefix.
3. **Coordinator deploy**: on start it creates a unique partial index on
   `nodeConf.epoch` — the mongo user needs `createIndex`. Existing configs
   carry no epoch (treated as 0); the first `confapply` mints epoch 1.
4. **Deploy nodes with `shared: false` first** (or leave the flag out): the
   machinery stays dormant. Expect a one-time `fresh disk or new node`
   warning per node — the `.diskgen` marker is created on first boot after
   the upgrade. Its creation time also arms the sweeper's 30-day protection
   for unindexed legacy objects automatically.
5. **Dashboards/alerts before enabling**: panel the `node_resharder_*` and
   `node_adopter_*` gauges; alert on `state == 2` lasting days,
   `parked`/`errors` growing without `moved` growing, and coordinator
   `unsafe configuration` rejections.
6. **Mint a baseline epoch**: republish the *current unchanged* topology
   (`confapply ... -e`). Tree membership is identical, so no drains are
   scheduled — it just assigns epoch 1 everywhere and starts the config
   history that removed-node draining later relies on.
7. **Flip `s3Store.shared: true` fleet-wide.** Note: the first drain cycles
   will also digest *legacy* orphans (spaces from past topology changes that
   were never cleaned up) — expect `draining > 0` and background handoffs
   without any new topology change. That is the intended cleanup; it is
   verified the same way as a live drain. A legacy `notresponsible/` dir
   from the old spacechecker tool shows up as an unindexable id in logs —
   harmless; remove those dirs manually at leisure.
8. **Optional but recommended for the first real reshard**: enable bucket
   object versioning with a 30-day expiry of noncurrent versions — cheap
   insurance while trust in the machinery is being established; drop it
   later. Snapshot the coordinator's `nodeConf` collection before the first
   topology publish.
9. **Dry-run in staging** first: docs/resharding-e2e.md walks the full
   add/remove/guardrail/delete-during-drain scenarios on a local network.

## Adding node(s)

1. Deploy the new node(s) with the shared `s3Store` config; start them. They
   join with an empty storage root (logged as `fresh disk or new node`).
2. Add the node(s) to the network YAML with type `tree` and publish:
   `confapply ... -e`. If the guardrail rejects the change (too many
   partitions would change hands at once), add fewer nodes per step.
3. Within the nodeconf poll interval (`networkUpdateIntervalSec`, default
   10 min) every node logs `net configuration changed` and starts a drain
   cycle. Nodes that lost partitions hand the affected spaces to the new
   owner(s) through the bucket (archived spaces: server-side copy; live
   spaces: snapshot + eager restore on the new node) and delete their local
   copies only after **two current owners confirm the same heads**.
4. Watch progress per node (Prometheus):
   - `node_resharder_state` — 0 disabled (bucket not shared), 1 idle,
     2 draining; the whole fleet back at 1 = resharding complete
   - `node_resharder_epoch` — configuration epoch the node acts on; shows
     config propagation across the fleet after a publish
   - `node_resharder_draining` — spaces still to hand off (goal: 0)
   - `node_resharder_moved` — spaces handed off and deleted locally
   - `node_resharder_parked` / `node_resharder_errors` — handoffs postponed
     to the next cycle (persistently high = an owner is unreachable or
     diverged) and failed drain attempts
   - `node_resharder_last_cycle_unix` — when the last drain cycle finished
   - `node_adopter_adopted` on the new node — spaces received;
     `node_adopter_already_have_same` on surviving owners — head-verified
     ACKs; `node_adopter_rejected` — refused/failed adopt requests
5. Done when `node_resharder_state` is back to 1 (idle) and
   `node_resharder_draining` is 0 on all nodes. No client action is
   needed at any point: clients are bounced to the new owners by the normal
   `ErrPeerIsNotResponsible` flow when they refresh the configuration.

## Removing node(s)

1. Publish a configuration without the node(s): `confapply ... -e`. The
   guardrail ensures each partition keeps at least one current replica —
   remove at most one member of any replica set per step (in practice:
   remove one node at a time, or unrelated nodes together).
2. **Keep the removed node running.** It keeps polling the coordinator, sees
   the new epoch, and starts draining everything it holds to the current
   owners. Receivers recognize it through the retained configuration history
   (last 100 epochs), so being absent from the current config does not block
   the handoff.
3. Watch the removed node: `node_resharder_state` → 1 (idle),
   `node_resharder_draining` → 0 and
   `node_resharder_moved` settles. Spot-check with the debug API if desired:
   all its spaces end in status `Moved` and its bucket prefix empties.
4. Shut the node down and decommission it. Leftover objects in its prefix
   (if any) are collected by the daily archive sweep after the age guard.

## Replacing a node

Replace = add the new node (step above), wait for draining to finish, then
remove the old one. For a **dead** node (hardware loss, no drain possible):
publish the config without it; the surviving replicas hold every partition
(guardrail invariant) and the normal `nodesync` anti-entropy repopulates the
new owner(s). The dead node's bucket prefix can be deleted manually after
recovery is confirmed.

## Spaces in Error state (missing or corrupted data)

Resharding deliberately routes **around** broken copies instead of touching
them; redundancy is maintained by the healthy replicas.

How a broken copy behaves during a reshard, by kind:

- **Index status `Error`** (failed archive, drain found neither object nor
  db, ...): excluded from drain candidates, its archive object is never
  swept, and an owner holding an `Error` entry answers adopt requests with
  *diverged* — it neither adopts a snapshot over the possibly-valuable local
  data nor counts as a durable ACK for anyone's deletion.
- **Corrupted db with status `Ok`**: the drain's snapshot fails to open it —
  the space parks and retries every cycle; `node_resharder_errors` grows and
  the failure is logged with the space id. Nothing is deleted.
- **Status `Ok` with no local data at all**: nothing to hand off — the entry
  is marked `Moved`; the current owners get the space from the healthy
  replicas via the normal handoff/anti-entropy.

Why this is safe: the drain deletion rule needs **2 of 3 current owners** to
confirm the heads, so one broken owner never blocks a reshard (the two
healthy ones ACK) and never enables a wrong deletion (it refuses to ACK).
The space stays fully available through its healthy replicas throughout.

**Automatic self-healing (repairer).** A periodic loop (default hourly,
`repairer.repairIntervalMinutes`) repairs `Error` spaces this node is
responsible for:

1. *Repair in place*: if the local db opens and indexes fine, the error was
   transient (e.g. one failed archive upload) — the status is cleared without
   touching data.
2. *Quarantine + re-pull*: a genuinely broken db is moved to
   `<storage-root>/.quarantine/<spaceId>-<timestamp>` (data preserved for the
   operator, invisible to the node) and a valid copy is pulled from a
   responsible neighbor with the regular coldsync — works on every network,
   shared bucket or not. The pulled copy is validated before the status
   clears; on failure the space stays `Error` and is retried next cycle.

Metrics: `node_repairer_errored` (responsible Error spaces left after the
last cycle — the alerting signal), `repaired`, `repaired_in_place`,
`quarantined`.

Operator involvement is only needed for what the repairer refuses to touch:
`Error` spaces the node is *not* responsible for (drain territory — the
healthy owners already serve them; inspect with `spacechecker`, then clear
or delete), persistent `node_repairer_errored > 0` (all replicas
unreachable/broken — investigate the peers), and the `.quarantine` dir,
which is never cleaned automatically: delete old entries after confirming
the repaired spaces are healthy.

## Safety properties (what the machinery guarantees)

- A drained space is deleted locally only after ≥2 current owners durably
  hold **exactly the advertised heads** (verified against the snapshot, with
  an S3 existence check for archived copies).
- Writes landing during a handoff park the space for the next cycle — never
  lost, never handed off stale.
- Spaces pending deletion are not drained; cancelled deletions cannot lose
  the archived copy.
- A configuration can always be published mid-migration: nodes simply
  retarget to the newest owners. Epochs never wait for drains to finish.
