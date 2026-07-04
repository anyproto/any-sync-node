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
4. Watch progress per node:
   - `node_resharder_draining` — spaces still to hand off (goal: 0)
   - `node_resharder_moved` — spaces handed off and deleted locally
   - `node_resharder_parked` — handoffs postponed to the next cycle
     (a persistently high value means an owner is unreachable or diverged)
   - `node_archive_adopted` on the new node — spaces received
5. Done when `node_resharder_draining` is 0 on all nodes. No client action is
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
3. Watch the removed node: `node_resharder_draining` → 0 and
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
