# Resharding: manual e2e runbook

The in-process integration test (`resharder/integration_test.go`) covers the
data plane (drain → adopt → eager restore) with real storage and archive
stacks. This runbook exercises the full network path with real processes.

## Prerequisites

- mongodb on `localhost:27017` (coordinator storage)
- MinIO (or any S3-compatible store) with one bucket, e.g. `anysync-archive`
- binaries built from the resharding branches: `any-sync-coordinator`,
  `confapply`, `any-sync-node`
- network configs generated with `any-sync-network` (any-sync-tools),
  4 tree nodes + coordinator

## Node configuration

Every tree node gets the shared bucket with its own prefix and archiving on:

```yaml
s3Store:
  enabled: true
  endpoint: http://127.0.0.1:9000
  bucket: anysync-archive
  forcePathStyle: true
  keyPrefix: <nodePeerId>        # unique per node
  shared: true                   # gates the resharding machinery
  credentials: { accessKey: ..., secretKey: ... }
archive:
  enabled: true
  archiveAfterDays: 1
resharder:
  drainIntervalMinutes: 5
```

## Scenario 1: add a node

1. Start coordinator + tree nodes 1–3 with an epoch-1 config
   (`confapply -c coordinator.yml -n network.yml -e` → prints `epoch: 1`).
2. Create spaces via an any-sync-sdk2 client (`e2e/local.yml` shape) and let
   them sync; optionally wait for archiving to kick in.
3. Publish an epoch-2 config adding tree node 4 (`confapply ... -e`).
4. Within the nodeconf poll interval every node logs
   `net configuration changed` with `afterEpoch: 2` and drain cycles start.
5. Watch metrics: `node_resharder_draining` falls to 0; `node_resharder_moved`
   and `node_archive_adopted` grow. Spaces whose partitions moved to node 4
   appear in its index as Archived (dormant) or restored (hot, eager).
6. Verify a moved space: client reads it through node 4; the drainer answers
   `ErrPeerIsNotResponsible` for client requests and its copy is gone
   (index status Moved).

## Scenario 2: unsafe config rejected

Publish a config replacing all tree nodes at once: `confapply` must fail with
`unsafe configuration: N of 3000 partitions lose all current replicas`.
Re-run with `-force` only to confirm the escape hatch works.

## Scenario 3: node decommission

Same as scenario 1 but epoch-2 removes a node; watch its `draining` gauge go
to zero, then it can be shut down. Its S3 prefix empties as handoffs complete
(leftovers are collected by the daily archive sweep after the 7-day age guard).

## Scenario 4: deletion during drain

Delete a space (client flow → coordinator deletion log) while its old owner
drains: the owner receives `ErrSpaceDeleted` from the adopters and drops its
copy without resurrecting the space.
