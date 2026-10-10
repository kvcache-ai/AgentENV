# HA scheduler overlay (#259)

Three leader-elected scheduler replicas behind one Service, competing for a
`coordination.k8s.io/Lease` (`agentenv-system/agentenv-scheduler`). Exactly
one pod schedules and processes node reports; standbys stay liveness-healthy
and serve `LookupNode`/`GetNode` from the shared Redis bindings.

Traffic rules:

1. Node heartbeats → `agentenv-scheduler` Service (leader only).
2. Gateway writes (`Schedule`, `RecordAssignment`) → same Service.
3. Gateway reads (`LookupNode`) → same Service; served by standbys when
   `scheduler.redis_addr` is set, otherwise retried onto the leader.

Deploy:

```bash
kubectl apply -k deploy/k8s/overlays/ha
```

Prerequisites:

- RBAC: the scheduler Role needs `coordination.k8s.io/leases`
  get/create/update (already in `base/role.yaml`).
- Redis (recommended): set `scheduler.redis_addr` in
  `config/scheduler.json`. Without it the scheduler runs the documented
  degraded mode: reads retry to the leader, and failover loses routing for
  pre-failover sandboxes.
- `scheduler.node_admin_api_key`: the x-api-key for node admin APIs, used by
  the leader to pull node snapshots right after winning (sync-node-snapshots).

Upgrade ordering (rolling this out over a single-replica deployment):

1. Upgrade the scheduler binary first — with election off it behaves exactly
   like today, and it must know the leader health service before the probe
   points at it.
2. Then apply this overlay (RBAC, probe, replicas, config).

Rollback: set `leader_election.enabled` to false and scale back to 1 replica.
