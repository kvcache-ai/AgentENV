# HA scheduler overlay (#259)

Three leader-elected scheduler replicas behind one Service, competing for a
`coordination.k8s.io/Lease` (`agentenv-system/agentenv-scheduler`). Exactly
one pod schedules and processes node reports; standbys stay liveness-healthy
and serve `LookupNode`/`GetNode` from the shared Redis bindings.

Traffic rules:

1. Node heartbeats → `agentenv-scheduler` Service (leader only).
2. Gateway writes (`Schedule`, `RecordAssignment`) → same Service.
3. Gateway reads (`LookupNode`) → same Service, answered by the leader.

Read gap on failover: because endpoints contain only the leader, lookups
fail fast from the leader's death until the new leader is ready
(~lease_duration + election + probe, roughly 15–25s with defaults). With
`scheduler.redis_addr` bindings survive (TTL floored to
`lease_duration + 90s`) and lookups resume automatically; without it,
pre-failover bindings are lost (degraded mode). See "Failover read gap"
in services/README.md.

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
- `scheduler.node_admin_api_key`: the `X-API-Key` for node admin APIs, used by
  the leader to pull node snapshots right after winning (sync-node-snapshots).

Upgrade ordering (rolling this out over a single-replica deployment):

1. Upgrade the scheduler binary first — with election off it behaves exactly
   like today, and it must know the leader health service before the probe
   points at it.
2. Then apply this overlay (RBAC, probe, replicas, config).

Rollback: set `leader_election.enabled` to false and scale back to 1 replica.

Image updates: the overlay pins `rollingUpdate.maxUnavailable: 100%` /
`maxSurge: 1`. A default strategy deadlocks — the surged new-version pod is an
unready standby while the old leader keeps renewing, and `maxUnavailable: 0`
then forbids removing any old pod. Only one pod is ever Ready, so the
controller needs `minAvailable: 0` to make progress; with `maxSurge: 1` a
new-version pod is already warm when the old leader's lease expires. An image
rollout therefore rides the same bounded, self-healing failover gap
(~15–25s with defaults, see above), possibly more than once if the Lease passes
between old-version standbys, and finishes with the holder on the new image:

```bash
kubectl set image deployment/agentenv-scheduler scheduler=<image>:<tag> -n agentenv-system
kubectl get lease agentenv-scheduler -n agentenv-system -o jsonpath='{.spec.holderIdentity}'
```

`kubectl rollout status` completes once the pods are replaced (with
`minAvailable: 0` it does not wait for a new leader); check the Lease holder's
pod image for the real "new version is serving" signal.
