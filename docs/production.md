# Production operations

Production deployment is possible only after workload-specific compatibility,
load, failure, restore, and security testing. Start with a private network,
TLS, explicit credentials, durable storage, and a verified archive.

## Resource and topology checklist

| Concern | Baseline |
| --- | --- |
| Storage | Local NVMe for WAL and segments; monitor free space and fsync latency |
| Memory | Size `cache_bytes` for the hot working set and leave headroom for the process |
| Connections | Use pooling; `max_connections` is a safety cap, not a target |
| Cluster | Three members for a single-region quorum; odd membership is easier to reason about |
| Archives | Local plus a separate object/region target; scheduled verification drill |
| Network | TLS/mTLS, stable advertised addresses, explicit firewall rules |
| Deployments | Drain/transfer leadership, snapshot, upgrade one member at a time |

## Health and monitoring

Probe:

```bash
curl -fsS https://db.example.com/health
curl -fsS https://db.example.com/ready
curl -fsS https://db.example.com/metrics/prometheus
```

Alert on readiness loss, leader absence, term churn, commit lag, range
repair, WAL/segment growth, archive age, failed drills, connection rejection,
QoS throttling, p99/p99.9 latency, and repeated SQLSTATE `40001` conflicts.

The JSON metrics endpoint and Prometheus output expose request histograms,
operation counts, storage/cache state, QoS, ranges, and realtime counters.
`/v1/observe/slow` and `/v1/traces` provide recent slow operations and spans.

## QoS and noisy neighbors

Every authenticated tenant can be assigned a QoS tier. The tier controls
request rate, concurrent work, bytes, realtime messages, and socket quotas.
Inspect and change it with:

```bash
rymedb-cli qos
rymedb-cli qos tier tenant-a dedicated_cluster
```

Benchmark with the intended tier. The default shared tier intentionally limits
throughput and is not a hardware ceiling.

## Deployment and upgrade runbook

1. Read the changelog and compatibility notes.
2. Run the full tests and migration suite against a copy of production data.
3. Take a checkpoint and verify the newest archive.
4. For a cluster, transfer leadership away from the node being upgraded.
5. Stop one node, replace the binary/container, and wait for readiness/catch-up.
6. Repeat without taking the cluster below quorum.
7. Run driver, REST, RESP, realtime, and restore smoke tests.

Do not change the data directory format by hand. Keep the previous binary and
an external backup until the rollback window closes.

## Security operations

Use secret managers for API/JWT/KMS material, rotate keys, limit owner/admin
credentials, and audit archive and membership routes. File permissions must
protect `data_dir`; archive object policies should deny public access.

## Failure response

Preserve logs, metrics, node configuration, term/commit state, and the exact
request or migration before restarting repeatedly. Use
[troubleshooting](troubleshooting.md) and [backup/restore](backup-restore.md)
for the decision tree.
