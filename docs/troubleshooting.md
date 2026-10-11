# Troubleshooting

Start with the smallest reproducible request and preserve evidence before
restarting anything.

## Fast diagnosis

```bash
curl -i http://127.0.0.1:8080/health
curl -i -H "Authorization: Bearer $RYME_API_KEY" http://127.0.0.1:8080/ready
curl -s -H "Authorization: Bearer $RYME_API_KEY" http://127.0.0.1:8080/metrics | jq
rymedb-cli observe slow --limit 20
rymedb-cli traces --limit 20
```

Record the server commit, config (without secrets), node/leader, tenant,
branch, request id, exact SQL/command shape, payload size, and timestamp.

## Symptom matrix

| Symptom | Check first | Safe next action |
| --- | --- | --- |
| `connection refused` | Listener address, process logs, container port mapping | Run `/health`; verify `--*-listen` is reachable and not loopback inside Docker |
| `401` | API key/JWT header, issuer/audience, clock, `RYME_*` env | Test a known local key; do not disable auth on a public listener |
| `403` | Principal role, tenant, RLS policy, operator permission | Use the least privileged correct principal and inspect policy metadata |
| `40001` | Transaction scope, scan dependencies, hot keys | Retry the complete transaction with bounded backoff |
| `53300`/overloaded | `max_connections`, pool size, QoS tier | Pool connections and inspect slow work; do not blindly raise the cap |
| `57014` | `statement_timeout`, cancellation, slow plan | Reduce scope, add the appropriate index, or increase timeout intentionally |
| stream `cursor_expired` | Retention window and reconnect delay | Resnapshot, persist the new cursor, and reconnect without the expired cursor |
| stale readiness | Leader election, quorum, disk, range repair | Check cluster members and logs; preserve data before failover surgery |
| archive failure | target credentials, bucket/region, disk, KMS key | Run `backup verify` and fix the target before deleting old copies |
| high latency | QoS, fsync, payload, contention, CPU, queueing | Compare a controlled benchmark and inspect p99 traces |

## Realtime debugging

For CDC, query streams, and topics, log the last delivered sequence/cursor and
whether a snapshot was received. A raw change feed can expire; a query stream
resynchronizes with a snapshot; broadcast has no replay; durable topics retain
messages only within their bounded window. Test the reconnect path deliberately.

## Data safety rules

- Do not `rm -rf` a data directory to “fix” startup.
- Do not delete WAL, segment, branch, or placement files by hand.
- Do not force a Raft node to commit without quorum.
- Take a verified copy before restore or migration experiments.
- Treat a mismatch after restore as a stop condition, not a warning to ignore.

## Report a useful bug

Include the smallest SQL/RESP/HTTP request, expected and actual output,
version/commit, OS and hardware, config shape, whether the data is disposable,
and a test or log excerpt. Remove API keys, passwords, JWTs, customer data,
and private archive URLs. Vulnerabilities use the private process in
[security](security.md), not a public issue.
