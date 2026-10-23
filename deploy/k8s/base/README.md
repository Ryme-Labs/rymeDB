# rymeDB plain manifests (no Helm)

Single-node production install. For clustered Raft, use
`deploy/k8s/helm/rymeDB` which renders peer-aware entrypoints.

The manifest uses the published GHCR image. Replace the tag with a release
tag when deploying a version other than `2.0.1`:

```sh
kubectl -n rymedb set image statefulset/rymedb \
  ryme-server=ghcr.io/ryme-labs/rymedb:<version>
```

```sh
kubectl create namespace rymedb
read -rsp 'API key (at least 16 bytes): ' RYME_API_KEY; echo
read -rsp 'PostgreSQL password (at least 16 bytes): ' RYME_PG_PASSWORD; echo
read -rsp 'RESP password (at least 16 bytes): ' RYME_RESP_PASSWORD; echo
kubectl -n rymedb create secret generic rymedb-api-key \
  --from-literal=api-key="$RYME_API_KEY"
kubectl -n rymedb create secret generic rymedb-protocol-passwords \
  --from-literal=pg-password="$RYME_PG_PASSWORD" \
  --from-literal=resp-password="$RYME_RESP_PASSWORD"
unset RYME_API_KEY RYME_PG_PASSWORD RYME_RESP_PASSWORD
kubectl apply -f deploy/k8s/base/statefulset.yaml
kubectl -n rymedb get pods -w
curl http://$(kubectl -n rymedb get svc rymedb -o jsonpath='{.spec.clusterIP}'):8080/health
```
