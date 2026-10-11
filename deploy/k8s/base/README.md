# rymeDB plain manifests (no Helm)

Single-node production install. For clustered Raft, use
`deploy/k8s/helm/rymeDB` which renders peer-aware entrypoints.

The manifest uses the published GHCR image. Replace the tag with a release
tag when deploying a version other than `0.1.0`:

```sh
kubectl -n rymedb set image statefulset/rymedb \
  ryme-server=ghcr.io/ryme-labs/rymedb:<version>
```

```sh
kubectl apply -f deploy/k8s/base/statefulset.yaml
kubectl -n rymedb create secret generic rymedb-api-key \
  --from-literal=api-key=<key> --dry-run=client -o yaml | kubectl apply -f -
kubectl -n rymedb get pods -w
curl http://$(kubectl -n rymedb get svc rymedb -o jsonpath='{.spec.clusterIP}'):8080/health
```
