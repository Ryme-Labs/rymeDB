# rymeDB plain manifests (no Helm)

Single-node production install. For clustered Raft, use
`deploy/k8s/helm/rymeDB` which renders peer-aware entrypoints.

```sh
kubectl apply -f deploy/k8s/base/statefulset.yaml
kubectl -n rymedb create secret generic rymedb-api-key \
  --from-literal=api-key=<key> --dry-run=client -o yaml | kubectl apply -f -
kubectl -n rymedb get pods -w
curl http://$(kubectl -n rymedb get svc rymedb -o jsonpath='{.spec.clusterIP}'):8080/health
```
