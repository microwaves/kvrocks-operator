# Kvrocks operator

A Go operator for durable [Apache Kvrocks](https://github.com/apache/kvrocks) databases, licensed under Apache-2.0.

The first milestone manages one persistent standalone instance. Backup/restore, replication, and sharding are planned milestones. This is an initial implementation for development and learning; it has no high availability or automated recovery from volume loss.

## API

```yaml
apiVersion: kvrocks-operator.org/v1alpha1
kind: Kvrocks
metadata:
  name: example
spec:
  image: apache/kvrocks:2.17.0
  storage:
    size: 1Gi
  auth:
    passwordSecretRef:
      name: example-auth
      key: password
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
```

The image defaults to `apache/kvrocks:2.17.0`; the Secret key defaults to `password`. Omitting `storageClassName` selects the cluster's default StorageClass. `nodeSelector` and `tolerations` are optional. Names must be DNS labels starting with a letter and at most 50 characters.

Storage and image are immutable in this API version. Resource and scheduling changes can restart the single instance and interrupt availability. Secret updates also restart the instance to load new credentials. Passwords must be nonempty UTF-8, at most 512 bytes, without NUL, CR, or LF characters. Quotes, backslashes, and spaces are supported.

The operator enables RocksDB WAL and synchronous writes. The storage provider must honor synchronization requests. Kvrocks namespace tokens are stored in the database using `repl-namespace-enabled`, so writable runtime configuration can be recreated on restart.

## Resource ownership

Each database gets a retained PVC, a configuration ConfigMap, a client Service, a headless Service, and a one-replica StatefulSet. The client endpoint is `<name>.<namespace>.svc:6666`.

The operator watches one namespace, selected by `WATCH_NAMESPACE`. Its deployed Role is restricted to that namespace. It can read Secrets but cannot write them or delete PVCs. The database runs as a non-root user, without a service-account token, with a read-only root filesystem and dedicated writable data, runtime, and temporary volumes.

The password is supplied through a Secret reference and rendered into a private runtime configuration file. It is not embedded in a ConfigMap, status, container argument, or log. The initial milestone provides password authentication over an internal ClusterIP Service; TLS and network isolation policies are not implemented yet. Use a trusted development network.

A `Ready` condition reports the current observation and its `observedGeneration`. Reasons distinguish missing or invalid Secrets, pending storage, an incomplete rollout, missing data, and ownership conflicts. `Ready=True` means the current rollout passed its database readiness probe. It is not a guarantee of availability between observations or a backup guarantee.

## Local development

Prerequisites: Go 1.26+, Docker, kind, and kubectl compatible with the Kubernetes version under test. Dependency and generation-tool versions are pinned in `go.mod` and `Makefile`.

Create a dedicated cluster and kubeconfig. These commands do not use your normal Kubernetes context:

```bash
set -euo pipefail
mkdir -p bin
export KUBECONFIG="$(pwd)/bin/development.kubeconfig"
kind create cluster --name kvrocks-operator --kubeconfig "${KUBECONFIG}"
make docker-build
platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"
docker save --platform "${platform}" -o bin/operator.tar kvrocks-operator:dev
kind load image-archive bin/operator.tar --name kvrocks-operator
kubectl apply -k config/default
kubectl -n kvrocks-operator-system rollout status deployment/kvrocks-operator-controller-manager
```

The default manifests deploy the operator and watch `kvrocks-operator-system`. To use another namespace, edit `config/default/kustomization.yaml`; keep the namespace's Role and RoleBinding with the operator.

Create a password Secret and the database:

```bash
set -euo pipefail
password_file="$(mktemp)"
trap 'rm -f "${password_file}"' EXIT
openssl rand -hex 32 | tr -d '\n' > "${password_file}"
kubectl -n kvrocks-operator-system create secret generic example-auth \
  --from-file="password=${password_file}"
rm -f "${password_file}"
trap - EXIT
kubectl -n kvrocks-operator-system apply -f config/samples/kvrocks-operator_v1alpha1_kvrocks.yaml
kubectl -n kvrocks-operator-system wait --for=condition=Ready kvrocks/example --timeout=180s
kubectl -n kvrocks-operator-system exec example-0 -- redis-cli -p 6666 SET learning persisted
```

The database container's `REDISCLI_AUTH` environment provides authentication for this development command. Clients outside the container should obtain credentials through their own Secret reference.

For local debugging, set `KUBECONFIG` and `WATCH_NAMESPACE` explicitly before `make run`. Run only one operator for that namespace. Stop the deployed operator before running a local copy.

## Tests

```bash
make test-unit
make test-integration
```

Unit tests cover credential validation and resource safety. Integration tests start a real API server and etcd with `envtest`; they cover validation/defaults, reconciliation, readiness, no-op convergence, repair, rotation, and storage ownership. `envtest` does not run built-in controllers or actual database processes.

For the complete lifecycle test, build and load the image into the dedicated cluster above, then run:

```bash
set -euo pipefail
export KIND_CLUSTER=kvrocks-operator
export KUBECONFIG="$(pwd)/bin/development.kubeconfig"
make test-e2e
```

`KUBECTL` may select a specific kubectl binary. The test refuses an implicit kubeconfig or a context that does not match `KIND_CLUSTER`. It resets its own `e2e-database` and `e2e-auth` fixtures, including retained test storage, before each run. It installs the operator and verifies authentication, synchronous writes, Pod replacement with preserved data, Service and ConfigMap repair, operator restart, password rotation, and PVC retention. It leaves retained test storage in the dedicated cluster for inspection.

To remove the entire development environment, explicitly delete that kind cluster:

```bash
kind delete cluster --name kvrocks-operator
```

## Data lifecycle and limitations

Deleting a `Kvrocks` resource removes its ordinary children through garbage collection but retains its PVC. Creating a new database with the same name will not adopt that PVC: the original resource UID is recorded on the claim. Adoption and restore workflows belong to milestone two.

Before starting the first database Pod, the operator persists the volume name and UID in status. An uncached API read prevents stale cached status from authorizing fresh storage for an established database. If it disappears, the operator reports `VolumeMissing` and does not create an empty replacement. Do not clear that status or manually replace PVCs: those are administrative recovery operations outside the supported lifecycle. Kubernetes workload references identify PVCs by name, so status checks are not a security boundary against administrators replacing storage.

Standalone and cluster modes use different Kvrocks key encodings. Transitioning an existing standalone dataset to sharding will require a migration; the operator will not toggle cluster mode on an existing volume.

Backups, replication, failover, sharding, volume expansion, image upgrades, external exposure, and cache-specific durability settings are not yet implemented. A single local kind node does not validate storage-provider durability or multi-zone behavior.
