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

The checkpoint protocol has its own real-engine test:

```bash
make test-checkpoint
```

This command creates a temporary kind 1.37.0 cluster and a dedicated kubeconfig, runs the Go tests with Kvrocks 2.17.0 on a PVC as user 999, and removes the cluster on exit. It requires Docker, kind, and kubectl on `PATH`. It does not use the current Kubernetes context. Unit tests for the protocol also run in `make test-unit` with the race detector.

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

Managed backup/restore resources, object-store transfer, replication, failover, sharding, volume expansion, image upgrades, external exposure, and cache-specific durability settings are not yet implemented. A single local kind node does not validate storage-provider durability or multi-zone behavior.

## Checkpoint protocol and local artifact format

`internal/backup` implements the first part of backup/restore: capture a physical checkpoint from Kvrocks 2.17.0, publish a verifiable local artifact, and materialize its files into a fresh offline directory. It is an internal library, not yet a Kubernetes backup API or an object-store backup service.

Read `checkpoint.go` first. `Capturer.Capture` validates the request, obtains a filesystem lock, observes an existing operation, and either returns its verified artifact or starts one save. The `Database` interface describes the three operations the caller needs: inspect, set the backup directory, and request `BGSAVE`. `redis.go` implements them on a dedicated connection with automatic command retries disabled. After a connection failure, create a new adapter to resume observation; never reset and reuse a broken session to send another save.

The Kvrocks version is an explicit compatibility constraint, named `supportedKvrocksVersion` in the checkpoint implementation. Only 2.17.0 has passed the real-engine capture and restore checks. Supporting another release requires checking its checkpoint behavior and passing those tests before changing this constraint. The artifact format identifier is separately versioned; it identifies the manifest schema and does not follow the Kvrocks release number.

The database must run in standalone mode with synchronous writes, WAL and namespace replication enabled. The worker requires `max-backup-to-keep 1`, `max-backup-keep-hours 0`, `purge-backup-on-fullsync no`, and an empty native backup schedule. It validates these settings rather than silently changing them. `db-name` must match the request's namespace/name. These settings are supplied by the isolated test fixture; integrating them with managed backup Jobs is subsequent work.

The shared database volume contains:

```text
operator-backup/
  .lock
  active.json                 # present while an operation is unresolved
  <request-id>/
    intent.json               # source IDs, server PID, destination, start time
    incoming.tmp/             # Kvrocks's incomplete checkpoint, when present
    incoming/                 # published by Kvrocks after checkpoint creation
    checkpoint/               # incoming is renamed here before hashing
    captured.json             # stable upper bound on capture time
    manifest.json             # published last; marks a completed local artifact
```

`incoming` and `checkpoint` are successive names for the same directory. The worker never opens the live RocksDB directory. The same-filesystem rename protects a completed checkpoint from later saves and purges of Kvrocks's configured backup path. The transfer unit is `manifest.json` plus `checkpoint/`; journals are local coordination records. Local artifacts remain on the volume until explicitly removed. Retention and orphan cleanup are not implemented in this slice.

The filesystem lock serializes live workers. The durable active record prevents another request from changing the backup directory after the lock holder dies. The worker records intent before sending `BGSAVE` and sends it at most once for that request. A replacement worker observes the original destination and process information. It never treats `BGSAVE` acceptance or a changed `LASTSAVE` timestamp as proof of completion.

| Interruption | Behavior |
| --- | --- |
| Worker dies after save acceptance | Resume observation of the same checkpoint; do not send another save. |
| Worker dies after checkpoint rename | Hash and publish the protected files without contacting Kvrocks. |
| Save response is lost and no completed checkpoint exists | Return `ErrIndeterminate`; preserve the active record and block later requests. |
| Kvrocks restarts before protection | Preserve the unresolved operation and block later requests. PID changes can reject a result but cannot authorize releasing the fence. |
| Manifest already exists | Verify and return the same artifact; missing or modified files are errors. |

The ambiguous pre-send case is deliberately conservative. The journal cannot distinguish a crash immediately before sending the command from a lost response. Retry observation with the same request. Kvrocks 2.17.0 does not expose Redis's unique `run_id`; PID and uptime cannot prove a unique process incarnation. Therefore this slice provides no automatic abandonment of an unresolved operation, even following a restart. A future lifecycle-aware recovery mechanism must prove that the old server cannot execute the pending command before releasing the fence. Do not clear journals or issue competing administrative backup/configuration commands while a managed operation is active.

`artifact.go` defines the format `kvrocks-operator.org/checkpoint/v1alpha1`. Its JSON manifest contains the request and source resource/PVC UIDs, Kvrocks version, encoding, diagnostic server PID, capture time bounds, and a sorted list of file names, byte lengths, and SHA-256 checksums. The PID is not a unique identity or a fencing token. The manifest checksum is computed over its exact bytes and returned separately. It must be preserved by the eventual caller outside the source volume for disaster recovery.

All checkpoint files are included, including WAL; copying only SST files can lose unflushed data. Files are flat, regular files. Verification rejects unsafe paths, symlinks, duplicates, unknown formats/versions/fields, extra or missing files, and mismatched sizes or hashes. `CURRENT` must reference a MANIFEST in that checkpoint. JSON metadata is limited to 16 MiB. Administrator runtime configuration is excluded; database-persisted namespace tokens are included in the checkpoint and should be protected as sensitive data.

Files and directories are synchronized before the manifest is published through an atomic, non-replacing hard link. The implementation requires a POSIX filesystem with working `flock`, hard links, same-filesystem rename, and synchronization semantics. This is local worker coordination, not fencing against a storage split-brain or an administrator changing the files.

`Verify` checks integrity against an independently supplied manifest checksum. It does not prove that RocksDB can open the database. `Materialize` verifies and copies the checkpoint into an exclusively created offline directory, checking file hashes during copying as well. It never overwrites an existing directory; an interrupted copy remains for diagnosis. Kubernetes bootstrap, durable restore-completion markers, and retrying interrupted restores belong to the next slice.

The real-engine test opens the materialized checkpoint with another Kvrocks 2.17.0 process. It checks strings, hashes, lists, sets, sorted sets, streams, namespace data and tokens, unchanged expiration deadlines, writes before/after capture, and atomic paired writes during capture. It also races two worker processes, crashes workers, restarts Kvrocks, and verifies that later backups and native purge operations cannot change the protected artifact. Object-store publication, other versions/storage drivers, and power-loss durability remain separate acceptance work.

The protocol follows the pinned [checkpoint implementation](https://github.com/apache/kvrocks/blob/v2.17.0/src/storage/storage.cc) and [asynchronous save implementation](https://github.com/apache/kvrocks/blob/v2.17.0/src/server/server.cc). Capture timestamps bound the operation; they are not exact sequence timestamps or a point-in-time recovery capability.
