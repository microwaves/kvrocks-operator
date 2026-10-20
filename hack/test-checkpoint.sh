#!/usr/bin/env bash
set -euo pipefail

readonly cluster="kvrocks-checkpoint-${RANDOM}-${RANDOM}"
test_dir="$(mktemp -d)"
readonly test_dir
readonly kubeconfig="${test_dir}/kubeconfig"
readonly image="kvrocks-checkpoint-test:dev"
architecture="$(go env GOARCH)"
readonly architecture
cluster_created=false

cleanup() {
  if [[ "${cluster_created}" == true ]]; then
    kind delete cluster --name "${cluster}"
  fi
  rm -rf "${test_dir}"
}
trap cleanup EXIT

wait_for_checkpoint() {
  local attempt conditions
  for ((attempt = 0; attempt < 360; attempt++)); do
    if ! conditions="$(kubectl --kubeconfig "${kubeconfig}" get job checkpoint-test -o jsonpath='{range .status.conditions[*]}{.type}={.status}{" "}{end}')"; then
      return 1
    fi
    if [[ "${conditions}" == *Complete=True* ]]; then
      return 0
    fi
    if [[ "${conditions}" == *Failed=True* ]]; then
      return 1
    fi
    sleep 1
  done
  printf 'Timed out waiting for checkpoint-test Job to finish\n' >&2
  return 1
}

CGO_ENABLED=0 GOOS=linux GOARCH="${architecture}" go test -c -tags=checkpoint -o "${test_dir}/checkpoint.test" ./internal/backup
docker build -f test/checkpoint/Dockerfile -t "${image}" "${test_dir}"
docker save --platform "linux/${architecture}" -o "${test_dir}/image.tar" "${image}"
cluster_created=true
kind create cluster --name "${cluster}" --image kindest/node:v1.37.0 --kubeconfig "${kubeconfig}"
kind load image-archive "${test_dir}/image.tar" --name "${cluster}"
kubectl --kubeconfig "${kubeconfig}" apply -f - <<'YAML'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: checkpoint-test
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: batch/v1
kind: Job
metadata:
  name: checkpoint-test
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 360
  template:
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 999
        runAsGroup: 999
        fsGroup: 999
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: tests
          image: kvrocks-checkpoint-test:dev
          imagePullPolicy: Never
          env:
            - name: TMPDIR
              value: /data
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: [ALL]
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: checkpoint-test
YAML
passed=false
if wait_for_checkpoint; then
  passed=true
fi
kubectl --kubeconfig "${kubeconfig}" logs job/checkpoint-test --pod-running-timeout=10s
if [[ "${passed}" != true ]]; then
  kubectl --kubeconfig "${kubeconfig}" describe job/checkpoint-test
  kubectl --kubeconfig "${kubeconfig}" describe pod -l job-name=checkpoint-test
  exit 1
fi
