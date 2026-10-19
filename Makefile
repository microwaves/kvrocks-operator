SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := build

CONTROLLER_GEN := ./bin/controller-gen-v0.22.0
ENVTEST := ./bin/setup-envtest-v0.25.2
KUSTOMIZE := ./bin/kustomize-v5.8.1
ENVTEST_K8S_VERSION ?= 1.37.x!
IMG ?= kvrocks-operator:dev

.PHONY: generate manifests fmt vet test test-unit test-integration test-e2e build run docker-build installer

generate: $(CONTROLLER_GEN)
	"$(CONTROLLER_GEN)" object:headerFile=hack/boilerplate.go.txt,year=2026 paths="./..."

manifests: $(CONTROLLER_GEN)
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd paths="./..." output:crd:artifacts:config=config/crd/bases

fmt:
	gofmt -w api cmd internal test

vet:
	go vet ./...

test: test-unit test-integration

test-unit: generate manifests
	go test -race ./...

test-integration: generate manifests $(ENVTEST)
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use '$(ENVTEST_K8S_VERSION)' --bin-dir "$(CURDIR)/bin/envtest" -p path)" go test -race -tags=integration ./internal/controller/... -count=1

test-e2e: generate manifests
	go test -tags=e2e ./test/e2e/... -v -count=1 -timeout=15m

build: generate manifests
	go build -o bin/manager ./cmd

run:
	@test -n "$${KUBECONFIG:-}" -a -n "$${WATCH_NAMESPACE:-}" || { echo 'Set KUBECONFIG and WATCH_NAMESPACE explicitly' >&2; exit 1; }
	go run ./cmd

docker-build:
	docker build -t "$(IMG)" .

installer: generate manifests $(KUSTOMIZE)
	mkdir -p dist
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

$(CONTROLLER_GEN):
	mkdir -p bin
	GOBIN="$(CURDIR)/bin" go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.22.0
	mv bin/controller-gen "$(CONTROLLER_GEN)"

$(ENVTEST):
	mkdir -p bin
	GOBIN="$(CURDIR)/bin" go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.25.2
	mv bin/setup-envtest "$(ENVTEST)"

$(KUSTOMIZE):
	mkdir -p bin
	GOBIN="$(CURDIR)/bin" go install sigs.k8s.io/kustomize/kustomize/v5@v5.8.1
	mv bin/kustomize "$(KUSTOMIZE)"
