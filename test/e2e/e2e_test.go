//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
)

const namespace = "kvrocks-operator-system"

func TestStandaloneLifecycle(t *testing.T) {
	prepareLifecycleCluster(t)
	db, secret := createAuthenticatedDatabase(t)
	pod := db.Name + "-0"
	volumeUID := field(t, "pvc", db.Name+"-data", ".metadata.uid")

	assertAuthenticationAndDurability(t, pod)
	assertPodReplacementPreservesData(t, db, volumeUID)
	assertDeletedChildrenAreRepaired(t, db)
	assertOperatorRestartPreservesData(t, pod)
	assertPasswordRotationPreservesData(t, db, secret)
	assertRetainedStorageCannotBeAdopted(t, db, volumeUID)
}

func prepareLifecycleCluster(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBECONFIG") == "" || os.Getenv("KIND_CLUSTER") == "" {
		t.Fatal("set KUBECONFIG and KIND_CLUSTER to an isolated kind cluster")
	}
	if got := kubectl(t, nil, "config", "current-context"); got != "kind-"+os.Getenv("KIND_CLUSTER") {
		t.Fatalf("refusing to test against unexpected context %q", got)
	}
	kubectl(t, nil, "apply", "-k", filepath.Join("..", "..", "config", "default"))
	kubectl(t, nil, "-n", namespace, "rollout", "restart", "deployment/kvrocks-operator-controller-manager")
	kubectl(t, nil, "-n", namespace, "rollout", "status", "deployment/kvrocks-operator-controller-manager", "--timeout=180s")

	// Reset only this test's fixtures; retained volumes make this explicit.
	for _, fixture := range []struct{ kind, name string }{
		{"kvrocks", "e2e-database"},
		{"statefulset", "e2e-database"},
		{"pvc", "e2e-database-data"},
		{"configmap", "e2e-database-config"},
		{"service", "e2e-database"},
		{"service", "e2e-database-headless"},
		{"secret", "e2e-auth"},
	} {
		kubectl(t, nil, "-n", namespace, "delete", fixture.kind, fixture.name, "--ignore-not-found", "--timeout=90s")
	}
}

func createAuthenticatedDatabase(t *testing.T) (*kvrocksv1alpha1.Kvrocks, *corev1.Secret) {
	t.Helper()
	db := &kvrocksv1alpha1.Kvrocks{
		TypeMeta:   metav1.TypeMeta{APIVersion: "kvrocks-operator.org/v1alpha1", Kind: "Kvrocks"},
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-database", Namespace: namespace},
		Spec: kvrocksv1alpha1.KvrocksSpec{
			Storage: kvrocksv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
			Auth:    kvrocksv1alpha1.AuthSpec{PasswordSecretRef: kvrocksv1alpha1.PasswordSecretReference{Name: "e2e-auth"}},
		},
	}
	apply(t, db)
	waitFor(t, "missing Secret condition", func() bool {
		return field(t, "kvrocks", db.Name, ".status.conditions[0].reason") == "SecretNotFound"
	})
	if got := kubectl(t, nil, "-n", namespace, "get", "statefulset", db.Name, "--ignore-not-found", "-o", "name"); got != "" {
		t.Fatal("created a workload before authentication was configured")
	}

	password := make([]byte, 32)
	if _, err := rand.Read(password); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-auth", Namespace: namespace},
		Data:       map[string][]byte{"password": []byte(hex.EncodeToString(password) + ` "quoted" \ $ test`)},
	}
	apply(t, secret)
	kubectl(t, nil, "-n", namespace, "wait", "--for=condition=Ready", "kvrocks/"+db.Name, "--timeout=180s")
	return db, secret
}

func assertAuthenticationAndDurability(t *testing.T, pod string) {
	t.Helper()
	if got := databaseCommand(t, pod, "SET", "learning", "persisted"); got != "OK" {
		t.Fatalf("SET failed: %s", got)
	}
	if got := databaseCommand(t, pod, "CONFIG", "GET", "rocksdb.write_options.sync"); !strings.HasSuffix(got, "\nyes") {
		t.Fatalf("synchronous writes are not enabled: %s", got)
	}
	unauthenticated := kubectl(t, nil, "-n", namespace, "exec", pod, "--", "env", "-u", "REDISCLI_AUTH", "redis-cli", "--raw", "-p", "6666", "PING")
	if !strings.Contains(unauthenticated, "NOAUTH") {
		t.Fatalf("unauthenticated connection was not rejected: %s", unauthenticated)
	}
}

func assertPodReplacementPreservesData(t *testing.T, db *kvrocksv1alpha1.Kvrocks, volumeUID string) {
	t.Helper()
	pod := db.Name + "-0"
	podUID := field(t, "pod", pod, ".metadata.uid")

	t.Log("Replacing the Pod and verifying data on the original volume")
	kubectl(t, nil, "-n", namespace, "delete", "pod", pod, "--wait=true", "--timeout=90s")
	waitFor(t, "replacement Pod", func() bool {
		uid := field(t, "pod", pod, ".metadata.uid")
		return uid != "" && uid != podUID
	})
	kubectl(t, nil, "-n", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=180s")
	assertData(t, pod)
	if got := field(t, "pvc", db.Name+"-data", ".metadata.uid"); got != volumeUID {
		t.Fatalf("Pod replacement changed the data volume: %s -> %s", volumeUID, got)
	}
}

func assertDeletedChildrenAreRepaired(t *testing.T, db *kvrocksv1alpha1.Kvrocks) {
	t.Helper()
	t.Log("Repairing deleted Service and ConfigMap resources")
	for _, child := range []struct{ kind, name string }{{"service", db.Name}, {"configmap", db.Name + "-config"}} {
		oldUID := field(t, child.kind, child.name, ".metadata.uid")
		kubectl(t, nil, "-n", namespace, "delete", child.kind, child.name)
		waitFor(t, "recreated "+child.kind, func() bool {
			uid := field(t, child.kind, child.name, ".metadata.uid")
			return uid != "" && uid != oldUID
		})
	}
}

func assertOperatorRestartPreservesData(t *testing.T, pod string) {
	t.Helper()
	t.Log("Restarting the operator and verifying convergence")
	kubectl(t, nil, "-n", namespace, "rollout", "restart", "deployment/kvrocks-operator-controller-manager")
	assertData(t, pod)
	kubectl(t, nil, "-n", namespace, "rollout", "status", "deployment/kvrocks-operator-controller-manager", "--timeout=180s")
}

func assertPasswordRotationPreservesData(t *testing.T, db *kvrocksv1alpha1.Kvrocks, secret *corev1.Secret) {
	t.Helper()
	pod := db.Name + "-0"
	t.Log("Rotating credentials with a controlled standalone restart")
	oldRevision := field(t, "statefulset", db.Name, ".spec.template.metadata.annotations.kvrocks-operator\\.org/secret-revision")
	secret.Data["password"] = append(secret.Data["password"], "-rotated"...)
	apply(t, secret)
	waitFor(t, "Secret revision reconciliation", func() bool {
		return field(t, "statefulset", db.Name, ".spec.template.metadata.annotations.kvrocks-operator\\.org/secret-revision") != oldRevision
	})
	kubectl(t, nil, "-n", namespace, "rollout", "status", "statefulset/"+db.Name, "--timeout=180s")
	assertData(t, pod)
}

func assertRetainedStorageCannotBeAdopted(t *testing.T, db *kvrocksv1alpha1.Kvrocks, volumeUID string) {
	t.Helper()
	t.Log("Retaining storage and rejecting implicit data adoption")
	kubectl(t, nil, "-n", namespace, "delete", "kvrocks", db.Name)
	kubectl(t, nil, "-n", namespace, "wait", "--for=delete", "statefulset/"+db.Name, "--timeout=90s")
	if got := field(t, "pvc", db.Name+"-data", ".metadata.uid"); got != volumeUID {
		t.Fatal("custom resource deletion did not retain its original data volume")
	}
	apply(t, db)
	waitFor(t, "retained volume ownership conflict", func() bool {
		return field(t, "kvrocks", db.Name, ".status.conditions[0].reason") == "VolumeOwnershipConflict"
	})
	if got := kubectl(t, nil, "-n", namespace, "get", "statefulset", db.Name, "--ignore-not-found", "-o", "name"); got != "" {
		t.Fatal("a replacement database was allowed to attach retained data")
	}
	kubectl(t, nil, "-n", namespace, "delete", "kvrocks", db.Name)
}

func databaseCommand(t *testing.T, pod string, arguments ...string) string {
	t.Helper()
	args := []string{"-n", namespace, "exec", pod, "--", "redis-cli", "--raw", "-p", "6666"}
	return kubectl(t, nil, append(args, arguments...)...)
}

func assertData(t *testing.T, pod string) {
	t.Helper()
	if got := databaseCommand(t, pod, "GET", "learning"); got != "persisted" {
		t.Fatalf("persisted value is unavailable: %q", got)
	}
}

func apply(t *testing.T, object any) {
	t.Helper()
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	kubectl(t, data, "apply", "--server-side", "--field-manager=e2e", "-f", "-")
}

func field(t *testing.T, kind, name, path string) string {
	t.Helper()
	return kubectl(t, nil, "-n", namespace, "get", kind, name, "--ignore-not-found", "-o", "jsonpath={"+path+"}")
}

func waitFor(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(time.Second):
		}
	}
	t.Fatal("timed out waiting for " + description)
}

func kubectl(t *testing.T, input []byte, arguments ...string) string {
	t.Helper()
	binary := os.Getenv("KUBECTL")
	if binary == "" {
		binary = "kubectl"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s\n%s", strings.Join(arguments, " "), err, output, stderr.String())
	}
	return strings.TrimSpace(string(output))
}
