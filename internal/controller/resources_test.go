package controller

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
)

func testDatabase(name string) *kvrocksv1alpha1.Kvrocks {
	return &kvrocksv1alpha1.Kvrocks{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test", UID: "database-uid"},
		Spec: kvrocksv1alpha1.KvrocksSpec{
			Image:   "apache/kvrocks:2.17.0",
			Storage: kvrocksv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
			Auth:    kvrocksv1alpha1.AuthSpec{PasswordSecretRef: kvrocksv1alpha1.PasswordSecretReference{Name: "auth", Key: "password"}},
		},
	}
}

func TestValidatePassword(t *testing.T) {
	tests := []struct {
		name     string
		password []byte
		valid    bool
	}{
		{"missing", nil, false},
		{"empty", []byte{}, false},
		{"newline injection", []byte("password\nrequirepass other"), false},
		{"carriage return", []byte("password\r"), false},
		{"nul", []byte("pass\x00word"), false},
		{"invalid UTF-8", []byte{0xff}, false},
		{"oversized", []byte(strings.Repeat("a", 513)), false},
		{"quoted and escaped", []byte("test only: \"quoted\" \\ password"), true},
		{"unicode", []byte("test-only-密碼"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validatePassword(tt.password); (err == nil) != tt.valid {
				t.Fatalf("valid=%t, got error %v", tt.valid, err)
			}
		})
	}
}

func TestDatabaseResourcesPreserveDataAndCredentials(t *testing.T) {
	db := testDatabase("example")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{UID: "secret-uid", ResourceVersion: "42"},
		Data:       map[string][]byte{"password": []byte("test-only-sensitive-value")},
	}
	config := configurationFor(db)
	for _, setting := range []string{"rocksdb.write_options.sync yes", "rocksdb.write_options.disable_wal no", "repl-namespace-enabled yes"} {
		if !strings.Contains(config.Data["kvrocks.conf"], setting) {
			t.Errorf("configuration is missing durability setting %q", setting)
		}
	}
	claim := volumeClaimFor(db)
	if len(claim.OwnerReferences) != 0 || claim.Labels[ownerLabel] != string(db.UID) {
		t.Fatal("data claim must retain data and identify the originating database")
	}
	sts := statefulSetFor(db, secret)
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatal("StatefulSet must not recreate missing data claims")
	}
	pod := sts.Spec.Template.Spec
	if *pod.AutomountServiceAccountToken || !*pod.SecurityContext.RunAsNonRoot {
		t.Fatal("database must run without an API token and without root privileges")
	}
	if !*pod.Containers[0].SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("database root filesystem must be read-only")
	}
	for _, object := range []any{config, sts} {
		data, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), string(secret.Data["password"])) {
			t.Fatal("resource contains the Secret's value")
		}
	}
	previous := sts.Spec.Template.Annotations["kvrocks-operator.org/secret-revision"]
	secret.ResourceVersion = "43"
	updated := statefulSetFor(db, secret)
	if updated.Spec.Template.Annotations["kvrocks-operator.org/secret-revision"] == previous {
		t.Fatal("Secret update must change the Pod template to restart with new credentials")
	}
	if serviceFor(db, false).Spec.Selector[ownerLabel] != string(db.UID) {
		t.Fatal("client Service must select only this database's Pods")
	}
}

func TestStaleCacheCannotReplaceEstablishedStorage(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kvrocksv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	persisted := testDatabase("established")
	persisted.Status.VolumeClaimName = "established-data"
	persisted.Status.VolumeClaimUID = "original-volume"
	cached := persisted.DeepCopy()
	cached.Status = kvrocksv1alpha1.KvrocksStatus{}
	api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(persisted).Build()
	reconciler := &KvrocksReconciler{Client: api, APIReader: api}
	if _, err := reconciler.reconcileStorage(t.Context(), cached); err != nil {
		var waiting *blockedError
		if !errors.As(err, &waiting) || waiting.reason != "VolumeMissing" {
			t.Fatalf("expected VolumeMissing, got %v", err)
		}
	} else {
		t.Fatal("stale cached status allowed a fresh bootstrap")
	}
	claim := volumeClaimFor(persisted)
	if err := api.Get(t.Context(), client.ObjectKeyFromObject(claim), claim); !apierrors.IsNotFound(err) {
		t.Fatalf("missing established storage was recreated: %v", err)
	}
}
