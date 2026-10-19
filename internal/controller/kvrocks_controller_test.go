//go:build integration

package controller

import (
	"context"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
)

func TestControllerIntegration(t *testing.T) {
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kvrocksv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := api.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test"}}); err != nil {
		t.Fatal(err)
	}
	reconciler := &KvrocksReconciler{Client: api, APIReader: api}

	createDatabase := func(t *testing.T, name string) *kvrocksv1alpha1.Kvrocks {
		t.Helper()
		db := testDatabase(name)
		db.UID = ""
		if err := api.Create(ctx, db); err != nil {
			t.Fatal(err)
		}
		return db
	}
	reconcileDatabase := func(t *testing.T, db *kvrocksv1alpha1.Kvrocks, reason string) {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)}); err != nil {
			t.Fatal(err)
		}
		if err := api.Get(ctx, client.ObjectKeyFromObject(db), db); err != nil {
			t.Fatal(err)
		}
		condition := meta.FindStatusCondition(db.Status.Conditions, "Ready")
		if condition == nil || condition.Reason != reason || condition.ObservedGeneration != db.Generation {
			t.Fatalf("expected reason %s at generation %d, got %+v", reason, db.Generation, condition)
		}
	}

	t.Run("schema validation and defaults", func(t *testing.T) {
		invalid := []struct {
			name   string
			change func(*kvrocksv1alpha1.Kvrocks)
		}{
			{"zero-storage", func(db *kvrocksv1alpha1.Kvrocks) { db.Spec.Storage.Size = resource.MustParse("0") }},
			{"negative-storage", func(db *kvrocksv1alpha1.Kvrocks) { db.Spec.Storage.Size = resource.MustParse("-1Gi") }},
			{"missing-secret-name", func(db *kvrocksv1alpha1.Kvrocks) { db.Spec.Auth.PasswordSecretRef.Name = "" }},
			{"invalid-name", func(db *kvrocksv1alpha1.Kvrocks) { db.Name = "invalid.name" }},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				db := testDatabase(tt.name)
				db.UID = ""
				tt.change(db)
				if err := api.Create(ctx, db); !apierrors.IsInvalid(err) {
					t.Fatalf("expected API validation failure, got %v", err)
				}
			})
		}
		db := testDatabase("defaults")
		db.UID = ""
		db.Spec.Image = ""
		db.Spec.Auth.PasswordSecretRef.Key = ""
		if err := api.Create(ctx, db); err != nil {
			t.Fatal(err)
		}
		if db.Spec.Image != "apache/kvrocks:2.17.0" || db.Spec.Auth.PasswordSecretRef.Key != "password" {
			t.Fatalf("API defaults were not applied: %+v", db.Spec)
		}
		for _, field := range []string{"image", "storage"} {
			changed := db.DeepCopy()
			if field == "image" {
				changed.Spec.Image = "apache/kvrocks:2.16.0"
			} else {
				changed.Spec.Storage.Size = resource.MustParse("2Gi")
			}
			if err := api.Update(ctx, changed); !apierrors.IsInvalid(err) {
				t.Fatalf("expected immutable %s rejection, got %v", field, err)
			}
		}
	})

	t.Run("convergence repair rotation and lost storage", func(t *testing.T) {
		db := createDatabase(t, "database")
		reconcileDatabase(t, db, "SecretNotFound")
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "test"}, Data: map[string][]byte{"password": []byte("test-only-password")}}
		if err := api.Create(ctx, secret); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "InitializingStorage")
		if err := api.Get(ctx, client.ObjectKeyFromObject(db), &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
			t.Fatalf("workload must wait for persisted storage identity, got %v", err)
		}
		reconcileDatabase(t, db, "VolumePending")
		claim := &corev1.PersistentVolumeClaim{}
		if err := api.Get(ctx, client.ObjectKey{Namespace: "test", Name: "database-data"}, claim); err != nil {
			t.Fatal(err)
		}
		if len(claim.OwnerReferences) != 0 || db.Status.VolumeClaimUID != claim.UID {
			t.Fatal("claim retention and identity were not recorded")
		}
		workload := &appsv1.StatefulSet{}
		if err := api.Get(ctx, client.ObjectKeyFromObject(db), workload); err != nil {
			t.Fatal("workload must exist before a WaitForFirstConsumer claim binds:", err)
		}
		claim.Status.Phase = corev1.ClaimBound
		if err := api.Status().Update(ctx, claim); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "InstanceNotReady")
		if err := api.Get(ctx, client.ObjectKeyFromObject(db), workload); err != nil {
			t.Fatal(err)
		}
		workload.Status = appsv1.StatefulSetStatus{ObservedGeneration: workload.Generation, Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1, CurrentRevision: "revision", UpdateRevision: "revision"}
		if err := api.Status().Update(ctx, workload); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "InstanceReady")
		for range 2 {
			reconcileDatabase(t, db, "InstanceReady")
		}
		versions := resourceVersions(t, ctx, api, db)
		reconcileDatabase(t, db, "InstanceReady")
		after := resourceVersions(t, ctx, api, db)
		for key, version := range versions {
			if after[key] != version {
				t.Errorf("stable reconciliation wrote %s: %s -> %s", key, version, after[key])
			}
		}
		service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: db.Name, Namespace: db.Namespace}}
		if err := api.Delete(ctx, service); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "InstanceReady")
		if err := api.Get(ctx, client.ObjectKeyFromObject(service), service); err != nil {
			t.Fatal("deleted Service was not repaired:", err)
		}
		secret.Data["password"] = []byte("test-only-rotated-password")
		if err := api.Update(ctx, secret); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "InstanceNotReady")
		if err := api.Get(ctx, client.ObjectKeyFromObject(db), workload); err != nil {
			t.Fatal(err)
		}
		if workload.Spec.Template.Annotations["kvrocks-operator.org/secret-revision"] != string(secret.UID)+"/"+secret.ResourceVersion {
			t.Fatal("password rotation did not update the Pod template")
		}
		if err := api.Delete(ctx, claim); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "VolumeUnavailable")
		// Envtest has PVC protection admission but no protection controller.
		if err := api.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
			t.Fatal(err)
		}
		claim.Finalizers = nil
		if err := api.Update(ctx, claim); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "VolumeMissing")
		if err := api.Get(ctx, client.ObjectKeyFromObject(claim), claim); !apierrors.IsNotFound(err) {
			t.Fatalf("missing data claim must not be recreated: %v", err)
		}
	})

	t.Run("retained storage is not adopted", func(t *testing.T) {
		db := createDatabase(t, "retained")
		reconcileDatabase(t, db, "InitializingStorage")
		reconcileDatabase(t, db, "VolumePending")
		originalUID := db.UID
		if err := api.Delete(ctx, db); err != nil {
			t.Fatal(err)
		}
		db = createDatabase(t, "retained")
		if db.UID == originalUID {
			t.Fatal("recreated database unexpectedly retained its UID")
		}
		reconcileDatabase(t, db, "VolumeOwnershipConflict")
	})

	t.Run("unowned child is not overwritten", func(t *testing.T) {
		db := createDatabase(t, "collision")
		config := configurationFor(db)
		config.Data = map[string]string{"user-owned": "preserve"}
		if err := api.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		reconcileDatabase(t, db, "InitializingStorage")
		reconcileDatabase(t, db, "ResourceOwnershipConflict")
		if err := api.Get(ctx, client.ObjectKeyFromObject(config), config); err != nil {
			t.Fatal(err)
		}
		if config.Data["user-owned"] != "preserve" || len(config.OwnerReferences) != 0 {
			t.Fatal("unowned ConfigMap was modified")
		}
	})
}

func resourceVersions(t *testing.T, ctx context.Context, api client.Client, db *kvrocksv1alpha1.Kvrocks) map[string]string {
	t.Helper()
	objects := []client.Object{db.DeepCopy(), configurationFor(db), serviceFor(db, false), serviceFor(db, true), volumeClaimFor(db), &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: db.Name, Namespace: db.Namespace}}}
	versions := make(map[string]string, len(objects))
	for _, object := range objects {
		if err := api.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
		versions[object.GetName()+"/"+object.GetObjectKind().GroupVersionKind().Kind] = object.GetResourceVersion()
	}
	return versions
}
