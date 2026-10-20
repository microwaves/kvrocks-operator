//go:build integration

package controller

import (
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
)

func TestBackupAPIIntegration(t *testing.T) {
	api := startTestAPI(t)
	cases := []struct {
		name string
		run  func(*testing.T, client.Client)
	}{
		{"backup admission", testBackupAdmission},
		{"backup spec is immutable", testBackupImmutability},
		{"backup status subresource", testBackupStatus},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) { test.run(t, api) })
	}
}

func testBackup(name string) *kvrocksv1alpha1.KvrocksBackup {
	return &kvrocksv1alpha1.KvrocksBackup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kvrocksv1alpha1.SchemeGroupVersion.String(),
			Kind:       "KvrocksBackup",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Spec: kvrocksv1alpha1.KvrocksBackupSpec{
			KvrocksRef: kvrocksv1alpha1.KvrocksReference{Name: "orders"},
			Destination: kvrocksv1alpha1.BackupDestination{
				S3: kvrocksv1alpha1.S3BackupDestination{
					S3BucketSpec: testBackupBucket(),
					Prefix:       "orders",
				},
			},
		},
	}
}

func testBackupBucket() kvrocksv1alpha1.S3BucketSpec {
	return kvrocksv1alpha1.S3BucketSpec{
		Bucket:               "test-backups",
		Region:               "eu-west-1",
		CredentialsSecretRef: kvrocksv1alpha1.BackupCredentialsSecretReference{Name: "backup-credentials"},
	}
}

func testBackupAdmission(t *testing.T, api client.Client) {
	invalid := []struct {
		name  string
		field string
		value any
	}{
		{"missing-spec", "spec", nil},
		{"missing-source", "spec.kvrocksRef", nil},
		{"missing-source-name", "spec.kvrocksRef.name", nil},
		{"empty-source-name", "spec.kvrocksRef.name", ""},
		{"invalid-source-name", "spec.kvrocksRef.name", "orders.prod"},
		{"long-source-name", "spec.kvrocksRef.name", strings.Repeat("a", 51)},
		{"missing-destination", "spec.destination", nil},
		{"missing-s3", "spec.destination.s3", nil},
		{"missing-bucket", "spec.destination.s3.bucket", nil},
		{"short-bucket", "spec.destination.s3.bucket", "ab"},
		{"long-bucket", "spec.destination.s3.bucket", strings.Repeat("a", 64)},
		{"invalid-bucket", "spec.destination.s3.bucket", "INVALID_BUCKET"},
		{"missing-region", "spec.destination.s3.region", nil},
		{"empty-region", "spec.destination.s3.region", ""},
		{"invalid-region", "spec.destination.s3.region", "eu west 1"},
		{"missing-secret", "spec.destination.s3.credentialsSecretRef", nil},
		{"missing-secret-name", "spec.destination.s3.credentialsSecretRef.name", nil},
		{"invalid-secret-name", "spec.destination.s3.credentialsSecretRef.name", "another/secret"},
		{"long-prefix", "spec.destination.s3.prefix", strings.Repeat("a", 513)},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			object := admissionObject(t, testBackup("backup-"+test.name), "KvrocksBackup")
			setAdmissionField(t, object, test.field, test.value)
			expectInvalidField(t, api.Create(t.Context(), object), test.field)
		})
	}

	// Admission validates shape; source and credential availability belongs to reconciliation.
	backup := testBackup("backup-without-dependencies")
	backup.Spec.Destination.S3.Prefix = ""
	if err := api.Create(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if backup.UID == "" || backup.Generation != 1 {
		t.Fatalf("expected a persisted backup request, got UID %q, generation %d", backup.UID, backup.Generation)
	}
}

func testBackupImmutability(t *testing.T, api client.Client) {
	backup := testBackup("immutable-backup")
	if err := api.Create(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	changes := []struct {
		name   string
		change func(*kvrocksv1alpha1.KvrocksBackupSpec)
	}{
		{"source", func(spec *kvrocksv1alpha1.KvrocksBackupSpec) { spec.KvrocksRef.Name = "another" }},
		{"bucket", func(spec *kvrocksv1alpha1.KvrocksBackupSpec) { spec.Destination.S3.Bucket = "another-bucket" }},
		{"region", func(spec *kvrocksv1alpha1.KvrocksBackupSpec) { spec.Destination.S3.Region = "us-east-1" }},
		{"prefix", func(spec *kvrocksv1alpha1.KvrocksBackupSpec) { spec.Destination.S3.Prefix = "another" }},
		{"remove-prefix", func(spec *kvrocksv1alpha1.KvrocksBackupSpec) { spec.Destination.S3.Prefix = "" }},
		{"secret", func(spec *kvrocksv1alpha1.KvrocksBackupSpec) {
			spec.Destination.S3.CredentialsSecretRef.Name = "another-secret"
		}},
	}
	for _, test := range changes {
		t.Run(test.name, func(t *testing.T) {
			changed := backup.DeepCopy()
			test.change(&changed.Spec)
			err := api.Update(t.Context(), changed)
			expectInvalidField(t, err, "spec")
			if !strings.Contains(err.Error(), "backup spec is immutable") {
				t.Fatalf("expected immutable backup explanation, got %v", err)
			}
		})
	}

	backup.Labels = map[string]string{"purpose": "recovery-drill"}
	if err := api.Update(t.Context(), backup); err != nil {
		t.Fatal("metadata changes must remain possible:", err)
	}
	if backup.Generation != 1 {
		t.Fatalf("metadata update changed spec generation to %d", backup.Generation)
	}

	withoutPrefix := testBackup("immutable-absent-prefix")
	withoutPrefix.Spec.Destination.S3.Prefix = ""
	if err := api.Create(t.Context(), withoutPrefix); err != nil {
		t.Fatal(err)
	}
	withoutPrefix.Spec.Destination.S3.Prefix = "added"
	expectInvalidField(t, api.Update(t.Context(), withoutPrefix), "spec")
}

func testBackupStatus(t *testing.T, api client.Client) {
	ctx := t.Context()
	backup := testBackup("backup-status")
	backup.Status.SourceUID = "must-not-be-accepted-on-create"
	if err := api.Create(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.SourceUID != "" {
		t.Fatal("create accepted user-supplied status")
	}

	now := metav1.Now()
	backup.Status = kvrocksv1alpha1.KvrocksBackupStatus{
		SourceUID:       "source-uid",
		VolumeClaimName: "orders-data",
		VolumeClaimUID:  "volume-uid",
		StartedAt:       &now,
		CompletedAt:     &now,
		Artifact: &kvrocksv1alpha1.BackupArtifactStatus{
			ManifestKey:    "orders/source-uid/backup-uid/manifest.json",
			ManifestSHA256: strings.Repeat("a", 64),
			SizeBytes:      1024,
		},
		Conditions: []metav1.Condition{{
			Type: "Complete", Status: metav1.ConditionTrue, Reason: "ArtifactCommitted",
			Message:            "The remote artifact was committed and verified",
			ObservedGeneration: backup.Generation, LastTransitionTime: now,
		}},
	}
	if err := api.Status().Update(ctx, backup); err != nil {
		t.Fatal(err)
	}
	stored := &kvrocksv1alpha1.KvrocksBackup{}
	if err := api.Get(ctx, client.ObjectKeyFromObject(backup), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Artifact == nil || *stored.Status.Artifact != *backup.Status.Artifact ||
		stored.Status.SourceUID != "source-uid" || stored.Status.VolumeClaimUID != "volume-uid" ||
		!meta.IsStatusConditionTrue(stored.Status.Conditions, "Complete") || stored.Generation != 1 {
		t.Fatalf("status was not preserved independently of spec: %+v", stored)
	}
	assertBackupStatusIsolation(t, api, stored)
	assertBackupStatusValidation(t, api, stored)
}

func assertBackupStatusIsolation(t *testing.T, api client.Client, backup *kvrocksv1alpha1.KvrocksBackup) {
	t.Helper()
	ctx := t.Context()
	changed := backup.DeepCopy()
	changed.Status.SourceUID = "must-not-be-accepted-on-main-update"
	if err := api.Update(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(backup), backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.SourceUID != "source-uid" {
		t.Fatal("main endpoint changed status")
	}

	changed = backup.DeepCopy()
	changed.Spec.Destination.S3.Bucket = "must-not-change"
	if err := api.Status().Update(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(backup), backup); err != nil {
		t.Fatal(err)
	}
	if backup.Spec.Destination.S3.Bucket != "test-backups" {
		t.Fatal("status endpoint changed spec")
	}
}

func assertBackupStatusValidation(t *testing.T, api client.Client, backup *kvrocksv1alpha1.KvrocksBackup) {
	t.Helper()
	invalid := []struct {
		name   string
		field  string
		change func(*kvrocksv1alpha1.KvrocksBackupStatus)
	}{
		{"empty-key", "status.artifact.manifestKey", func(status *kvrocksv1alpha1.KvrocksBackupStatus) {
			status.Artifact.ManifestKey = ""
		}},
		{"invalid-digest", "status.artifact.manifestSHA256", func(status *kvrocksv1alpha1.KvrocksBackupStatus) {
			status.Artifact.ManifestSHA256 = "invalid"
		}},
		{"negative-size", "status.artifact.sizeBytes", func(status *kvrocksv1alpha1.KvrocksBackupStatus) {
			status.Artifact.SizeBytes = -1
		}},
		{"duplicate-condition", "status.conditions", func(status *kvrocksv1alpha1.KvrocksBackupStatus) {
			status.Conditions = append(status.Conditions, status.Conditions[0])
		}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			changed := backup.DeepCopy()
			test.change(&changed.Status)
			expectInvalidField(t, api.Status().Update(t.Context(), changed), test.field)
		})
	}
}

func admissionObject(t *testing.T, object client.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		t.Fatal(err)
	}
	result := &unstructured.Unstructured{Object: content}
	result.SetAPIVersion(kvrocksv1alpha1.SchemeGroupVersion.String())
	result.SetKind(kind)
	return result
}

func setAdmissionField(t *testing.T, object *unstructured.Unstructured, field string, value any) {
	t.Helper()
	path := strings.Split(field, ".")
	if value == nil {
		unstructured.RemoveNestedField(object.Object, path...)
		return
	}
	if err := unstructured.SetNestedField(object.Object, value, path...); err != nil {
		t.Fatal(err)
	}
}

func expectInvalidField(t *testing.T, err error, field string) {
	t.Helper()
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), field) {
		t.Fatalf("expected API validation failure for %s, got %v", field, err)
	}
}
