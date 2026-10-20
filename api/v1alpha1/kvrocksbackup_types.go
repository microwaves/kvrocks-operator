/*
Copyright 2026 Stephano Zanzin Ferreira.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="backup spec is immutable; create a new KvrocksBackup for another backup"
type KvrocksBackupSpec struct {
	KvrocksRef  KvrocksReference  `json:"kvrocksRef"`
	Destination BackupDestination `json:"destination"`
}

type KvrocksReference struct {
	// Name identifies a Kvrocks resource in the backup's namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	// +kubebuilder:validation:Pattern="^[a-z]([a-z0-9-]*[a-z0-9])?$"
	Name string `json:"name"`
}

type BackupDestination struct {
	S3 S3BackupDestination `json:"s3"`
}

type S3BackupDestination struct {
	S3BucketSpec `json:",inline"`

	// Prefix is the parent object key prefix; source and backup UIDs are appended.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Prefix string `json:"prefix,omitempty"`
}

type S3BucketSpec struct {
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
	Bucket string `json:"bucket"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z0-9]+(-[a-z0-9]+)*$"
	Region string `json:"region"`

	CredentialsSecretRef BackupCredentialsSecretReference `json:"credentialsSecretRef"`
}

type BackupCredentialsSecretReference struct {
	// Name identifies a Secret in the requesting resource's namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
	Name string `json:"name"`
}

type KvrocksBackupStatus struct {
	// SourceUID and VolumeClaimUID bind the request before a worker starts.
	// +optional
	SourceUID types.UID `json:"sourceUID,omitempty"`
	// +optional
	VolumeClaimName string `json:"volumeClaimName,omitempty"`
	// +optional
	VolumeClaimUID types.UID `json:"volumeClaimUID,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Artifact *BackupArtifactStatus `json:"artifact,omitempty"`

	// Complete means the remote artifact was committed and verified.
	// Failed means execution ended without a completed artifact.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type BackupArtifactStatus struct {
	// ManifestKey locates the committed manifest in spec.destination.s3.bucket.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	ManifestKey string `json:"manifestKey"`
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern="^[a-f0-9]{64}$"
	ManifestSHA256 string `json:"manifestSHA256"`
	// SizeBytes is the total size of the checkpoint files.
	// +kubebuilder:validation:Minimum=0
	SizeBytes int64 `json:"sizeBytes"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=kvrbackup
// +kubebuilder:printcolumn:name="Complete",type=string,JSONPath=".status.conditions[?(@.type=='Complete')].status"
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=".spec.kvrocksRef.name"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type KvrocksBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KvrocksBackupSpec `json:"spec"`
	// +optional
	Status KvrocksBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KvrocksBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KvrocksBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &KvrocksBackup{}, &KvrocksBackupList{})
		return nil
	})
}
