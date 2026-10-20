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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// KvrocksSpec describes the standalone database and its initialization source.
// +kubebuilder:validation:XValidation:rule="has(self.bootstrap) == has(oldSelf.bootstrap)",message="bootstrap cannot be added or removed after creation"
type KvrocksSpec struct {
	// Image is immutable until database upgrade workflows are implemented.
	// +kubebuilder:default="apache/kvrocks:2.17.0"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="image changes require a supported upgrade workflow"
	// +optional
	Image string `json:"image,omitempty"`

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage is immutable; expansion and adoption are not supported yet"
	Storage StorageSpec `json:"storage"`
	Auth    AuthSpec    `json:"auth"`

	// Bootstrap initializes a new volume from a backup. Omit it for an empty database.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="bootstrap is immutable; create a new Kvrocks resource to restore another backup"
	// +optional
	Bootstrap *BootstrapSpec `json:"bootstrap,omitempty"`

	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

type BootstrapSpec struct {
	Restore RestoreSpec `json:"restore"`
}

type RestoreSpec struct {
	S3 S3RestoreSource `json:"s3"`
	// ManifestSHA256 verifies the exact manifest bytes independently of object storage.
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern="^[a-f0-9]{64}$"
	ManifestSHA256 string `json:"manifestSHA256"`
}

type S3RestoreSource struct {
	S3BucketSpec `json:",inline"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	ManifestKey string `json:"manifestKey"`
}

type StorageSpec struct {
	// Size is the requested persistent volume capacity.
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).isGreaterThan(quantity('0'))",message="storage size must be positive"
	Size resource.Quantity `json:"size"`

	// StorageClassName defaults to the cluster's default StorageClass when omitted.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

type AuthSpec struct {
	PasswordSecretRef PasswordSecretReference `json:"passwordSecretRef"`
}

type PasswordSecretReference struct {
	// Name identifies a Secret in the database's namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:default=password
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Key string `json:"key,omitempty"`
}

type KvrocksStatus struct {
	// VolumeClaimName and VolumeClaimUID identify the original data volume.
	// A missing or replaced volume requires explicit recovery, never fresh bootstrap.
	// +optional
	VolumeClaimName string `json:"volumeClaimName,omitempty"`
	// +optional
	VolumeClaimUID types.UID `json:"volumeClaimUID,omitempty"`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=kvr
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=".status.endpoint"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 50 && self.metadata.name.matches('^[a-z]([a-z0-9-]*[a-z0-9])?$')",message="name must be a DNS label starting with a letter and at most 50 characters"
type Kvrocks struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KvrocksSpec `json:"spec"`
	// +optional
	Status KvrocksStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KvrocksList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Kvrocks `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Kvrocks{}, &KvrocksList{})
		return nil
	})
}
