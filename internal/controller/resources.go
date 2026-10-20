package controller

import (
	"crypto/sha256"
	_ "embed"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
)

const (
	ownerLabel    = "kvrocks-operator.org/owner-uid"
	databaseLabel = "kvrocks-operator.org/database"
	secretIndex   = "spec.auth.passwordSecretRef.name"
	databasePort  = 6666
)

//go:embed startup.sh
var startupScript string

func labelsFor(db *kvrocksv1alpha1.Kvrocks) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "kvrocks",
		"app.kubernetes.io/instance":   db.Name,
		"app.kubernetes.io/managed-by": "kvrocks-operator",
		databaseLabel:                  db.Name,
		ownerLabel:                     string(db.UID),
	}
}

func configurationFor(db *kvrocksv1alpha1.Kvrocks) *corev1.ConfigMap {
	configuration := fmt.Sprintf(`bind 0.0.0.0
port 6666
daemonize no
dir /var/lib/kvrocks
pidfile /run/kvrocks/kvrocks.pid
log-dir stdout
db-name %s/%s
cluster-enabled no
repl-namespace-enabled yes
rocksdb.write_options.sync yes
rocksdb.write_options.disable_wal no
`, db.Namespace, db.Name)
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: db.Name + "-config", Namespace: db.Namespace, Labels: labelsFor(db)},
		Data:       map[string]string{"kvrocks.conf": configuration, "startup.sh": startupScript},
	}
}

func volumeClaimFor(db *kvrocksv1alpha1.Kvrocks) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: db.Name + "-data", Namespace: db.Namespace, Labels: labelsFor(db)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: db.Spec.Storage.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: db.Spec.Storage.Size},
			},
		},
	}
}

func serviceFor(db *kvrocksv1alpha1.Kvrocks, headless bool) *corev1.Service {
	service := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: db.Name, Namespace: db.Namespace, Labels: labelsFor(db)},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{ownerLabel: string(db.UID)},
			Ports:    []corev1.ServicePort{{Name: "redis", Port: databasePort, TargetPort: intstr.FromString("redis"), Protocol: corev1.ProtocolTCP}},
		},
	}
	if headless {
		service.Name += "-headless"
		service.Spec.ClusterIP = corev1.ClusterIPNone
	}
	return service
}

func statefulSetFor(db *kvrocksv1alpha1.Kvrocks, secret *corev1.Secret) *appsv1.StatefulSet {
	config := configurationFor(db)
	configHash := sha256.Sum256([]byte(config.Data["kvrocks.conf"] + startupScript))
	return &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{Name: db.Name, Namespace: db.Namespace, Labels: labelsFor(db)},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    ptr.To(int32(1)),
			ServiceName: db.Name + "-headless",
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{ownerLabel: string(db.UID)}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labelsFor(db),
					Annotations: map[string]string{
						"kvrocks-operator.org/config-hash":     fmt.Sprintf("%x", configHash),
						"kvrocks-operator.org/secret-revision": string(secret.UID) + "/" + secret.ResourceVersion,
					},
				},
				Spec: podSpecFor(db, config.Name),
			},
		},
	}
}

func podSpecFor(db *kvrocksv1alpha1.Kvrocks, configName string) corev1.PodSpec {
	return corev1.PodSpec{
		AutomountServiceAccountToken:  ptr.To(false),
		EnableServiceLinks:            ptr.To(false),
		TerminationGracePeriodSeconds: ptr.To(int64(60)),
		NodeSelector:                  db.Spec.NodeSelector,
		Tolerations:                   db.Spec.Tolerations,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:        ptr.To(true),
			RunAsUser:           ptr.To(int64(999)),
			RunAsGroup:          ptr.To(int64(999)),
			FSGroup:             ptr.To(int64(999)),
			FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
			SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Containers: []corev1.Container{kvrocksContainerFor(db)},
		Volumes: []corev1.Volume{
			{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: db.Name + "-data"}}},
			{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configName}}}},
			{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
			{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
	}
}

func kvrocksContainerFor(db *kvrocksv1alpha1.Kvrocks) corev1.Container {
	passwordRef := db.Spec.Auth.PasswordSecretRef
	probeCommand := []string{"/bin/bash", "-c", "set -euo pipefail; [[ \"$(redis-cli --raw -h 127.0.0.1 -p 6666 ping)\" == PONG ]]"}
	return corev1.Container{
		Name:            "kvrocks",
		Image:           db.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/bin/bash", "/etc/kvrocks/startup.sh"},
		Env: []corev1.EnvVar{{
			Name: "REDISCLI_AUTH",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: passwordRef.Name},
					Key:                  passwordRef.Key,
				},
			},
		}},
		Ports:     []corev1.ContainerPort{{Name: "redis", ContainerPort: databasePort}},
		Resources: db.Spec.Resources,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand}},
			PeriodSeconds:    10,
			TimeoutSeconds:   5,
			FailureThreshold: 180,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:   corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand}},
			PeriodSeconds:  5,
			TimeoutSeconds: 5,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("redis")}},
			PeriodSeconds:    10,
			TimeoutSeconds:   5,
			FailureThreshold: 6,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/kvrocks"},
			{Name: "config", MountPath: "/etc/kvrocks", ReadOnly: true},
			{Name: "runtime", MountPath: "/run/kvrocks"},
			{Name: "tmp", MountPath: "/tmp"},
		},
	}
}
