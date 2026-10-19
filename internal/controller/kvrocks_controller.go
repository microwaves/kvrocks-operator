package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
)

const resyncInterval = 30 * time.Second

type KvrocksReconciler struct {
	client.Client
	APIReader client.Reader
}

type blockedError struct {
	reason  string
	message string
}

func (e *blockedError) Error() string { return e.message }

func blocked(reason, message string) error {
	return &blockedError{reason: reason, message: message}
}

// +kubebuilder:rbac:groups=kvrocks-operator.org,resources=kvrocks,verbs=get;list;watch,namespace=system
// +kubebuilder:rbac:groups=kvrocks-operator.org,resources=kvrocks/status,verbs=get;update;patch,namespace=system
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch,namespace=system
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create,namespace=system
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;patch,namespace=system
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;patch,namespace=system

func (r *KvrocksReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	db := &kvrocksv1alpha1.Kvrocks{}
	if err := r.Get(ctx, request.NamespacedName, db); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !db.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	before := db.DeepCopy()
	reconcileErr := r.reconcileDatabase(ctx, db)
	condition := metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "InstanceReady",
		Message: "The standalone instance is ready", ObservedGeneration: db.Generation,
	}
	if reconcileErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "ReconcileFailed"
		condition.Message = reconcileErr.Error()
		var waiting *blockedError
		if errors.As(reconcileErr, &waiting) {
			condition.Reason = waiting.reason
			reconcileErr = nil
		}
	}
	meta.SetStatusCondition(&db.Status.Conditions, condition)
	if !reflect.DeepEqual(before.Status, db.Status) {
		patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
		if err := r.Status().Patch(ctx, db, patch); err != nil {
			return ctrl.Result{}, errors.Join(reconcileErr, fmt.Errorf("update Kvrocks %s/%s status: %w", db.Namespace, db.Name, err))
		}
	}
	if reconcileErr != nil {
		return ctrl.Result{}, reconcileErr
	}
	return ctrl.Result{RequeueAfter: resyncInterval}, nil
}

func (r *KvrocksReconciler) reconcileDatabase(ctx context.Context, db *kvrocksv1alpha1.Kvrocks) error {
	secret := &corev1.Secret{}
	reference := db.Spec.Auth.PasswordSecretRef
	if err := r.Get(ctx, client.ObjectKey{Namespace: db.Namespace, Name: reference.Name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return blocked("SecretNotFound", fmt.Sprintf("password Secret %s/%s does not exist", db.Namespace, reference.Name))
		}
		return fmt.Errorf("read password Secret %s/%s: %w", db.Namespace, reference.Name, err)
	}
	if err := validatePassword(secret.Data[reference.Key]); err != nil {
		return blocked("InvalidSecret", fmt.Sprintf("password Secret %s/%s key %q: %s", db.Namespace, reference.Name, reference.Key, err))
	}

	knownVolumeUID := db.Status.VolumeClaimUID
	claim, err := r.reconcileStorage(ctx, db)
	if err != nil {
		return err
	}
	if knownVolumeUID == "" {
		return blocked("InitializingStorage", "recording the data volume identity before starting the database")
	}
	if err := r.applyOwned(ctx, db, configurationFor(db)); err != nil {
		return err
	}
	if err := r.applyOwned(ctx, db, serviceFor(db, true)); err != nil {
		return err
	}
	if err := r.applyOwned(ctx, db, serviceFor(db, false)); err != nil {
		return err
	}
	workload := statefulSetFor(db, secret)
	if err := r.applyOwned(ctx, db, workload); err != nil {
		return err
	}
	db.Status.Endpoint = fmt.Sprintf("%s.%s.svc:%d", db.Name, db.Namespace, databasePort)

	// Create the workload before waiting for storage: WaitForFirstConsumer
	// provisioners need a scheduled Pod to choose the volume's topology.
	if claim.Status.Phase != corev1.ClaimBound {
		return blocked("VolumePending", fmt.Sprintf("waiting for persistent volume claim %s/%s to bind", claim.Namespace, claim.Name))
	}
	if workload.Status.ObservedGeneration < workload.Generation || workload.Status.ReadyReplicas != 1 ||
		workload.Status.UpdatedReplicas != 1 || workload.Status.CurrentRevision != workload.Status.UpdateRevision {
		return blocked("InstanceNotReady", fmt.Sprintf("waiting for StatefulSet %s/%s to finish its rollout and pass readiness checks", db.Namespace, db.Name))
	}
	return nil
}

func validatePassword(password []byte) error {
	if len(password) == 0 {
		return errors.New("password is missing or empty")
	}
	if len(password) > 512 || !utf8.Valid(password) || strings.ContainsAny(string(password), "\x00\r\n") {
		return errors.New("password must be valid UTF-8, at most 512 bytes, and contain no NUL, CR, or LF characters")
	}
	return nil
}

func (r *KvrocksReconciler) reconcileStorage(ctx context.Context, db *kvrocksv1alpha1.Kvrocks) (*corev1.PersistentVolumeClaim, error) {
	claim := volumeClaimFor(db)
	err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(claim), claim)
	if apierrors.IsNotFound(err) {
		// A stale cache must never turn a missing established volume into a
		// fresh bootstrap. Check persisted identity before creating storage.
		latest := &kvrocksv1alpha1.Kvrocks{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(db), latest); err != nil {
			return nil, fmt.Errorf("verify Kvrocks %s/%s before storage initialization: %w", db.Namespace, db.Name, err)
		}
		if latest.UID != db.UID || !latest.DeletionTimestamp.IsZero() {
			return nil, blocked("DatabaseChanged", "database identity changed during storage initialization; waiting for a fresh observation")
		}
		if latest.Status.VolumeClaimName != "" {
			return nil, blocked("VolumeMissing", fmt.Sprintf("data volume %s/%s is missing; explicit recovery is required", db.Namespace, latest.Status.VolumeClaimName))
		}
		// Deliberately omit owner references: deleting the database must retain data.
		if err := r.Create(ctx, claim); err != nil {
			return nil, fmt.Errorf("create data volume %s/%s: %w", claim.Namespace, claim.Name, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("read data volume %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	if claim.Labels[ownerLabel] != string(db.UID) || len(claim.OwnerReferences) != 0 {
		return nil, blocked("VolumeOwnershipConflict", fmt.Sprintf("data volume %s/%s does not belong exclusively to this Kvrocks resource; automatic adoption is disabled", claim.Namespace, claim.Name))
	}
	if db.Status.VolumeClaimUID != "" && db.Status.VolumeClaimUID != claim.UID {
		return nil, blocked("VolumeReplaced", fmt.Sprintf("data volume %s/%s has been replaced; explicit recovery is required", claim.Namespace, claim.Name))
	}
	if !claim.DeletionTimestamp.IsZero() || claim.Status.Phase == corev1.ClaimLost {
		return nil, blocked("VolumeUnavailable", fmt.Sprintf("data volume %s/%s is being deleted or has lost its backing volume", claim.Namespace, claim.Name))
	}
	db.Status.VolumeClaimName = claim.Name
	db.Status.VolumeClaimUID = claim.UID
	return claim, nil
}

func (r *KvrocksReconciler) applyOwned(ctx context.Context, db *kvrocksv1alpha1.Kvrocks, desired client.Object) error {
	owner := metav1.NewControllerRef(db, kvrocksv1alpha1.SchemeGroupVersion.WithKind("Kvrocks"))
	owner.BlockOwnerDeletion = ptr.To(false)
	desired.SetOwnerReferences([]metav1.OwnerReference{*owner})
	current := desired.DeepCopyObject().(client.Object)
	key := client.ObjectKeyFromObject(desired)
	kind := desired.GetObjectKind().GroupVersionKind().Kind

	// Ownership checks use an uncached read so a stale cache cannot make us
	// adopt an existing object. UID and resourceVersion guard replacement races.
	if err := r.APIReader.Get(ctx, key, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read %s %s: %w", kind, key, err)
		}
		if err := r.Create(ctx, desired, client.FieldOwner("kvrocks-operator")); err != nil {
			return fmt.Errorf("create %s %s: %w", kind, key, err)
		}
		return nil
	}
	if !metav1.IsControlledBy(current, db) {
		return blocked("ResourceOwnershipConflict", fmt.Sprintf("%s %s is not owned by this Kvrocks resource", kind, key))
	}
	if !current.GetDeletionTimestamp().IsZero() {
		return blocked("ResourceTerminating", fmt.Sprintf("waiting for %s %s to finish deletion", kind, key))
	}
	// These child fields belong to the operator. Taking ownership also moves
	// fields recorded during Create from Update ownership to Apply ownership.
	desired.SetUID(current.GetUID())
	desired.SetResourceVersion(current.GetResourceVersion())
	if err := r.Patch(ctx, desired, client.Apply, client.FieldOwner("kvrocks-operator"), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply %s %s: %w", kind, key, err)
	}
	return nil
}

func (r *KvrocksReconciler) SetupWithManager(ctx context.Context, manager ctrl.Manager) error {
	if err := manager.GetFieldIndexer().IndexField(ctx, &kvrocksv1alpha1.Kvrocks{}, secretIndex, func(object client.Object) []string {
		return []string{object.(*kvrocksv1alpha1.Kvrocks).Spec.Auth.PasswordSecretRef.Name}
	}); err != nil {
		return fmt.Errorf("index Kvrocks password Secret references: %w", err)
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&kvrocksv1alpha1.Kvrocks{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.databasesForSecret)).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			name := object.GetLabels()[databaseLabel]
			if name == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: object.GetNamespace(), Name: name}}}
		})).
		Complete(r)
}

func (r *KvrocksReconciler) databasesForSecret(ctx context.Context, object client.Object) []reconcile.Request {
	databases := &kvrocksv1alpha1.KvrocksList{}
	if err := r.List(ctx, databases, client.InNamespace(object.GetNamespace()), client.MatchingFields{secretIndex: object.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Kvrocks resources referencing Secret", "secret", client.ObjectKeyFromObject(object))
		return nil
	}
	requests := make([]reconcile.Request, 0, len(databases.Items))
	for _, db := range databases.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&db)})
	}
	return requests
}
