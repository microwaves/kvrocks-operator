package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	kvrocksv1alpha1 "github.com/microwaves/kvrocks-operator/api/v1alpha1"
	"github.com/microwaves/kvrocks-operator/internal/controller"
	// +kubebuilder:scaffold:imports
)

func main() {
	var leaderElection bool
	var probeAddress string
	flag.BoolVar(&leaderElection, "leader-elect", true, "Elect one active operator in the watched namespace")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "Address for operator health checks")
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))

	if err := run(ctrl.SetupSignalHandler(), leaderElection, probeAddress); err != nil {
		ctrl.Log.Error(err, "Operator stopped")
		os.Exit(1)
	}
}

func run(ctx context.Context, leaderElection bool, probeAddress string) error {
	namespace := os.Getenv("WATCH_NAMESPACE")
	if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
		return fmt.Errorf("WATCH_NAMESPACE must name exactly one namespace: %v", problems)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register Kubernetes API types: %w", err)
	}
	if err := kvrocksv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register Kvrocks API types: %w", err)
	}
	// +kubebuilder:scaffold:scheme

	config, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                  scheme,
		Cache:                   cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  probeAddress,
		LeaderElection:          leaderElection,
		LeaderElectionID:        "kvrocks-operator.org",
		LeaderElectionNamespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}
	reconciler := &controller.KvrocksReconciler{Client: manager.GetClient(), APIReader: manager.GetAPIReader()}
	if err := reconciler.SetupWithManager(ctx, manager); err != nil {
		return fmt.Errorf("register Kvrocks controller: %w", err)
	}
	// +kubebuilder:scaffold:builder

	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("register liveness check: %w", err)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("register readiness check: %w", err)
	}
	ctrl.Log.Info("Starting operator", "namespace", namespace)
	if err := manager.Start(ctx); err != nil {
		return fmt.Errorf("run controller manager: %w", err)
	}
	return nil
}
