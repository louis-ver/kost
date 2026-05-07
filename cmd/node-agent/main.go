package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/nodeagent"
)

func main() {
	logger := slog.Default()

	nodeName := os.Getenv("KUBE_NODE_NAME")
	if nodeName == "" {
		logger.Error("KUBE_NODE_NAME env var not set — set via Downward API in DaemonSet spec")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		logger.Error("failed to add client-go scheme", "error", err)
		os.Exit(1)
	}
	if err := kostv1alpha1.AddToScheme(scheme); err != nil {
		logger.Error("failed to add kost scheme", "error", err)
		os.Exit(1)
	}

	k8sClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		logger.Error("failed to create Kubernetes client", "error", err)
		os.Exit(1)
	}

	handler := nodeagent.NewInterruptionHandler(nodeName, k8sClient, logger)
	poller := nodeagent.NewIMDSPoller(5*time.Second, logger)

	// Signal-aware context for the polling loop — allows clean shutdown via SIGTERM/SIGINT
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("kost node agent started", "node", nodeName)

	if poller.Run(ctx) {
		// Use a fresh background context for the handler — the node is going away,
		// we must not abort eviction due to the signal context being cancelled.
		handler.Handle(context.Background())
	}
}
