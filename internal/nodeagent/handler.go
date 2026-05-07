package nodeagent

import (
	"context"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
)

// SelectPodsForEviction returns pods on nodeName that belong to a kost-managed Deployment.
// Matching uses the pod's "app" label against each CostAwareScaler's spec.targetRef.name.
func SelectPodsForEviction(nodeName string, scalers []kostv1alpha1.CostAwareScaler, pods []corev1.Pod) []corev1.Pod {
	targets := make(map[string]bool, len(scalers))
	for _, s := range scalers {
		targets[s.Spec.TargetRef.Name] = true
	}

	var result []corev1.Pod
	for _, pod := range pods {
		if pod.Spec.NodeName != nodeName {
			continue
		}
		if targets[pod.Labels["app"]] {
			result = append(result, pod)
		}
	}
	return result
}

type InterruptionHandler struct {
	nodeName  string
	k8sClient client.Client
	logger    *slog.Logger
}

func NewInterruptionHandler(nodeName string, k8sClient client.Client, logger *slog.Logger) *InterruptionHandler {
	return &InterruptionHandler{nodeName: nodeName, k8sClient: k8sClient, logger: logger}
}

// Handle cordons the node, discovers kost-managed pods on it, and evicts them.
// Each step failure is logged but does not block subsequent steps.
func (h *InterruptionHandler) Handle(ctx context.Context) {
	h.cordonNode(ctx)
	pods := h.findKostPods(ctx)
	h.evictPods(ctx, pods)
	h.logger.Info("interruption handling complete", "node", h.nodeName, "evicted", len(pods))
}

func (h *InterruptionHandler) cordonNode(ctx context.Context) {
	var node corev1.Node
	if err := h.k8sClient.Get(ctx, types.NamespacedName{Name: h.nodeName}, &node); err != nil {
		h.logger.Error("cordon: failed to get node", "node", h.nodeName, "error", err)
		return
	}
	patch := client.MergeFrom(node.DeepCopy())
	node.Spec.Unschedulable = true
	if err := h.k8sClient.Patch(ctx, &node, patch); err != nil {
		h.logger.Error("cordon: failed to patch node", "node", h.nodeName, "error", err)
		return
	}
	h.logger.Info("node cordoned", "node", h.nodeName)
}

func (h *InterruptionHandler) findKostPods(ctx context.Context) []corev1.Pod {
	var scalers kostv1alpha1.CostAwareScalerList
	if err := h.k8sClient.List(ctx, &scalers); err != nil {
		h.logger.Error("find: failed to list CostAwareScalers", "error", err)
		return nil
	}

	var podList corev1.PodList
	if err := h.k8sClient.List(ctx, &podList); err != nil {
		h.logger.Error("find: failed to list pods", "error", err)
		return nil
	}

	pods := SelectPodsForEviction(h.nodeName, scalers.Items, podList.Items)
	h.logger.Info("found kost pods on node", "node", h.nodeName, "count", len(pods))
	return pods
}

func (h *InterruptionHandler) evictPods(ctx context.Context, pods []corev1.Pod) {
	for _, pod := range pods {
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		}
		if err := h.k8sClient.SubResource("eviction").Create(ctx, &pod, eviction); err != nil {
			h.logger.Error("evict: failed", "pod", pod.Name, "namespace", pod.Namespace, "error", err)
			continue
		}
		h.logger.Info("pod evicted", "pod", pod.Name, "namespace", pod.Namespace)
	}
}
