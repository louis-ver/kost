package nodeagent

import (
	corev1 "k8s.io/api/core/v1"

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
