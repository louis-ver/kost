package nodeagent_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/nodeagent"
)

func TestSelectPodsForEviction_ReturnsKostPodsOnNode(t *testing.T) {
	scalers := []kostv1alpha1.CostAwareScaler{
		{Spec: kostv1alpha1.CostAwareScalerSpec{
			TargetRef: kostv1alpha1.TargetRef{Name: "my-worker"},
		}},
	}
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: "default",
				Labels: map[string]string{"app": "my-worker"}},
			Spec: corev1.PodSpec{NodeName: "node-a"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "unrelated-1", Namespace: "default",
				Labels: map[string]string{"app": "other-app"}},
			Spec: corev1.PodSpec{NodeName: "node-a"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-2", Namespace: "default",
				Labels: map[string]string{"app": "my-worker"}},
			Spec: corev1.PodSpec{NodeName: "node-b"},
		},
	}

	result := nodeagent.SelectPodsForEviction("node-a", scalers, pods)

	if len(result) != 1 {
		t.Fatalf("expected 1 pod, got %d", len(result))
	}
	if result[0].Name != "worker-1" {
		t.Errorf("expected worker-1, got %s", result[0].Name)
	}
}

func TestSelectPodsForEviction_MultipleScalers(t *testing.T) {
	scalers := []kostv1alpha1.CostAwareScaler{
		{Spec: kostv1alpha1.CostAwareScalerSpec{
			TargetRef: kostv1alpha1.TargetRef{Name: "worker-a"},
		}},
		{Spec: kostv1alpha1.CostAwareScalerSpec{
			TargetRef: kostv1alpha1.TargetRef{Name: "worker-b"},
		}},
	}
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "a-1", Labels: map[string]string{"app": "worker-a"}},
			Spec:       corev1.PodSpec{NodeName: "node-x"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "b-1", Labels: map[string]string{"app": "worker-b"}},
			Spec:       corev1.PodSpec{NodeName: "node-x"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "c-1", Labels: map[string]string{"app": "worker-c"}},
			Spec:       corev1.PodSpec{NodeName: "node-x"},
		},
	}

	result := nodeagent.SelectPodsForEviction("node-x", scalers, pods)

	if len(result) != 2 {
		t.Fatalf("expected 2 pods, got %d", len(result))
	}
}

func TestSelectPodsForEviction_EmptyInputs(t *testing.T) {
	result := nodeagent.SelectPodsForEviction("node-a", nil, nil)
	if len(result) != 0 {
		t.Fatalf("expected 0 pods, got %d", len(result))
	}
}
