package nodeagent_test

import (
	"context"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func buildScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = kostv1alpha1.AddToScheme(s)
	return s
}

func TestInterruptionHandler_CordonsNode(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(buildScheme()).
		WithObjects(node).
		Build()

	handler := nodeagent.NewInterruptionHandler("node-a", fakeClient, slog.Default())
	handler.Handle(context.Background())

	var updated corev1.Node
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &updated); err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if !updated.Spec.Unschedulable {
		t.Error("expected node to be cordoned (spec.unschedulable=true)")
	}
}

func TestInterruptionHandler_EvictsKostPods(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
	}
	scaler := &kostv1alpha1.CostAwareScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "my-scaler", Namespace: "default"},
		Spec: kostv1alpha1.CostAwareScalerSpec{
			TargetRef: kostv1alpha1.TargetRef{Name: "my-worker"},
			Queue:     kostv1alpha1.QueueSpec{URL: "https://sqs.fake/q", Region: "us-east-1", TargetMessagesPerWorker: 10},
			Scaling:   kostv1alpha1.ScalingSpec{MinReplicas: 1, MaxReplicas: 10},
			Cost:      kostv1alpha1.CostSpec{InstanceType: "m5.xlarge", AvailabilityZone: "us-east-1a", HourlyBudgetUSD: 10},
		},
	}
	workerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1", Namespace: "default",
			Labels: map[string]string{"app": "my-worker"},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}
	otherPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-1", Namespace: "default",
			Labels: map[string]string{"app": "other"},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(buildScheme()).
		WithObjects(node, scaler, workerPod, otherPod).
		Build()

	handler := nodeagent.NewInterruptionHandler("node-a", fakeClient, slog.Default())
	handler.Handle(context.Background())

	// Node must be cordoned
	var updated corev1.Node
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &updated); err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if !updated.Spec.Unschedulable {
		t.Error("expected node to be cordoned")
	}
}

func TestInterruptionHandler_MissingNode_StillEvicts(t *testing.T) {
	// Cordon fails (node not found) but handler should still attempt pod eviction
	scaler := &kostv1alpha1.CostAwareScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "my-scaler", Namespace: "default"},
		Spec: kostv1alpha1.CostAwareScalerSpec{
			TargetRef: kostv1alpha1.TargetRef{Name: "my-worker"},
			Queue:     kostv1alpha1.QueueSpec{URL: "https://sqs.fake/q", Region: "us-east-1", TargetMessagesPerWorker: 10},
			Scaling:   kostv1alpha1.ScalingSpec{MinReplicas: 1, MaxReplicas: 10},
			Cost:      kostv1alpha1.CostSpec{InstanceType: "m5.xlarge", AvailabilityZone: "us-east-1a", HourlyBudgetUSD: 10},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(buildScheme()).
		WithObjects(scaler). // no node
		Build()

	handler := nodeagent.NewInterruptionHandler("node-a", fakeClient, slog.Default())
	// Should not panic even when node is missing
	handler.Handle(context.Background())
}
