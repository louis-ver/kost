package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	metricscache "github.com/louisolivier/kost/internal/cache"
)

var _ = Describe("CostAwareScaler controller", func() {
	const (
		scalerName     = "test-scaler"
		deploymentName = "test-worker"
		namespaceName  = "default"
	)

	ctx := context.Background()
	scalerKey := types.NamespacedName{Name: scalerName, Namespace: namespaceName}
	depKey := types.NamespacedName{Name: deploymentName, Namespace: namespaceName}

	createDeployment := func(replicas int32) {
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: namespaceName},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "nginx"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
	}

	createScaler := func() {
		scaler := &kostv1alpha1.CostAwareScaler{
			ObjectMeta: metav1.ObjectMeta{Name: scalerName, Namespace: namespaceName},
			Spec: kostv1alpha1.CostAwareScalerSpec{
				TargetRef: kostv1alpha1.TargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: deploymentName},
				Queue: kostv1alpha1.QueueSpec{
					Provider: "sqs", URL: "https://sqs.fake/q", Region: "us-east-1",
					TargetMessagesPerWorker: 10,
				},
				Scaling: kostv1alpha1.ScalingSpec{MinReplicas: 1, MaxReplicas: 50, ScaleDownStabilizationSeconds: 0},
				Cost:    kostv1alpha1.CostSpec{InstanceType: "m5.xlarge", AvailabilityZone: "us-east-1a", HourlyBudgetUSD: 10.0},
			},
		}
		Expect(k8sClient.Create(ctx, scaler)).To(Succeed())
	}

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, &kostv1alpha1.CostAwareScaler{ObjectMeta: metav1.ObjectMeta{Name: scalerName, Namespace: namespaceName}})
		_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: namespaceName}})
		// Replace cache so next test starts with an unpopulated cache
		testCache = metricscache.New()
		reconciler.Cache = testCache
	})

	It("scales deployment up to match queue depth", func() {
		createDeployment(1)
		createScaler()
		testCache.SetQueueDepth(100)
		testCache.SetSpotPrice(0.05)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())

		var dep appsv1.Deployment
		Expect(k8sClient.Get(ctx, depKey, &dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(10)))
	})

	It("halts scale-up when estimated cost exceeds budget", func() {
		createDeployment(3)
		createScaler()
		testCache.SetQueueDepth(500)
		testCache.SetSpotPrice(1.0)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())

		var dep appsv1.Deployment
		Expect(k8sClient.Get(ctx, depKey, &dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(3)))

		var scaler kostv1alpha1.CostAwareScaler
		Expect(k8sClient.Get(ctx, scalerKey, &scaler)).To(Succeed())
		budgetHalted := false
		for _, c := range scaler.Status.Conditions {
			if c.Type == kostv1alpha1.ConditionBudgetHalted && c.Status == metav1.ConditionTrue {
				budgetHalted = true
			}
		}
		Expect(budgetHalted).To(BeTrue())
	})

	It("sets Invalid condition for bad spec", func() {
		createDeployment(1)
		scaler := &kostv1alpha1.CostAwareScaler{
			ObjectMeta: metav1.ObjectMeta{Name: scalerName, Namespace: namespaceName},
			Spec: kostv1alpha1.CostAwareScalerSpec{
				TargetRef: kostv1alpha1.TargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: deploymentName},
				Queue:     kostv1alpha1.QueueSpec{Provider: "sqs", URL: "https://sqs.fake/q", Region: "us-east-1", TargetMessagesPerWorker: 10},
				Scaling:   kostv1alpha1.ScalingSpec{MinReplicas: 10, MaxReplicas: 5},
				Cost:      kostv1alpha1.CostSpec{InstanceType: "m5.xlarge", AvailabilityZone: "us-east-1a", HourlyBudgetUSD: 10.0},
			},
		}
		Expect(k8sClient.Create(ctx, scaler)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())

		var updated kostv1alpha1.CostAwareScaler
		Expect(k8sClient.Get(ctx, scalerKey, &updated)).To(Succeed())
		invalid := false
		for _, c := range updated.Status.Conditions {
			if c.Type == kostv1alpha1.ConditionInvalid && c.Status == metav1.ConditionTrue {
				invalid = true
			}
		}
		Expect(invalid).To(BeTrue())
	})

	It("uses on-demand price fallback when spot price cache is empty", func() {
		createDeployment(1)
		createScaler()
		testCache.SetQueueDepth(20) // 20 / 10 = 2 replicas
		// Deliberately no spot price set — reconciler should fall back to
		// m5.xlarge on-demand ($0.192/hr), 2 * $0.192 = $0.384 << $10 budget

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())

		var dep appsv1.Deployment
		Expect(k8sClient.Get(ctx, depKey, &dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(2)))
	})

	It("requeues when queue cache is not yet populated", func() {
		createDeployment(1)
		createScaler()

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(15 * time.Second))
	})
})
