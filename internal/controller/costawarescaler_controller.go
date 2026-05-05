package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/prometheus/client_golang/prometheus"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/algorithm"
	metricscache "github.com/louisolivier/kost/internal/cache"
	kostmetrics "github.com/louisolivier/kost/internal/metrics"
	"github.com/louisolivier/kost/internal/poller"
)

// CostAwareScalerReconciler reconciles a CostAwareScaler object
// +kubebuilder:rbac:groups=kost.kost.io,resources=costawarescalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kost.kost.io,resources=costawarescalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch
type CostAwareScalerReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Cache     *metricscache.MetricsCache
	SQSClient poller.SQSClient
	EC2Client poller.EC2Client

	pollerOnce   sync.Once
	pollerCancel context.CancelFunc

	stabMu                    sync.Mutex
	lastScaleDownProposalTime time.Time
}

func (r *CostAwareScalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	var scaler kostv1alpha1.CostAwareScaler
	if err := r.Get(ctx, req.NamespacedName, &scaler); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Inline spec validation
	if err := validateSpec(scaler.Spec); err != nil {
		setCondition(&scaler.Status.Conditions, kostv1alpha1.ConditionInvalid, metav1.ConditionTrue, "InvalidSpec", err.Error())
		if updateErr := r.Status().Update(ctx, &scaler); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}
	setCondition(&scaler.Status.Conditions, kostv1alpha1.ConditionInvalid, metav1.ConditionFalse, "SpecValid", "")

	// Start pollers on first successful reconcile
	if r.SQSClient != nil && r.EC2Client != nil {
		r.pollerOnce.Do(func() {
			pollerCtx, cancel := context.WithCancel(context.Background())
			r.pollerCancel = cancel
			sl := slog.Default()
			sqsPoller := poller.NewSQSPoller(r.SQSClient, scaler.Spec.Queue.URL, r.Cache, 15*time.Second, sl)
			pricingPoller := poller.NewPricingPoller(r.EC2Client, scaler.Spec.Cost.InstanceType, scaler.Spec.Cost.AvailabilityZone, r.Cache, 5*time.Minute, sl)
			go sqsPoller.Run(pollerCtx)
			go pricingPoller.Run(pollerCtx)
			logger.Info("pollers started", "queueURL", scaler.Spec.Queue.URL, "instanceType", scaler.Spec.Cost.InstanceType)
		})
	}

	// Check queue depth cache freshness
	queueDepth, queueAge, queueOK := r.Cache.QueueDepth()
	if !queueOK {
		logger.Info("waiting for initial queue depth poll")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if queueAge > 60*time.Second {
		setCondition(&scaler.Status.Conditions, kostv1alpha1.ConditionDegraded, metav1.ConditionTrue,
			"StaleQueueMetrics", fmt.Sprintf("queue depth cache is %s old (threshold: 60s)", queueAge.Round(time.Second)))
		_ = r.Status().Update(ctx, &scaler)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	setCondition(&scaler.Status.Conditions, kostv1alpha1.ConditionDegraded, metav1.ConditionFalse, "MetricsFresh", "")

	// Get spot price; fall back to on-demand if stale
	spotPrice, priceAge, priceOK := r.Cache.SpotPrice()
	if !priceOK || priceAge > 15*time.Minute {
		spotPrice = onDemandPrice(scaler.Spec.Cost.InstanceType)
		logger.Info("using on-demand price fallback", "instanceType", scaler.Spec.Cost.InstanceType, "price", spotPrice)
	}

	// Fetch target Deployment
	var deployment appsv1.Deployment
	depKey := types.NamespacedName{Namespace: scaler.Namespace, Name: scaler.Spec.TargetRef.Name}
	if err := r.Get(ctx, depKey, &deployment); err != nil {
		if apierrors.IsNotFound(err) {
			setCondition(&scaler.Status.Conditions, "Ready", metav1.ConditionFalse,
				"DeploymentNotFound", fmt.Sprintf("deployment %s not found", scaler.Spec.TargetRef.Name))
			_ = r.Status().Update(ctx, &scaler)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	currentReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		currentReplicas = *deployment.Spec.Replicas
	}

	// Compute seconds since last scale-down proposal for stabilization
	r.stabMu.Lock()
	secondsSinceLastScaleDown := int32(-1)
	if !r.lastScaleDownProposalTime.IsZero() {
		secondsSinceLastScaleDown = int32(time.Since(r.lastScaleDownProposalTime).Seconds())
	}
	r.stabMu.Unlock()

	result := algorithm.Compute(algorithm.Input{
		QueueDepth:                queueDepth,
		SpotPrice:                 spotPrice,
		CurrentReplicas:           currentReplicas,
		TargetMessagesPerWorker:   scaler.Spec.Queue.TargetMessagesPerWorker,
		MinReplicas:               scaler.Spec.Scaling.MinReplicas,
		MaxReplicas:               scaler.Spec.Scaling.MaxReplicas,
		HourlyBudgetUSD:           scaler.Spec.Cost.HourlyBudgetUSD,
		StabilizationWindow:       scaler.Spec.Scaling.ScaleDownStabilizationSeconds,
		SecondsSinceLastScaleDown: secondsSinceLastScaleDown,
	})

	// Update stabilization tracking when a scale-down is committed
	if result.DesiredReplicas < currentReplicas && !result.StabilizationHeld {
		r.stabMu.Lock()
		r.lastScaleDownProposalTime = time.Now()
		r.stabMu.Unlock()
	}

	// Patch Deployment if replicas changed
	if result.DesiredReplicas != currentReplicas {
		patch := client.MergeFrom(deployment.DeepCopy())
		deployment.Spec.Replicas = &result.DesiredReplicas
		if err := r.Patch(ctx, &deployment, patch); err != nil {
			return ctrl.Result{}, err
		}
		now := metav1.Now()
		scaler.Status.LastScaleTime = &now
		logger.Info("scaled deployment", "from", currentReplicas, "to", result.DesiredReplicas, "reason", result.Reason)
	}

	// Update status
	budgetStatus := metav1.ConditionFalse
	if result.BudgetHalted {
		budgetStatus = metav1.ConditionTrue
	}
	setCondition(&scaler.Status.Conditions, kostv1alpha1.ConditionBudgetHalted, budgetStatus, result.Reason, "")
	scaler.Status.CurrentReplicas = currentReplicas
	scaler.Status.DesiredReplicas = result.DesiredReplicas
	scaler.Status.QueueDepth = queueDepth
	scaler.Status.SpotPricePerHourUSD = spotPrice
	scaler.Status.EstimatedHourlyCostUSD = result.EstimatedHourlyCost
	if err := r.Status().Update(ctx, &scaler); err != nil {
		return ctrl.Result{}, err
	}

	labels := prometheus.Labels{"scaler": scaler.Name, "namespace": scaler.Namespace}
	kostmetrics.QueueDepth.With(labels).Set(float64(queueDepth))
	kostmetrics.DesiredReplicas.With(labels).Set(float64(result.DesiredReplicas))
	kostmetrics.CurrentReplicas.With(labels).Set(float64(currentReplicas))
	kostmetrics.EstimatedHourlyCostUSD.With(labels).Set(result.EstimatedHourlyCost)
	kostmetrics.SpotPricePerHourUSD.With(labels).Set(spotPrice)
	budgetHaltedVal := float64(0)
	if result.BudgetHalted {
		budgetHaltedVal = 1
	}
	kostmetrics.BudgetHalted.With(labels).Set(budgetHaltedVal)
	if result.DesiredReplicas != currentReplicas {
		kostmetrics.ScalingDecisions.With(prometheus.Labels{
			"scaler": scaler.Name, "namespace": scaler.Namespace, "reason": result.Reason,
		}).Inc()
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *CostAwareScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kostv1alpha1.CostAwareScaler{}).
		Named("costawarescaler").
		Complete(r)
}

func validateSpec(spec kostv1alpha1.CostAwareScalerSpec) error {
	if spec.Scaling.MinReplicas < 0 {
		return fmt.Errorf("scaling.minReplicas must be >= 0")
	}
	if spec.Scaling.MaxReplicas < 1 {
		return fmt.Errorf("scaling.maxReplicas must be >= 1")
	}
	if spec.Scaling.MinReplicas > spec.Scaling.MaxReplicas {
		return fmt.Errorf("scaling.minReplicas (%d) must be <= scaling.maxReplicas (%d)",
			spec.Scaling.MinReplicas, spec.Scaling.MaxReplicas)
	}
	if spec.Queue.TargetMessagesPerWorker <= 0 {
		return fmt.Errorf("queue.targetMessagesPerWorker must be > 0")
	}
	if spec.Queue.URL == "" {
		return fmt.Errorf("queue.url must not be empty")
	}
	if spec.Cost.InstanceType == "" {
		return fmt.Errorf("cost.instanceType must not be empty")
	}
	if spec.Cost.HourlyBudgetUSD <= 0 {
		return fmt.Errorf("cost.hourlyBudgetUSD must be > 0")
	}
	return nil
}

// onDemandPrice returns a conservative on-demand $/hr fallback for known instance types.
func onDemandPrice(instanceType string) float64 {
	prices := map[string]float64{
		"m5.large": 0.096, "m5.xlarge": 0.192, "m5.2xlarge": 0.384, "m5.4xlarge": 0.768,
		"c5.large": 0.085, "c5.xlarge": 0.170, "c5.2xlarge": 0.340, "c5.4xlarge": 0.680,
		"r5.large": 0.126, "r5.xlarge": 0.252,
	}
	if p, ok := prices[instanceType]; ok {
		return p
	}
	return 1.0
}

func setCondition(conditions *[]metav1.Condition, condType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i, c := range *conditions {
		if c.Type == condType {
			(*conditions)[i].Status = status
			(*conditions)[i].Reason = reason
			(*conditions)[i].Message = message
			(*conditions)[i].LastTransitionTime = now
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
}
