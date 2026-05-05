package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kost_queue_depth",
		Help: "Current number of messages in the queue (queued + in-flight).",
	}, []string{"scaler", "namespace"})

	DesiredReplicas = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kost_desired_replicas",
		Help: "Desired replica count computed by kost.",
	}, []string{"scaler", "namespace"})

	CurrentReplicas = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kost_current_replicas",
		Help: "Current replica count of the target Deployment.",
	}, []string{"scaler", "namespace"})

	EstimatedHourlyCostUSD = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kost_estimated_hourly_cost_usd",
		Help: "Estimated hourly cost in USD (desiredReplicas * spotPrice).",
	}, []string{"scaler", "namespace"})

	SpotPricePerHourUSD = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kost_spot_price_per_hour_usd",
		Help: "Current spot price per hour for the configured instance type.",
	}, []string{"scaler", "namespace"})

	BudgetHalted = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kost_budget_halted",
		Help: "1 if scale-up is currently halted by the cost budget, 0 otherwise.",
	}, []string{"scaler", "namespace"})

	ScalingDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kost_scaling_decisions_total",
		Help: "Total number of scaling decisions made, by reason.",
	}, []string{"scaler", "namespace", "reason"})

	SQSPollErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kost_sqs_poll_errors_total",
		Help: "Total number of SQS poll errors.",
	}, []string{"scaler"})

	PricingPollErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kost_pricing_poll_errors_total",
		Help: "Total number of EC2 pricing poll errors.",
	}, []string{"scaler"})
)
