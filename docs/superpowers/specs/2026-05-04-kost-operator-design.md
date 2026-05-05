# kost Operator — Design Spec
**Date:** 2026-05-04
**Status:** Approved

---

## Overview

`kost` is a Kubernetes operator that autoscales queue-based, CPU-intensive batch workloads with cost awareness. It fills a gap that KEDA leaves open: KEDA is queue-aware but cost-blind. `kost` scales Deployment replicas proportionally to queue depth, then applies a cost budget constraint that halts scale-up before a declared hourly spend limit is exceeded.

**Target use case:** batch workers draining an SQS queue, running on spot instances, where runaway scaling is a real cost risk.

**v0.1 scope:** AWS only (SQS queue source, EC2 spot pricing), Deployments only, single operator process.

---

## Architecture

Three runtime components share a process inside the `kost-controller` Pod:

```
┌─────────────────────────────────────────────────────┐
│ kost-controller Pod                                  │
│                                                      │
│  ┌─────────────┐    ┌──────────────────────────┐    │
│  │ SQS Poller  │───▶│                          │    │
│  │ (every 15s) │    │   MetricsCache           │    │
│  └─────────────┘    │   (RWMutex-guarded)      │    │
│                     │                          │    │
│  ┌─────────────┐    │   queueDepth + timestamp │    │
│  │Pricing Poll │───▶│   spotPrice + timestamp  │    │
│  │ (every 5m)  │    │                          │    │
│  └─────────────┘    └────────────┬─────────────┘    │
│                                  │                   │
│  ┌───────────────────────────────▼──────────────┐   │
│  │  CostAwareScaler Reconciler                  │   │
│  │  (triggered by CRD changes + RequeueAfter)   │   │
│  │                                              │   │
│  │  reads cache → computes replicas →           │   │
│  │  updates Deployment + CRD status             │   │
│  └──────────────────────────────────────────────┘   │
│                                                      │
│  ┌────────────────┐                                  │
│  │ /metrics       │                                  │
│  │ (Prometheus)   │                                  │
│  └────────────────┘                                  │
└─────────────────────────────────────────────────────┘
```

The reconciler never calls AWS directly — it always reads from `MetricsCache`. Pollers run as goroutines started in `main.go` and write to the cache on their own schedule. This decouples external API latency (AWS Pricing can be 500ms+) from the reconcile hot path.

**Limitation:** pollers are global per operator process. v0.1 supports one `CostAwareScaler` per operator deployment. Multi-scaler support (different queues, different instance types) requires per-scaler poller management and is out of scope.

---

## CRD Schema

```yaml
apiVersion: kost.io/v1alpha1
kind: CostAwareScaler
metadata:
  name: my-worker-scaler
  namespace: default
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-worker

  queue:
    provider: sqs                          # only valid value in v0.1
    url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
    region: us-east-1
    targetMessagesPerWorker: 10            # desired queue depth per replica

  scaling:
    minReplicas: 1
    maxReplicas: 50
    scaleDownStabilizationSeconds: 120     # hold before committing a scale-down

  cost:
    instanceType: m5.xlarge                # user-declared; used for spot price lookup
    availabilityZone: us-east-1a
    hourlyBudgetUSD: 10.00                 # halt scale-up if desiredReplicas * spotPrice exceeds this

status:
  currentReplicas: 5
  desiredReplicas: 8
  queueDepth: 80
  spotPricePerHourUSD: 0.089
  estimatedHourlyCostUSD: 0.445
  conditions:
    - type: BudgetHalted    # scale-up frozen due to cost constraint
      status: "False"
    - type: Degraded        # cache too stale to make reliable decisions
      status: "False"
    - type: Invalid         # misconfigured spec (inline validation)
      status: "False"
  lastScaleTime: "2026-05-04T10:00:00Z"
```

**Notes:**
- `instanceType` is a declaration, not a guarantee. kost does not manage node provisioning — that is Karpenter/CA's job. Cost estimates are approximate.
- `cost.hourlyBudgetUSD` is a **rate check** (`desiredReplicas * spotPrice <= budget`), not accumulated spend tracking. This avoids the need for persistent state.
- `kost` and HPA must not both target the same Deployment — they will fight over `spec.replicas`. Document this as a hard constraint.

---

## Scaling Algorithm

```
// Inputs
queueDepth    = queued + inFlight
              = ApproximateNumberOfMessages + ApproximateNumberOfMessagesNotVisible
spotPrice     = cache.SpotPrice()      ($/hr per instance)
currentReplicas = deployment.Spec.Replicas

// Step 1: compute desired from queue depth
rawDesired = ceil(queueDepth / targetMessagesPerWorker)
rawDesired = clamp(rawDesired, minReplicas, maxReplicas)

// Step 2: budget hard halt (scale-UP only)
estimatedCost = rawDesired * spotPrice
if estimatedCost > hourlyBudgetUSD && rawDesired > currentReplicas:
    desired = currentReplicas      // freeze scale-up
    set BudgetHalted condition
else:
    desired = rawDesired
    clear BudgetHalted condition

// Step 3: scale-down stabilization
if desired < currentReplicas:
    if now - lastScaleDownProposal < scaleDownStabilizationSeconds:
        desired = currentReplicas  // hold — not stable yet
    else:
        record lastScaleDownProposal = now

// Step 4: apply
if desired != currentReplicas:
    patch Deployment spec.replicas = desired
    update status + lastScaleTime
```

**Design decisions:**
- **Budget does not force scale-down.** If spot price spikes while workers are running, we do not kill in-flight jobs. We only prevent new scale-up. This is intentional.
- **`inFlight` messages are included** in `queueDepth`. Omitting them would cause premature scale-down while workers are actively processing.
- **Scale-down stabilization** prevents replica thrashing on bursty queue patterns. Default 120s mirrors HPA's default stabilization window.

---

## Background Pollers + Cache

### MetricsCache

```go
type MetricsCache struct {
    mu           sync.RWMutex
    queueDepth   int64
    queueFetched time.Time
    spotPrice    float64
    priceFetched time.Time
}

func (c *MetricsCache) QueueDepth() (depth int64, age time.Duration, ok bool)
func (c *MetricsCache) SpotPrice() (price float64, age time.Duration, ok bool)
```

### SQS Poller
- Poll interval: 15s (configurable via operator flag)
- Calls `sqs.GetQueueAttributes` for both `ApproximateNumberOfMessages` and `ApproximateNumberOfMessagesNotVisible`
- On success: writes sum to cache
- On error: logs, increments `kost_sqs_poll_errors_total`, leaves cache unchanged

### Pricing Poller
- Poll interval: 5 minutes (configurable via operator flag)
- Calls `ec2.DescribeSpotPriceHistory` filtered to `instanceType` + `availabilityZone`
- Takes the most recent price entry from the response
- On error: logs, increments `kost_pricing_poll_errors_total`, leaves cache unchanged

### Staleness thresholds (operator flags, not per-CRD)
| Cache entry | Stale after | Reconciler behavior |
|---|---|---|
| Queue depth | 60s | Set `Degraded` condition, skip reconcile |
| Spot price | 15 minutes | Fall back to on-demand price (conservative — budget check becomes stricter) |

---

## Error Handling

| Scenario | Behavior |
|---|---|
| AWS credentials missing at startup | Preflight check in `main.go` — log and exit |
| SQS poll error | Non-fatal; cache unchanged; `Degraded` after 60s staleness |
| Pricing poll error | Non-fatal; cache unchanged; fallback to on-demand after 15m staleness |
| Deployment update fails | controller-runtime exponential backoff retry |
| Target Deployment not found | Set `Ready=False / DeploymentNotFound`, requeue after 30s |
| Invalid spec (minReplicas > maxReplicas, targetMessagesPerWorker = 0, etc.) | Inline reconciler validation; set `Invalid` condition, skip reconcile |

Validation is handled inline in the reconciler (no admission webhook) to reduce deployment complexity for MVP.

---

## Prometheus Metrics

```
kost_queue_depth{scaler, namespace}
kost_desired_replicas{scaler, namespace}
kost_current_replicas{scaler, namespace}
kost_estimated_hourly_cost_usd{scaler, namespace}
kost_spot_price_per_hour_usd{scaler, namespace}
kost_budget_halted{scaler, namespace}              # gauge: 1 or 0
kost_sqs_poll_errors_total{scaler, namespace}
kost_pricing_poll_errors_total{scaler, namespace}
kost_scaling_decisions_total{scaler, namespace, reason}
  # reason values: queue_depth, budget_halt, min_replicas, max_replicas, stabilization
```

---

## Testing

### Unit tests
The scaling algorithm is extracted as a pure function — takes plain inputs, returns desired replicas + reason string. Table-driven tests cover:
- Normal scale-up and scale-down
- Budget halt triggers (exactly at limit, above limit)
- Scale-down stabilization window (within window, outside window)
- Clamp to min/max replicas
- Edge cases: empty queue, zero spot price, queueDepth < targetMessagesPerWorker

### Integration tests
Using `envtest` (kubebuilder) + LocalStack for SQS:
- Create `CostAwareScaler` → verify Deployment replicas update
- Mock high spot price → verify budget halt blocks scale-up
- Stop SQS poller → wait for staleness threshold → verify `Degraded` condition set
- Mock `DescribeSpotPriceHistory` error → verify on-demand fallback after threshold

### Not in scope for v0.1
- End-to-end tests (kind + real AWS credentials)
- Grafana dashboard
- Load/stress testing

---

## Out of Scope for v0.1

- Kafka, RabbitMQ, Google Pub/Sub, Azure Service Bus queue sources
- GCP / Azure cloud pricing
- StatefulSet targets
- Daily or monthly budget windows (accumulated spend tracking)
- Multi-scaler support (multiple `CostAwareScaler` instances per operator)
- Admission webhook validation
- Grafana dashboards
- Karpenter / ASG node pool management
