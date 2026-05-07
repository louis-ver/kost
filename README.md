# kost

A Kubernetes operator that autoscales queue-based batch workloads with cost awareness.

[KEDA](https://keda.sh) scales on queue depth. The Kubernetes HPA scales on CPU. Neither one knows what your workers cost per hour. `kost` does.

## How it works

`kost` watches an SQS queue and an EC2 spot price feed. Every 30 seconds it computes:

```
desiredReplicas = ceil(queueDepth / targetMessagesPerWorker)
estimatedCost   = desiredReplicas × spotPrice

if estimatedCost > hourlyBudget && desiredReplicas > currentReplicas:
    freeze scale-up   # BudgetHalted condition = True
```

Scale-down uses a configurable stabilization window (default 120s) to prevent thrashing on bursty queues.

The spot price and queue depth are polled in background goroutines (SQS every 15s, EC2 pricing every 5m) and cached in memory. The reconciler reads from the cache — it never blocks on an AWS API call in the hot path.

## Architecture

```
┌─────────────────────────────────────────┐
│ kost-controller Pod                     │
│                                         │
│  SQS Poller (15s) ──┐                  │
│                      ├─▶ MetricsCache  │
│  EC2 Poller  (5m)  ──┘        │        │
│                                │        │
│            Reconciler ◀────────┘        │
│            reads cache                  │
│            patches Deployment           │
│            updates status               │
└─────────────────────────────────────────┘
```

## Quick start

```bash
# Install the CRD
kubectl apply -f https://raw.githubusercontent.com/louis-ver/kost/main/config/crd/bases/kost.kost.io_costawarescalers.yaml

# Deploy your workers as a normal Deployment, then create a scaler
kubectl apply -f - <<EOF
apiVersion: kost.kost.io/v1alpha1
kind: CostAwareScaler
metadata:
  name: my-worker-scaler
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-worker

  queue:
    provider: sqs
    url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
    region: us-east-1
    targetMessagesPerWorker: 10   # target queue depth per replica

  scaling:
    minReplicas: 1
    maxReplicas: 50
    scaleDownStabilizationSeconds: 120

  cost:
    instanceType: m5.xlarge
    availabilityZone: us-east-1a
    hourlyBudgetUSD: 10.00        # halt scale-up above this rate
EOF
```

Check status:

```bash
$ kubectl get costawarescaler
NAME               CURRENT   DESIRED   QUEUEDEPTH   COST/HR
my-worker-scaler   8         12        124          1.07

$ kubectl get costawarescaler my-worker-scaler -o jsonpath='{.status.conditions}'
[{"type":"BudgetHalted","status":"False",...},{"type":"Degraded","status":"False",...}]
```

## CRD reference

| Field | Description |
|-------|-------------|
| `spec.targetRef` | The Deployment to scale |
| `spec.queue.url` | SQS queue URL |
| `spec.queue.targetMessagesPerWorker` | Desired queue depth per replica |
| `spec.scaling.minReplicas` / `maxReplicas` | Replica bounds |
| `spec.scaling.scaleDownStabilizationSeconds` | Hold before committing a scale-down (default 120s) |
| `spec.cost.instanceType` | Instance type for spot price lookup |
| `spec.cost.availabilityZone` | AZ for spot price lookup |
| `spec.cost.hourlyBudgetUSD` | Max $/hr rate — halts scale-up if exceeded |

### Status conditions

| Condition | Meaning |
|-----------|---------|
| `BudgetHalted=True` | Scale-up frozen — `desiredReplicas × spotPrice > hourlyBudgetUSD` |
| `Degraded=True` | Queue depth cache is stale (>60s) — scaling decisions paused |
| `Invalid=True` | Spec misconfiguration (e.g. `minReplicas > maxReplicas`) |

## Metrics

```
kost_queue_depth{scaler, namespace}
kost_desired_replicas{scaler, namespace}
kost_current_replicas{scaler, namespace}
kost_estimated_hourly_cost_usd{scaler, namespace}
kost_spot_price_per_hour_usd{scaler, namespace}
kost_budget_halted{scaler, namespace}           # gauge: 1 or 0
kost_scaling_decisions_total{scaler, namespace, reason}
kost_sqs_poll_errors_total{scaler}
kost_pricing_poll_errors_total{scaler}
```

## Design notes

**Cost is a rate estimate, not accumulated spend.** `estimatedCost = desiredReplicas × spotPrice` is a $/hr rate check. It prevents scaling to a level you can't afford, not tracking total spend over time.

**`instanceType` is a declaration, not a guarantee.** `kost` does not manage node provisioning — that is Karpenter or Cluster Autoscaler's job. The cost estimate is approximate.

**Budget halt never forces a scale-down.** If spot price spikes while workers are running, in-flight jobs are not killed. Only new scale-up is blocked.

**`kost` and HPA are mutually exclusive** for the same Deployment — they will conflict over `spec.replicas`.

## IAM permissions required

```json
{
  "Effect": "Allow",
  "Action": [
    "sqs:GetQueueAttributes",
    "ec2:DescribeSpotPriceHistory"
  ],
  "Resource": "*"
}
```

## v0.1 scope

- AWS only (SQS queue source, EC2 spot pricing)
- Deployments only
- Single `CostAwareScaler` per operator deployment

## Spot interruption handling (v0.2)

`kost-node-agent` is a DaemonSet that detects EC2 spot interruption notices and gracefully evicts kost worker pods within the 2-minute window — before Kubernetes would normally react (5+ minute lag).

**What it does:**
1. Polls the EC2 Instance Metadata Service every 5s for a termination notice (supports IMDSv1 and IMDSv2)
2. On notice: cordons the node (prevents new pods scheduling there)
3. Discovers kost-managed worker pods on the node via `CostAwareScaler` objects
4. Evicts them via the Kubernetes Eviction API (respects PodDisruptionBudgets)

**What it does NOT do:** immediately reset SQS message visibility. Workers that handle `SIGTERM` can call `ChangeMessageVisibility(0)` to immediately re-queue their in-flight messages. Without this, messages become visible again after the SQS visibility timeout expires naturally.

**Deploy:**

```bash
kubectl create namespace kost-system
kubectl apply -f config/node-agent/rbac.yaml
kubectl apply -f config/node-agent/daemonset.yaml
```

**Pod matching:** the node agent matches pods using the `app=<deployment-name>` label, which `kubectl create deployment` sets by default. If your Deployment uses a custom label selector, ensure the `app` label is set to the Deployment name.

## Roadmap

- **v0.3 — GPU instance support**: extend cost lookup to GPU instance types for batch inference workloads
- **Multi-queue / multi-CRD**: per-scaler poller lifecycle

## Building

```bash
go build ./...
go test ./...

# Controller tests require envtest binaries
make setup-envtest
KUBEBUILDER_ASSETS=$(./bin/setup-envtest use 1.31.0 -p path) go test ./internal/controller/...
```

Requires Go 1.22+, kubebuilder 4.x.
