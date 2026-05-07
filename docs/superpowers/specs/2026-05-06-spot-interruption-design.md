# kost v0.2 — Spot Interruption Handling Design Spec
**Date:** 2026-05-06
**Status:** Approved

---

## Overview

When AWS reclaims a spot instance, without intervention Kubernetes takes 5+ minutes to detect the dead node and reschedule pods. Workers are hard-killed mid-message, leaving SQS messages invisible for the full visibility timeout. v0.2 adds a node agent that acts within the 2-minute interruption notice window — cordoning the node and gracefully evicting kost worker pods before the instance is terminated.

**Value proposition:** reduces disruption time from 5–10 minutes to within the 2-minute window, and gives workers a SIGTERM + `terminationGracePeriodSeconds` to finish or abandon their current message instead of being hard-killed mid-job.

**What this does NOT do:** immediately reset SQS message visibility. Workers that handle SIGTERM can call `ChangeMessageVisibility(0)` themselves to get immediate requeue — but that requires worker code changes, which is out of scope. Without worker SIGTERM handling, messages become visible again after the visibility timeout expires naturally.

---

## Architecture

Two new components alongside the existing, unchanged operator:

```
┌─────────────────────────────┐     ┌──────────────────────────────────┐
│  kost-controller Pod        │     │  kost-node-agent Pod (DaemonSet) │
│  (existing, unchanged)      │     │  one per node                    │
│                             │     │                                  │
│  Reconciler                 │     │  IMDSPoller (every 5s)           │
│  SQS Poller                 │     │    polls 169.254.169.254         │
│  Pricing Poller             │     │    /spot/termination-time        │
└─────────────────────────────┘     │                                  │
                                    │  InterruptionHandler (once)      │
                                    │    1. cordon node                │
                                    │    2. discover kost pods         │
                                    │    3. evict them                 │
                                    └──────────────────────────────────┘
```

The existing operator is not modified. When the dying node is cordoned, replacement pods schedule naturally onto healthy nodes via the existing reconciler loop.

---

## New Files

| File | Responsibility |
|------|----------------|
| `cmd/node-agent/main.go` | Entrypoint — wires Kubernetes client, starts IMDS poller |
| `internal/nodeagent/imds.go` | Polls IMDS, signals interruption notice |
| `internal/nodeagent/handler.go` | Cordons node, discovers kost pods, evicts them |
| `config/node-agent/daemonset.yaml` | DaemonSet manifest |
| `config/node-agent/rbac.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding |

---

## IMDS Poller

Polls `http://169.254.169.254/latest/meta-data/spot/termination-time` every 5 seconds.

- **404** → no notice yet, keep polling (this is the normal steady state — nothing is logged)
- **200** → interruption notice received, fire handler once and exit
- **Connection error** → log warning, keep polling — do NOT trigger handler (could be non-EC2 environment or transient failure)

**HTTP timeout:** 1 second. IMDS is local to the instance; anything slower is stalled.

**Node name:** injected via the Kubernetes Downward API as `KUBE_NODE_NAME` environment variable in the DaemonSet spec. Used by the handler to cordon the correct node and filter pods.

**Poll interval:** 5 seconds gives 12–24 checks within the 2-minute window — tight enough to react quickly without log noise.

---

## Interruption Handler

Runs once on 200 response. Steps execute in order; each failure is logged but does not block subsequent steps.

### Step 1: Cordon node
```
PATCH nodes/<KUBE_NODE_NAME>
  spec.unschedulable: true
```
Prevents the scheduler from placing new pods on the dying node during eviction. If this fails, log error and continue — eviction without cordon is still better than nothing.

### Step 2: Discover kost-managed pods on this node
```
LIST CostAwareScalers (all namespaces)
for each scaler:
    LIST pods where:
        spec.nodeName = KUBE_NODE_NAME
        labels["app"] = scaler.Spec.TargetRef.Name
```
Targets only pods belonging to kost-managed Deployments. Does not evict unrelated workloads on the same node. If the `CostAwareScaler` list fails, log error and exit — cannot safely identify pods to evict.

**Label matching note:** relies on the `app=<deployment-name>` label convention set by `kubectl create deployment`. Users with custom label selectors on their Deployment must ensure `app` is set accordingly. This limitation is documented.

### Step 3: Evict each pod
```
POST pods/<pod-name>/eviction
```
Uses the Kubernetes Eviction API (not DELETE) to respect PodDisruptionBudgets. Kubernetes handles graceful termination via `terminationGracePeriodSeconds`. Eviction failures are logged and skipped — the node is going away regardless.

### Step 4: Exit
Log a summary of what was cordoned and evicted. The process exits — there is nothing more to do.

---

## What the Reconciler Sees

The reconciler is unaware of the interruption. After cordon + eviction:
- Deployment reports fewer ready replicas
- Queue depth is still high
- Reconciler computes `desiredReplicas > currentReplicas` and patches the Deployment
- New pods schedule on healthy nodes (cordon prevents them landing on the dying node)

This is the correct behavior — no changes to the reconciler are needed.

---

## Comparison: Node Agent vs. Alternatives

| | Hard spot kill (no agent) | Reconciler scale-down | Node agent (v0.2) |
|---|---|---|---|
| Timing | After node dies (5+ min Kubernetes lag) | Immediate | Within 2-min window |
| Graceful SIGTERM | No — hard kill | Yes | Yes |
| Prevents new pods on dying node | No | N/A | Yes (cordon) |
| Respects PDBs | No | Yes | Yes (Eviction API) |
| Queue-state aware | No | Yes | No |

---

## Error Handling

| Failure | Behavior |
|---------|----------|
| IMDS unreachable | Log warning, keep polling — do NOT trigger handler |
| Cordon fails | Log error, continue to eviction |
| `CostAwareScaler` list fails | Log error, exit handler |
| Pod list fails for one scaler | Log error, skip that scaler |
| Eviction fails | Log error, move on |

---

## RBAC

ClusterRole (cluster-wide access required — the node agent acts on nodes and pods across all namespaces):

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["kost.kost.io"]
    resources: ["costawarescalers"]
    verbs: ["list"]
```

---

## Testing

### Unit tests
- IMDS response parsing: 200, 404, connection error, non-spot-related 4xx
- Pod selection logic: given node name + `CostAwareScaler` list + pod list → correct pods selected
- Handler with mocked Kubernetes client: verify cordon patch and eviction calls are made, verify partial failures don't block remaining steps

### Integration tests (envtest)
- Fake HTTP server standing in for IMDS returns 200
- Verify node is cordoned (`spec.unschedulable=true`)
- Verify correct pods are evicted (and only those pods)
- Verify handler exits cleanly after eviction

### Not in scope for v0.2
- End-to-end test on a real spot instance
- Testing actual SIGTERM handling in workers (that's user code)

---

## Out of Scope for v0.2

- Immediate SQS `ChangeMessageVisibility(0)` reset (requires worker code changes)
- EventBridge interruption events (IMDS polling is sufficient)
- Handling On-Demand instance terminations
- Multi-CRD node agent (inherits from operator v0.1 limitation)
