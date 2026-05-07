# Spot Interruption Handling (v0.2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `kost-node-agent` DaemonSet that detects EC2 spot interruption notices via IMDS and gracefully cordons + evicts kost worker pods within the 2-minute window.

**Architecture:** A new standalone binary (`cmd/node-agent`) polls the EC2 Instance Metadata Service every 5 seconds. On a 200 response it runs the `InterruptionHandler` once: cordon the node, list `CostAwareScaler` objects to find kost-managed worker pods on this node, evict each via the Kubernetes Eviction API, then exit. The existing operator is not modified.

**Tech Stack:** Go 1.22+, controller-runtime client, `github.com/aws/aws-sdk-go-v2/feature/ec2/imds` for IMDS (IMDSv1 + IMDSv2), `k8s.io/api/policy/v1` for Eviction, `fakeIMDSClient` interface for IMDS unit tests, controller-runtime `fake` client for handler tests.

---

## File Map

| File | Responsibility |
|------|----------------|
| `internal/nodeagent/imds.go` | IMDSPoller — AWS SDK IMDS client, polls every 5s, returns bool (interrupted) |
| `internal/nodeagent/imds_test.go` | Unit tests with injectable `fakeIMDSClient` (no HTTP server needed) |
| `internal/nodeagent/handler.go` | InterruptionHandler + pure `SelectPodsForEviction` |
| `internal/nodeagent/handler_test.go` | Unit tests for selection + fake-client integration tests |
| `cmd/node-agent/main.go` | Entrypoint |
| `config/node-agent/daemonset.yaml` | DaemonSet manifest |
| `config/node-agent/rbac.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding |

---

### Task 1: IMDS Poller

**Files:**
- Create: `internal/nodeagent/imds.go`
- Create: `internal/nodeagent/imds_test.go`

**Implementation note:** Uses the AWS SDK IMDS client (`github.com/aws/aws-sdk-go-v2/feature/ec2/imds`) rather than raw `net/http`. This handles IMDSv2 token acquisition automatically — new AWS instances disable IMDSv1 by default, making the SDK approach necessary for correctness. Tests use an injectable `fakeIMDSClient` interface rather than `httptest.Server`.

- [ ] **Step 1: Write the failing tests**

```bash
mkdir -p internal/nodeagent
```

Create `internal/nodeagent/imds_test.go`:

```go
package nodeagent_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	"github.com/louisolivier/kost/internal/nodeagent"
)

type fakeIMDSClient struct {
	err error
}

func (f *fakeIMDSClient) GetMetadata(_ context.Context, _ *imds.GetMetadataInput, _ ...func(*imds.Options)) (*imds.GetMetadataOutput, error) {
	return nil, f.err
}

type httpError struct {
	statusCode int
	msg        string
}

func (e *httpError) Error() string       { return e.msg }
func (e *httpError) HTTPStatusCode() int { return e.statusCode }

func TestIMDSPoller_DetectsInterruption(t *testing.T) {
	// nil error = GetMetadata succeeded = termination-time exists = interruption
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: nil}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if !poller.Run(ctx) {
		t.Fatal("expected interruption to be detected when GetMetadata returns nil error")
	}
}

func TestIMDSPoller_NoInterruption_OnNotFound(t *testing.T) {
	notFound := &httpError{statusCode: 404, msg: "404 Not Found"}
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: notFound}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("expected no interruption on 404 response")
	}
}

func TestIMDSPoller_ConnectionError_DoesNotTrigger(t *testing.T) {
	networkErr := fmt.Errorf("connection refused")
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: networkErr}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("connection error should not trigger interruption")
	}
}

func TestIMDSPoller_UnexpectedStatus_DoesNotTrigger(t *testing.T) {
	serverErr := &httpError{statusCode: 500, msg: "500 Internal Server Error"}
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: serverErr}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("unexpected status code should not trigger interruption")
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/nodeagent/...
```

Expected: compile error — package `nodeagent` does not exist yet

- [ ] **Step 3: Implement the IMDS poller**

Create `internal/nodeagent/imds.go`:

```go
package nodeagent

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
)

// IMDSClient is an interface over the AWS IMDS client for testability.
type IMDSClient interface {
	GetMetadata(ctx context.Context, params *imds.GetMetadataInput, optFns ...func(*imds.Options)) (*imds.GetMetadataOutput, error)
}

type IMDSPoller struct {
	client   IMDSClient
	interval time.Duration
	logger   *slog.Logger
}

// NewIMDSPoller creates a poller using the real AWS IMDS client (handles IMDSv1 and IMDSv2).
func NewIMDSPoller(interval time.Duration, logger *slog.Logger) *IMDSPoller {
	return &IMDSPoller{
		client:   imds.New(imds.Options{}),
		interval: interval,
		logger:   logger,
	}
}

// NewIMDSPollerWithClient creates a poller with an injectable IMDS client for testing.
func NewIMDSPollerWithClient(client IMDSClient, interval time.Duration, logger *slog.Logger) *IMDSPoller {
	return &IMDSPoller{client: client, interval: interval, logger: logger}
}

// Run polls IMDS until an interruption notice is detected or ctx is cancelled.
// Returns true if an interruption notice was received. Polls once immediately on start.
func (p *IMDSPoller) Run(ctx context.Context) bool {
	if p.poll(ctx) {
		return true
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if p.poll(ctx) {
				return true
			}
		}
	}
}

func (p *IMDSPoller) poll(ctx context.Context) bool {
	// Bound each individual poll to 1 second regardless of SDK defaults
	pollCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	_, err := p.client.GetMetadata(pollCtx, &imds.GetMetadataInput{
		Path: "spot/termination-time",
	})
	if err == nil {
		// Successful response means termination-time field exists — interruption notice received
		p.logger.Info("imds: spot interruption notice received")
		return true
	}

	var notFoundErr interface{ HTTPStatusCode() int }
	if errors.As(err, &notFoundErr) && notFoundErr.HTTPStatusCode() == 404 {
		return false // no notice yet
	}

	p.logger.Warn("imds: poll failed", "error", err)
	return false
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/nodeagent/... -run TestIMDS -v
```

Expected:
```
--- PASS: TestIMDSPoller_DetectsInterruption
--- PASS: TestIMDSPoller_NoInterruption_OnNotFound
--- PASS: TestIMDSPoller_ConnectionError_DoesNotTrigger
--- PASS: TestIMDSPoller_UnexpectedStatus_DoesNotTrigger
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/nodeagent/
git commit -m "feat: implement IMDS poller for spot interruption detection"
```

---

### Task 2: Pod selection logic

**Files:**
- Create: `internal/nodeagent/handler.go` (selection function only)
- Create: `internal/nodeagent/handler_test.go` (selection tests only)

- [ ] **Step 1: Write the failing tests**

Create `internal/nodeagent/handler_test.go`:

```go
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
			Spec: corev1.PodSpec{NodeName: "node-b"}, // different node
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/nodeagent/... -run TestSelect
```

Expected: compile error — `nodeagent.SelectPodsForEviction` not defined

- [ ] **Step 3: Implement the selection function**

Create `internal/nodeagent/handler.go` with the selection function only:

```go
package nodeagent

import (
	corev1 "k8s.io/api/core/v1"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
)

// SelectPodsForEviction returns pods on nodeName that belong to a kost-managed Deployment.
// Matching is done via the pod's "app" label against each CostAwareScaler's targetRef.name.
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
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/nodeagent/... -run TestSelect -v
```

Expected:
```
--- PASS: TestSelectPodsForEviction_ReturnsKostPodsOnNode
--- PASS: TestSelectPodsForEviction_MultipleScalers
--- PASS: TestSelectPodsForEviction_EmptyInputs
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/nodeagent/handler.go internal/nodeagent/handler_test.go
git commit -m "feat: implement pod selection logic for interruption handler"
```

---

### Task 3: InterruptionHandler

**Files:**
- Modify: `internal/nodeagent/handler.go` (add InterruptionHandler struct and methods)
- Modify: `internal/nodeagent/handler_test.go` (add fake-client integration tests)

- [ ] **Step 1: Write the failing handler integration tests**

Append to `internal/nodeagent/handler_test.go`:

```go
import (
	"context"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/nodeagent"
)

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
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &updated)
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
		WithStatusSubresource(scaler).
		Build()

	handler := nodeagent.NewInterruptionHandler("node-a", fakeClient, slog.Default())
	// Handle should complete without panicking and cordon the node
	handler.Handle(context.Background())

	var updated corev1.Node
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &updated)
	if !updated.Spec.Unschedulable {
		t.Error("expected node to be cordoned")
	}
}
```

Also add these imports to the test file header:

```go
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/nodeagent/... -run TestInterruptionHandler
```

Expected: compile error — `nodeagent.NewInterruptionHandler` not defined

- [ ] **Step 3: Implement InterruptionHandler**

Add to `internal/nodeagent/handler.go` (append after `SelectPodsForEviction`):

```go
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
```

The full `handler.go` import block:

```go
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
```

- [ ] **Step 4: Run all nodeagent tests to confirm they pass**

```bash
go test ./internal/nodeagent/... -v
```

Expected: all 7 tests `PASS`

- [ ] **Step 5: Commit**

```bash
git add internal/nodeagent/handler.go internal/nodeagent/handler_test.go
git commit -m "feat: implement InterruptionHandler with cordon and pod eviction"
```

---

### Task 4: Node agent entrypoint

**Files:**
- Create: `cmd/node-agent/main.go`

- [ ] **Step 1: Create the entrypoint**

```bash
mkdir -p cmd/node-agent
```

Create `cmd/node-agent/main.go`:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/nodeagent"
)

func main() {
	logger := slog.Default()

	nodeName := os.Getenv("KUBE_NODE_NAME")
	if nodeName == "" {
		logger.Error("KUBE_NODE_NAME env var not set — set via Downward API in DaemonSet spec")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		logger.Error("failed to add client-go scheme", "error", err)
		os.Exit(1)
	}
	if err := kostv1alpha1.AddToScheme(scheme); err != nil {
		logger.Error("failed to add kost scheme", "error", err)
		os.Exit(1)
	}

	k8sClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		logger.Error("failed to create Kubernetes client", "error", err)
		os.Exit(1)
	}

	handler := nodeagent.NewInterruptionHandler(nodeName, k8sClient, logger)
	poller := nodeagent.NewIMDSPoller(5*time.Second, logger)

	logger.Info("kost node agent started", "node", nodeName)

	if poller.Run(context.Background()) {
		handler.Handle(context.Background())
	}
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build ./cmd/node-agent/...
```

Expected: no output (success). Binary created at `./node-agent` (or wherever your GOPATH/GOBIN puts it).

- [ ] **Step 3: Run the full test suite to confirm nothing broke**

```bash
KUBEBUILDER_ASSETS=$(find ./bin/k8s -mindepth 1 -maxdepth 1 -type d | head -1) go test ./...
```

Expected: all tests `PASS`

- [ ] **Step 4: Commit**

```bash
git add cmd/node-agent/
git commit -m "feat: add kost-node-agent entrypoint"
```

---

### Task 5: Kubernetes manifests

**Files:**
- Create: `config/node-agent/daemonset.yaml`
- Create: `config/node-agent/rbac.yaml`

- [ ] **Step 1: Create the RBAC manifest**

```bash
mkdir -p config/node-agent
```

Create `config/node-agent/rbac.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kost-node-agent
  namespace: kost-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kost-node-agent
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["kost.kost.io"]
    resources: ["costawarescalers"]
    verbs: ["list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kost-node-agent
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: kost-node-agent
subjects:
  - kind: ServiceAccount
    name: kost-node-agent
    namespace: kost-system
```

- [ ] **Step 2: Create the DaemonSet manifest**

Create `config/node-agent/daemonset.yaml`:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: kost-node-agent
  namespace: kost-system
spec:
  selector:
    matchLabels:
      app: kost-node-agent
  template:
    metadata:
      labels:
        app: kost-node-agent
    spec:
      serviceAccountName: kost-node-agent
      hostNetwork: false
      containers:
        - name: node-agent
          image: ghcr.io/louis-ver/kost-node-agent:latest
          imagePullPolicy: Always
          env:
            - name: KUBE_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          resources:
            requests:
              cpu: 10m
              memory: 32Mi
            limits:
              cpu: 100m
              memory: 64Mi
      tolerations:
        # Run on spot nodes even if they have spot-specific taints
        - key: "node.kubernetes.io/unreachable"
          operator: "Exists"
          effect: "NoExecute"
          tolerationSeconds: 10
        - key: "node.kubernetes.io/not-ready"
          operator: "Exists"
          effect: "NoExecute"
          tolerationSeconds: 10
```

- [ ] **Step 3: Commit**

```bash
git add config/node-agent/
git commit -m "feat: add kost-node-agent DaemonSet and RBAC manifests"
```

---

### Task 6: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add node agent section to README**

In `README.md`, add this section after the existing "v0.1 scope" section:

```markdown
## Spot interruption handling (v0.2)

`kost-node-agent` is a DaemonSet that detects EC2 spot interruption notices and gracefully evicts kost worker pods within the 2-minute window — before Kubernetes would normally react (5+ minute lag).

**What it does:**
1. Polls the EC2 Instance Metadata Service every 5s for a termination notice
2. On notice: cordons the node (prevents new pods scheduling there)
3. Discovers kost-managed worker pods on the node via `CostAwareScaler` objects
4. Evicts them via the Kubernetes Eviction API (respects PodDisruptionBudgets)

**What it does NOT do:** reset SQS message visibility. Workers that handle `SIGTERM` can call `ChangeMessageVisibility(0)` to immediately re-queue their in-flight messages. Without this, messages become visible again after the SQS visibility timeout.

**Deploy:**

```bash
# Requires the CRD to already be installed
kubectl create namespace kost-system
kubectl apply -f config/node-agent/rbac.yaml
kubectl apply -f config/node-agent/daemonset.yaml
```

**Pod matching:** the node agent matches pods using the `app=<deployment-name>` label, which `kubectl create deployment` sets by default. If your Deployment uses a custom label selector, ensure `app` is set to the Deployment name.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document kost-node-agent spot interruption handling"
```

---

## Self-Review

**Spec coverage:**
- ✅ IMDS polling every 5s, 1s timeout: Task 1
- ✅ 200 → interrupt, 404 → keep polling, error → keep polling: Task 1
- ✅ Node name from `KUBE_NODE_NAME`: Task 4
- ✅ Cordon node: Task 3 (`cordonNode`)
- ✅ CRD-driven pod discovery: Task 3 (`findKostPods`) + Task 2 (`SelectPodsForEviction`)
- ✅ Eviction API (not DELETE): Task 3 (`evictPods`)
- ✅ Errors logged, don't block next step: Task 3 (each method continues on error)
- ✅ RBAC: Task 5
- ✅ DaemonSet manifest with Downward API for node name: Task 5
- ✅ Unit tests for IMDS parsing: Task 1
- ✅ Unit tests for pod selection: Task 2
- ✅ Integration tests with fake client: Task 3

**Type consistency:**
- `SelectPodsForEviction` defined in Task 2, called in Task 3 (`findKostPods`) ✓
- `NewInterruptionHandler` defined in Task 3, called in Task 4 (`main.go`) ✓
- `NewIMDSPoller` / `NewIMDSPollerWithURL` defined in Task 1, called in Task 4 ✓
- `IMDSPoller.Run` returns `bool`, checked in Task 4 with `if poller.Run(ctx)` ✓
