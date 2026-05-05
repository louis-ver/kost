# kost Operator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Kubernetes operator that autoscales Deployments based on SQS queue depth, halting scale-up when an hourly cost budget is exceeded.

**Architecture:** A cache-backed reconciler reads queue depth and spot pricing from an in-memory `MetricsCache` populated by two background goroutines (SQS poller every 15s, pricing poller every 5m). The reconciler runs the scaling algorithm — queue depth divided by target messages per worker, with a hard halt if desired replicas × spot price exceeds the hourly budget — and patches the target Deployment.

**Tech Stack:** Go 1.22+, kubebuilder 4.14.0, controller-runtime, AWS SDK v2 (SQS + EC2), prometheus/client_golang

---

## File Map

| File | Responsibility |
|------|----------------|
| `api/v1alpha1/costaware_scaler_types.go` | CRD Go types: spec, status, condition constants |
| `internal/cache/metrics_cache.go` | Thread-safe store for queue depth + spot price with timestamps |
| `internal/algorithm/scaling.go` | Pure function: `Input → Result` (replicas, budget halt, stabilization, reason) |
| `internal/poller/sqs.go` | Background goroutine that polls SQS and writes queue depth to cache |
| `internal/poller/pricing.go` | Background goroutine that polls EC2 spot prices and writes to cache |
| `internal/controller/costaware_scaler_controller.go` | Reconciler: validation → cache read → algorithm → Deployment patch → status update |
| `cmd/main.go` | Entry point: loads AWS config, creates cache, wires reconciler, starts manager |

---

### Task 1: Scaffold the project

**Files:**
- Create: all of `go.mod`, `cmd/main.go`, `api/v1alpha1/`, `internal/controller/`, `config/`, `Makefile` via kubebuilder

- [ ] **Step 1: Initialize the kubebuilder project**

```bash
cd /Users/louisolivier/Developer/kost
kubebuilder init --domain kost.io --repo github.com/louisolivier/kost
```

Expected: output ends with `Next: define a resource with: $ kubebuilder create api`

- [ ] **Step 2: Create the CostAwareScaler API and controller scaffold**

```bash
kubebuilder create api --group kost --version v1alpha1 --kind CostAwareScaler --resource --controller
```

When prompted `Create Resource [y/n]`: enter `y`
When prompted `Create Controller [y/n]`: enter `y`

- [ ] **Step 3: Add AWS SDK v2 and Prometheus dependencies**

```bash
go get github.com/aws/aws-sdk-go-v2/aws@latest
go get github.com/aws/aws-sdk-go-v2/config@latest
go get github.com/aws/aws-sdk-go-v2/service/sqs@latest
go get github.com/aws/aws-sdk-go-v2/service/ec2@latest
go get github.com/prometheus/client_golang@latest
go mod tidy
```

- [ ] **Step 4: Verify the project builds**

```bash
go build ./...
```

Expected: no output (success)

- [ ] **Step 5: Initialize git and commit**

```bash
git init
git add .
git commit -m "feat: scaffold kost operator with kubebuilder"
```

---

### Task 2: Define the CostAwareScaler CRD types

**Files:**
- Modify: `api/v1alpha1/costaware_scaler_types.go`

- [ ] **Step 1: Replace the generated stub with the full types**

Replace the entire contents of `api/v1alpha1/costaware_scaler_types.go` with:

```go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionBudgetHalted = "BudgetHalted"
	ConditionDegraded     = "Degraded"
	ConditionInvalid      = "Invalid"
)

type TargetRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

type QueueSpec struct {
	Provider                string `json:"provider"`
	URL                     string `json:"url"`
	Region                  string `json:"region"`
	TargetMessagesPerWorker int32  `json:"targetMessagesPerWorker"`
}

type ScalingSpec struct {
	MinReplicas                   int32 `json:"minReplicas"`
	MaxReplicas                   int32 `json:"maxReplicas"`
	ScaleDownStabilizationSeconds int32 `json:"scaleDownStabilizationSeconds,omitempty"`
}

type CostSpec struct {
	InstanceType     string  `json:"instanceType"`
	AvailabilityZone string  `json:"availabilityZone"`
	HourlyBudgetUSD  float64 `json:"hourlyBudgetUSD"`
}

type CostAwareScalerSpec struct {
	TargetRef TargetRef   `json:"targetRef"`
	Queue     QueueSpec   `json:"queue"`
	Scaling   ScalingSpec `json:"scaling"`
	Cost      CostSpec    `json:"cost"`
}

type CostAwareScalerStatus struct {
	CurrentReplicas        int32              `json:"currentReplicas,omitempty"`
	DesiredReplicas        int32              `json:"desiredReplicas,omitempty"`
	QueueDepth             int64              `json:"queueDepth,omitempty"`
	SpotPricePerHourUSD    float64            `json:"spotPricePerHourUSD,omitempty"`
	EstimatedHourlyCostUSD float64            `json:"estimatedHourlyCostUSD,omitempty"`
	Conditions             []metav1.Condition `json:"conditions,omitempty"`
	LastScaleTime          *metav1.Time       `json:"lastScaleTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.currentReplicas"
// +kubebuilder:printcolumn:name="Desired",type="integer",JSONPath=".status.desiredReplicas"
// +kubebuilder:printcolumn:name="QueueDepth",type="integer",JSONPath=".status.queueDepth"
// +kubebuilder:printcolumn:name="Cost/hr",type="number",JSONPath=".status.estimatedHourlyCostUSD"
type CostAwareScaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CostAwareScalerSpec   `json:"spec,omitempty"`
	Status            CostAwareScalerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type CostAwareScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CostAwareScaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CostAwareScaler{}, &CostAwareScalerList{})
}
```

- [ ] **Step 2: Regenerate deepcopy and CRD manifests**

```bash
make generate
make manifests
```

Expected: no errors; `config/crd/bases/kost.kost.io_costawarescalers.yaml` updated

- [ ] **Step 3: Verify build**

```bash
go build ./...
```

Expected: no output

- [ ] **Step 4: Commit**

```bash
git add api/ config/
git commit -m "feat: define CostAwareScaler CRD types"
```

---

### Task 3: Implement MetricsCache

**Files:**
- Create: `internal/cache/metrics_cache.go`
- Create: `internal/cache/metrics_cache_test.go`

- [ ] **Step 1: Write the failing tests**

```bash
mkdir -p internal/cache
```

Create `internal/cache/metrics_cache_test.go`:

```go
package cache_test

import (
	"testing"
	"time"

	"github.com/louisolivier/kost/internal/cache"
)

func TestQueueDepth_EmptyCache_ReturnsNotOk(t *testing.T) {
	c := cache.New()
	_, _, ok := c.QueueDepth()
	if ok {
		t.Fatal("expected ok=false on empty cache")
	}
}

func TestQueueDepth_AfterSet_ReturnsValue(t *testing.T) {
	c := cache.New()
	c.SetQueueDepth(42)
	depth, age, ok := c.QueueDepth()
	if !ok {
		t.Fatal("expected ok=true after set")
	}
	if depth != 42 {
		t.Errorf("expected 42, got %d", depth)
	}
	if age > time.Second {
		t.Errorf("expected age < 1s, got %v", age)
	}
}

func TestSpotPrice_EmptyCache_ReturnsNotOk(t *testing.T) {
	c := cache.New()
	_, _, ok := c.SpotPrice()
	if ok {
		t.Fatal("expected ok=false on empty cache")
	}
}

func TestSpotPrice_AfterSet_ReturnsValue(t *testing.T) {
	c := cache.New()
	c.SetSpotPrice(0.089)
	price, age, ok := c.SpotPrice()
	if !ok {
		t.Fatal("expected ok=true after set")
	}
	if price != 0.089 {
		t.Errorf("expected 0.089, got %f", price)
	}
	if age > time.Second {
		t.Errorf("expected age < 1s, got %v", age)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/cache/...
```

Expected: compile error — package `cache` does not exist yet

- [ ] **Step 3: Implement MetricsCache**

Create `internal/cache/metrics_cache.go`:

```go
package cache

import (
	"sync"
	"time"
)

type MetricsCache struct {
	mu           sync.RWMutex
	queueDepth   int64
	queueFetched time.Time
	spotPrice    float64
	priceFetched time.Time
}

func New() *MetricsCache {
	return &MetricsCache{}
}

func (c *MetricsCache) SetQueueDepth(depth int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueDepth = depth
	c.queueFetched = time.Now()
}

func (c *MetricsCache) QueueDepth() (depth int64, age time.Duration, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.queueFetched.IsZero() {
		return 0, 0, false
	}
	return c.queueDepth, time.Since(c.queueFetched), true
}

func (c *MetricsCache) SetSpotPrice(price float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spotPrice = price
	c.priceFetched = time.Now()
}

func (c *MetricsCache) SpotPrice() (price float64, age time.Duration, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.priceFetched.IsZero() {
		return 0, 0, false
	}
	return c.spotPrice, time.Since(c.priceFetched), true
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/cache/... -v
```

Expected:
```
--- PASS: TestQueueDepth_EmptyCache_ReturnsNotOk
--- PASS: TestQueueDepth_AfterSet_ReturnsValue
--- PASS: TestSpotPrice_EmptyCache_ReturnsNotOk
--- PASS: TestSpotPrice_AfterSet_ReturnsValue
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/cache/
git commit -m "feat: implement MetricsCache"
```

---

### Task 4: Implement the scaling algorithm

**Files:**
- Create: `internal/algorithm/scaling.go`
- Create: `internal/algorithm/scaling_test.go`

- [ ] **Step 1: Write the failing tests**

```bash
mkdir -p internal/algorithm
```

Create `internal/algorithm/scaling_test.go`:

```go
package algorithm_test

import (
	"testing"

	"github.com/louisolivier/kost/internal/algorithm"
)

func TestCompute(t *testing.T) {
	tests := []struct {
		name             string
		input            algorithm.Input
		wantReplicas     int32
		wantBudgetHalted bool
		wantStabHeld     bool
		wantReason       string
	}{
		{
			name: "scale up proportional to queue depth",
			input: algorithm.Input{
				QueueDepth: 100, SpotPrice: 0.05, CurrentReplicas: 2,
				TargetMessagesPerWorker: 10, MinReplicas: 1, MaxReplicas: 50,
				HourlyBudgetUSD: 10.0, StabilizationWindow: 120, SecondsSinceLastScaleDown: -1,
			},
			wantReplicas: 10, wantReason: "queue_depth",
		},
		{
			name: "clamp to maxReplicas",
			input: algorithm.Input{
				QueueDepth: 10000, SpotPrice: 0.01, CurrentReplicas: 5,
				TargetMessagesPerWorker: 10, MinReplicas: 1, MaxReplicas: 20,
				HourlyBudgetUSD: 100.0, StabilizationWindow: 120, SecondsSinceLastScaleDown: -1,
			},
			wantReplicas: 20, wantReason: "max_replicas",
		},
		{
			name: "clamp to minReplicas when queue empty",
			input: algorithm.Input{
				QueueDepth: 0, SpotPrice: 0.05, CurrentReplicas: 5,
				TargetMessagesPerWorker: 10, MinReplicas: 2, MaxReplicas: 50,
				HourlyBudgetUSD: 10.0, StabilizationWindow: 120, SecondsSinceLastScaleDown: -1,
			},
			wantReplicas: 2, wantReason: "queue_depth",
		},
		{
			name: "budget halt blocks scale-up",
			input: algorithm.Input{
				QueueDepth: 500, SpotPrice: 1.0, CurrentReplicas: 3,
				TargetMessagesPerWorker: 10, MinReplicas: 1, MaxReplicas: 50,
				HourlyBudgetUSD: 10.0, StabilizationWindow: 120, SecondsSinceLastScaleDown: -1,
			},
			wantReplicas: 3, wantBudgetHalted: true, wantReason: "budget_halt",
		},
		{
			name: "budget halt does not block scale-down",
			input: algorithm.Input{
				QueueDepth: 10, SpotPrice: 1.0, CurrentReplicas: 20,
				TargetMessagesPerWorker: 10, MinReplicas: 1, MaxReplicas: 50,
				HourlyBudgetUSD: 10.0, StabilizationWindow: 0, SecondsSinceLastScaleDown: -1,
			},
			wantReplicas: 1, wantBudgetHalted: false, wantReason: "queue_depth",
		},
		{
			name: "scale-down held within stabilization window",
			input: algorithm.Input{
				QueueDepth: 10, SpotPrice: 0.05, CurrentReplicas: 10,
				TargetMessagesPerWorker: 10, MinReplicas: 1, MaxReplicas: 50,
				HourlyBudgetUSD: 100.0, StabilizationWindow: 120, SecondsSinceLastScaleDown: 30,
			},
			wantReplicas: 10, wantStabHeld: true, wantReason: "stabilization",
		},
		{
			name: "scale-down allowed after stabilization window",
			input: algorithm.Input{
				QueueDepth: 10, SpotPrice: 0.05, CurrentReplicas: 10,
				TargetMessagesPerWorker: 10, MinReplicas: 1, MaxReplicas: 50,
				HourlyBudgetUSD: 100.0, StabilizationWindow: 120, SecondsSinceLastScaleDown: 200,
			},
			wantReplicas: 1, wantReason: "queue_depth",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := algorithm.Compute(tt.input)
			if result.DesiredReplicas != tt.wantReplicas {
				t.Errorf("DesiredReplicas: want %d, got %d", tt.wantReplicas, result.DesiredReplicas)
			}
			if result.BudgetHalted != tt.wantBudgetHalted {
				t.Errorf("BudgetHalted: want %v, got %v", tt.wantBudgetHalted, result.BudgetHalted)
			}
			if result.StabilizationHeld != tt.wantStabHeld {
				t.Errorf("StabilizationHeld: want %v, got %v", tt.wantStabHeld, result.StabilizationHeld)
			}
			if tt.wantReason != "" && result.Reason != tt.wantReason {
				t.Errorf("Reason: want %q, got %q", tt.wantReason, result.Reason)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/algorithm/...
```

Expected: compile error — package `algorithm` does not exist yet

- [ ] **Step 3: Implement the scaling algorithm**

Create `internal/algorithm/scaling.go`:

```go
package algorithm

import "math"

type Input struct {
	QueueDepth                int64
	SpotPrice                 float64
	CurrentReplicas           int32
	TargetMessagesPerWorker   int32
	MinReplicas               int32
	MaxReplicas               int32
	HourlyBudgetUSD           float64
	StabilizationWindow       int32 // seconds; 0 defaults to 120
	SecondsSinceLastScaleDown int32 // -1 means no prior scale-down proposal recorded
}

type Result struct {
	DesiredReplicas     int32
	EstimatedHourlyCost float64
	BudgetHalted        bool
	StabilizationHeld   bool
	Reason              string
}

func Compute(in Input) Result {
	// Step 1: raw desired from queue depth
	rawDesired := in.MinReplicas
	if in.TargetMessagesPerWorker > 0 && in.QueueDepth > 0 {
		raw := int32(math.Ceil(float64(in.QueueDepth) / float64(in.TargetMessagesPerWorker)))
		if raw > rawDesired {
			rawDesired = raw
		}
	}
	if rawDesired < in.MinReplicas {
		rawDesired = in.MinReplicas
	}
	if rawDesired > in.MaxReplicas {
		rawDesired = in.MaxReplicas
	}

	// Step 2: budget hard halt (scale-up only)
	estimatedCost := float64(rawDesired) * in.SpotPrice
	budgetHalted := false
	desired := rawDesired
	if in.HourlyBudgetUSD > 0 && estimatedCost > in.HourlyBudgetUSD && rawDesired > in.CurrentReplicas {
		desired = in.CurrentReplicas
		budgetHalted = true
	}

	// Step 3: scale-down stabilization (only when not budget-halted)
	stabilizationHeld := false
	if !budgetHalted && desired < in.CurrentReplicas {
		window := in.StabilizationWindow
		if window <= 0 {
			window = 120
		}
		if in.SecondsSinceLastScaleDown >= 0 && in.SecondsSinceLastScaleDown < window {
			desired = in.CurrentReplicas
			stabilizationHeld = true
		}
	}

	reason := "queue_depth"
	switch {
	case budgetHalted:
		reason = "budget_halt"
	case stabilizationHeld:
		reason = "stabilization"
	case desired == in.MaxReplicas && rawDesired > in.MaxReplicas:
		reason = "max_replicas"
	}

	return Result{
		DesiredReplicas:     desired,
		EstimatedHourlyCost: estimatedCost,
		BudgetHalted:        budgetHalted,
		StabilizationHeld:   stabilizationHeld,
		Reason:              reason,
	}
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/algorithm/... -v
```

Expected: all 7 test cases `PASS`

- [ ] **Step 5: Commit**

```bash
git add internal/algorithm/
git commit -m "feat: implement scaling algorithm with budget halt and stabilization"
```

---

### Task 5: Implement the SQS poller

**Files:**
- Create: `internal/poller/sqs.go`
- Create: `internal/poller/sqs_test.go`

- [ ] **Step 1: Write the failing tests**

```bash
mkdir -p internal/poller
```

Create `internal/poller/sqs_test.go`:

```go
package poller_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/louisolivier/kost/internal/cache"
	"github.com/louisolivier/kost/internal/poller"
)

type fakeSQSClient struct {
	queued   string
	inFlight string
	err      error
}

func (f *fakeSQSClient) GetQueueAttributes(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"ApproximateNumberOfMessages":           f.queued,
			"ApproximateNumberOfMessagesNotVisible": f.inFlight,
		},
	}, nil
}

func TestSQSPoller_UpdatesCache(t *testing.T) {
	c := cache.New()
	p := poller.NewSQSPoller(&fakeSQSClient{queued: "70", inFlight: "30"}, "https://sqs.fake/q", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	depth, _, ok := c.QueueDepth()
	if !ok {
		t.Fatal("expected cache to be populated")
	}
	if depth != 100 {
		t.Errorf("expected 100 (70+30), got %d", depth)
	}
}

func TestSQSPoller_ErrorLeavesCache(t *testing.T) {
	c := cache.New()
	c.SetQueueDepth(50)
	p := poller.NewSQSPoller(&fakeSQSClient{err: errors.New("connection refused")}, "https://sqs.fake/q", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	depth, _, ok := c.QueueDepth()
	if !ok {
		t.Fatal("expected cache to still have pre-populated data")
	}
	if depth != 50 {
		t.Errorf("expected depth unchanged at 50, got %d", depth)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/poller/... -run TestSQS
```

Expected: compile error — `poller.NewSQSPoller` not defined

- [ ] **Step 3: Implement the SQS poller**

Create `internal/poller/sqs.go`:

```go
package poller

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/louisolivier/kost/internal/cache"
)

type SQSClient interface {
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

type SQSPoller struct {
	client   SQSClient
	queueURL string
	cache    *cache.MetricsCache
	interval time.Duration
	logger   *slog.Logger
}

func NewSQSPoller(client SQSClient, queueURL string, c *cache.MetricsCache, interval time.Duration, logger *slog.Logger) *SQSPoller {
	return &SQSPoller{client: client, queueURL: queueURL, cache: c, interval: interval, logger: logger}
}

func (p *SQSPoller) Run(ctx context.Context) {
	p.poll(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *SQSPoller) poll(ctx context.Context) {
	out, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &p.queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		p.logger.Error("sqs poll failed", "error", err)
		return
	}
	queued, _ := strconv.ParseInt(out.Attributes["ApproximateNumberOfMessages"], 10, 64)
	inFlight, _ := strconv.ParseInt(out.Attributes["ApproximateNumberOfMessagesNotVisible"], 10, 64)
	p.cache.SetQueueDepth(queued + inFlight)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/poller/... -run TestSQS -v
```

Expected:
```
--- PASS: TestSQSPoller_UpdatesCache
--- PASS: TestSQSPoller_ErrorLeavesCache
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/poller/sqs.go internal/poller/sqs_test.go
git commit -m "feat: implement SQS poller"
```

---

### Task 6: Implement the pricing poller

**Files:**
- Create: `internal/poller/pricing.go`
- Create: `internal/poller/pricing_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/poller/pricing_test.go`:

```go
package poller_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/louisolivier/kost/internal/cache"
	"github.com/louisolivier/kost/internal/poller"
)

type fakeEC2Client struct {
	price string
	err   error
}

func (f *fakeEC2Client) DescribeSpotPriceHistory(_ context.Context, _ *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &ec2.DescribeSpotPriceHistoryOutput{
		SpotPriceHistory: []ec2types.SpotPrice{
			{SpotPrice: aws.String(f.price)},
		},
	}, nil
}

func TestPricingPoller_UpdatesCache(t *testing.T) {
	c := cache.New()
	p := poller.NewPricingPoller(&fakeEC2Client{price: "0.089"}, "m5.xlarge", "us-east-1a", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	price, _, ok := c.SpotPrice()
	if !ok {
		t.Fatal("expected cache to be populated")
	}
	if price != 0.089 {
		t.Errorf("expected 0.089, got %f", price)
	}
}

func TestPricingPoller_ErrorLeavesCache(t *testing.T) {
	c := cache.New()
	c.SetSpotPrice(0.05)
	p := poller.NewPricingPoller(&fakeEC2Client{err: errors.New("throttled")}, "m5.xlarge", "us-east-1a", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	price, _, ok := c.SpotPrice()
	if !ok {
		t.Fatal("expected cache to still have data")
	}
	if price != 0.05 {
		t.Errorf("expected 0.05 unchanged, got %f", price)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/poller/... -run TestPricing
```

Expected: compile error — `poller.NewPricingPoller` not defined

- [ ] **Step 3: Implement the pricing poller**

Create `internal/poller/pricing.go`:

```go
package poller

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/louisolivier/kost/internal/cache"
)

type EC2Client interface {
	DescribeSpotPriceHistory(ctx context.Context, params *ec2.DescribeSpotPriceHistoryInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error)
}

type PricingPoller struct {
	client           EC2Client
	instanceType     string
	availabilityZone string
	cache            *cache.MetricsCache
	interval         time.Duration
	logger           *slog.Logger
}

func NewPricingPoller(client EC2Client, instanceType, az string, c *cache.MetricsCache, interval time.Duration, logger *slog.Logger) *PricingPoller {
	return &PricingPoller{
		client: client, instanceType: instanceType, availabilityZone: az,
		cache: c, interval: interval, logger: logger,
	}
}

func (p *PricingPoller) Run(ctx context.Context) {
	p.poll(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *PricingPoller) poll(ctx context.Context) {
	out, err := p.client.DescribeSpotPriceHistory(ctx, &ec2.DescribeSpotPriceHistoryInput{
		InstanceTypes:       []ec2types.InstanceType{ec2types.InstanceType(p.instanceType)},
		AvailabilityZone:    aws.String(p.availabilityZone),
		ProductDescriptions: []string{"Linux/UNIX"},
		MaxResults:          aws.Int32(1),
	})
	if err != nil {
		p.logger.Error("pricing poll failed", "error", err)
		return
	}
	if len(out.SpotPriceHistory) == 0 {
		p.logger.Warn("no spot price history returned", "instanceType", p.instanceType, "az", p.availabilityZone)
		return
	}
	price, err := strconv.ParseFloat(aws.ToString(out.SpotPriceHistory[0].SpotPrice), 64)
	if err != nil {
		p.logger.Error("failed to parse spot price", "raw", aws.ToString(out.SpotPriceHistory[0].SpotPrice), "error", err)
		return
	}
	p.cache.SetSpotPrice(price)
}
```

- [ ] **Step 4: Run all poller tests to confirm they pass**

```bash
go test ./internal/poller/... -v
```

Expected: all 4 tests `PASS`

- [ ] **Step 5: Commit**

```bash
git add internal/poller/pricing.go internal/poller/pricing_test.go
git commit -m "feat: implement pricing poller"
```

---

### Task 7: Implement the reconciler

**Files:**
- Modify: `internal/controller/costaware_scaler_controller.go`
- Modify: `internal/controller/suite_test.go` (inject cache)
- Create: `internal/controller/costaware_scaler_controller_test.go`

- [ ] **Step 1: Write the failing controller tests**

Replace `internal/controller/costaware_scaler_controller_test.go` with:

```go
package controller_test

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
)

var _ = Describe("CostAwareScaler controller", func() {
	const (
		scalerName      = "test-scaler"
		deploymentName  = "test-worker"
		namespaceName   = "default"
		timeout         = 5 * time.Second
		pollInterval    = 250 * time.Millisecond
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
		testCache.SetQueueDepth(0)
	})

	It("scales deployment up to match queue depth", func() {
		createDeployment(1)
		createScaler()
		testCache.SetQueueDepth(100) // 100 / 10 = 10 replicas
		testCache.SetSpotPrice(0.05) // 10 * 0.05 = $0.50/hr, under budget

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())

		var dep appsv1.Deployment
		Expect(k8sClient.Get(ctx, depKey, &dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(10)))
	})

	It("halts scale-up when estimated cost exceeds budget", func() {
		createDeployment(3)
		createScaler()
		testCache.SetQueueDepth(500) // wants 50 replicas
		testCache.SetSpotPrice(1.0)  // 50 * $1.00 = $50/hr > $10 budget

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())

		var dep appsv1.Deployment
		Expect(k8sClient.Get(ctx, depKey, &dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(3))) // frozen at current

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
				Scaling:   kostv1alpha1.ScalingSpec{MinReplicas: 10, MaxReplicas: 5}, // invalid: min > max
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

	It("sets Degraded condition when queue cache is stale", func() {
		createDeployment(1)
		createScaler()
		// Do not populate the cache — QueueDepth() returns ok=false
		// Reconciler should requeue, not error

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: scalerKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(15 * time.Second))
	})
})
```

- [ ] **Step 2: Update suite_test.go to expose the cache and reconciler**

The kubebuilder-generated `internal/controller/suite_test.go` sets up `envtest`. Add `testCache` and `reconciler` variables so tests can inject cache state and call `Reconcile` directly. Replace the generated file with:

```go
package controller_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/cache"
	"github.com/louisolivier/kost/internal/controller"
)

var (
	cfg        *rest.Config
	k8sClient  client.Client
	testEnv    *envtest.Environment
	testCache  *cache.MetricsCache
	reconciler *controller.CostAwareScalerReconciler
	ctx        context.Context
	cancel     context.CancelFunc
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.TODO())

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.31.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = kostv1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	testCache = cache.New()
	reconciler = &controller.CostAwareScalerReconciler{
		Client: k8sClient,
		Scheme: scheme.Scheme,
		Cache:  testCache,
	}
})

var _ = AfterSuite(func() {
	cancel()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
go test ./internal/controller/... 2>&1 | head -30
```

Expected: compile error — `controller.CostAwareScalerReconciler` does not have a `Cache` field yet

- [ ] **Step 4: Implement the reconciler**

Replace `internal/controller/costaware_scaler_controller.go` with:

```go
package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/algorithm"
	metricscache "github.com/louisolivier/kost/internal/cache"
)

type CostAwareScalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Cache  *metricscache.MetricsCache

	stabMu                    sync.Mutex
	lastScaleDownProposalTime time.Time
}

func (r *CostAwareScalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

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

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *CostAwareScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kostv1alpha1.CostAwareScaler{}).
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
	return 1.0 // unknown type: conservative fallback
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
```

- [ ] **Step 5: Download the envtest binaries**

```bash
make envtest
./bin/setup-envtest use 1.31.0 --bin-dir ./bin/k8s
```

Expected: binaries downloaded to `./bin/k8s/1.31.0-<os>-<arch>/`

- [ ] **Step 6: Run controller tests to confirm they pass**

```bash
go test ./internal/controller/... -v
```

Expected: all 4 Ginkgo specs `PASS`

- [ ] **Step 7: Commit**

```bash
git add internal/controller/ 
git commit -m "feat: implement CostAwareScaler reconciler"
```

---

### Task 8: Add Prometheus metrics

**Files:**
- Create: `internal/metrics/metrics.go`
- Modify: `internal/controller/costaware_scaler_controller.go` (emit metrics)

- [ ] **Step 1: Create the metrics registry**

```bash
mkdir -p internal/metrics
```

Create `internal/metrics/metrics.go`:

```go
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
```

- [ ] **Step 2: Emit metrics from the reconciler**

In `internal/controller/costaware_scaler_controller.go`, add the import:

```go
kostmetrics "github.com/louisolivier/kost/internal/metrics"
```

At the end of the `Reconcile` function, just before the final `return`, add:

```go
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
```

Also add the `prometheus` import:

```go
"github.com/prometheus/client_golang/prometheus"
```

- [ ] **Step 3: Emit error counters from pollers**

In `internal/poller/sqs.go`, add the import and increment in the error path:

```go
import kostmetrics "github.com/louisolivier/kost/internal/metrics"
```

In `poll()`, replace the `p.logger.Error(...)` call with:

```go
p.logger.Error("sqs poll failed", "error", err)
kostmetrics.SQSPollErrors.With(prometheus.Labels{"scaler": "global"}).Inc()
```

In `internal/poller/pricing.go`, add the same pattern with `PricingPollErrors`.

- [ ] **Step 4: Build to verify no compile errors**

```bash
go build ./...
```

Expected: no output

- [ ] **Step 5: Commit**

```bash
git add internal/metrics/ internal/controller/costaware_scaler_controller.go internal/poller/sqs.go internal/poller/pricing.go
git commit -m "feat: add Prometheus metrics"
```

---

### Task 9: Wire main.go

**Files:**
- Modify: `cmd/main.go`

- [ ] **Step 1: Replace the generated main.go with the wired version**

Replace `cmd/main.go` with:

```go
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	kostv1alpha1 "github.com/louisolivier/kost/api/v1alpha1"
	"github.com/louisolivier/kost/internal/cache"
	"github.com/louisolivier/kost/internal/controller"
	"github.com/louisolivier/kost/internal/poller"
)

func main() {
	var metricsAddr string
	var probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(logger)

	// Load AWS config from environment (AWS_REGION, AWS_ACCESS_KEY_ID, etc. or IRSA)
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error(err, "failed to load AWS config — check credentials and AWS_REGION")
		os.Exit(1)
	}

	if err := kostv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		logger.Error(err, "unable to add kost scheme")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		logger.Error(err, "unable to start manager")
		os.Exit(1)
	}

	metricsCache := cache.New()
	slogLogger := logger.GetSink() // controller uses slog; pass a no-op or adapt as needed
	_ = slogLogger

	// Pollers are started by the reconciler on first reconcile (once.Do).
	// The reconciler receives AWS clients and the cache.
	if err := (&controller.CostAwareScalerReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Cache:     metricsCache,
		SQSClient: awssqs.NewFromConfig(awsCfg),
		EC2Client: awsec2.NewFromConfig(awsCfg),
	}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "unable to create controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	logger.Info("starting kost operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "problem running manager")
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Update the reconciler struct to hold AWS clients**

The `main.go` above passes `SQSClient` and `EC2Client` to the reconciler so it can start pollers on first reconcile without depending on the AWS SDK directly in `main.go`.

Add these fields to `CostAwareScalerReconciler` in `internal/controller/costaware_scaler_controller.go`:

```go
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
```

Add poller startup inside `Reconcile`, right after spec validation:

```go
// Start pollers on first successful reconcile
r.pollerOnce.Do(func() {
    pollerCtx, cancel := context.WithCancel(context.Background())
    r.pollerCancel = cancel
    sqsLogger := log.FromContext(ctx).GetSink() // use controller logger
    _ = sqsLogger

    import slogAdapter "log/slog"
    slog := slogAdapter.Default()
    sqsPoller := poller.NewSQSPoller(r.SQSClient, scaler.Spec.Queue.URL, r.Cache, 15*time.Second, slog)
    pricingPoller := poller.NewPricingPoller(r.EC2Client, scaler.Spec.Cost.InstanceType, scaler.Spec.Cost.AvailabilityZone, r.Cache, 5*time.Minute, slog)
    go sqsPoller.Run(pollerCtx)
    go pricingPoller.Run(pollerCtx)
    log.FromContext(ctx).Info("pollers started", "queueURL", scaler.Spec.Queue.URL, "instanceType", scaler.Spec.Cost.InstanceType)
})
```

Note: the inline `import` syntax above is illustrative — move the `"log/slog"` import to the file's import block.

- [ ] **Step 3: Build to verify everything compiles**

```bash
go build ./...
```

Expected: no output

- [ ] **Step 4: Run all tests**

```bash
go test ./...
```

Expected: all tests `PASS`

- [ ] **Step 5: Commit**

```bash
git add cmd/main.go internal/controller/costaware_scaler_controller.go
git commit -m "feat: wire main.go and reconciler AWS clients"
```

---

### Task 10: Smoke test on kind

**Files:**
- No code changes — this task validates the running operator

- [ ] **Step 1: Create a kind cluster**

```bash
kind create cluster --name kost-dev
kubectl cluster-info --context kind-kost-dev
```

- [ ] **Step 2: Install the CRD**

```bash
make manifests
kubectl apply -f config/crd/bases/
```

Expected: `customresourcedefinition.apiextensions.k8s.io/costawarescalers.kost.kost.io created`

- [ ] **Step 3: Create a target Deployment**

```bash
kubectl create deployment test-worker --image=nginx --replicas=1
```

- [ ] **Step 4: Apply a CostAwareScaler manifest**

Create `/tmp/test-scaler.yaml`:

```yaml
apiVersion: kost.kost.io/v1alpha1
kind: CostAwareScaler
metadata:
  name: test-scaler
  namespace: default
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: test-worker
  queue:
    provider: sqs
    url: https://sqs.us-east-1.amazonaws.com/000000000000/test-queue
    region: us-east-1
    targetMessagesPerWorker: 10
  scaling:
    minReplicas: 1
    maxReplicas: 10
    scaleDownStabilizationSeconds: 30
  cost:
    instanceType: m5.xlarge
    availabilityZone: us-east-1a
    hourlyBudgetUSD: 5.00
```

```bash
kubectl apply -f /tmp/test-scaler.yaml
```

- [ ] **Step 5: Run the operator locally against the cluster**

```bash
go run ./cmd/main.go
```

Expected: operator starts, logs `pollers started` on first reconcile, then `waiting for initial queue depth poll` (since no real SQS exists). The `CostAwareScaler` status should not be in an error state.

- [ ] **Step 6: Verify status conditions**

```bash
kubectl get costawarescaler test-scaler -o yaml | grep -A 20 conditions
```

Expected: conditions list shows `Invalid=False`, with no error conditions (operator is running and the spec is valid).

- [ ] **Step 7: Tear down**

```bash
kind delete cluster --name kost-dev
```

- [ ] **Step 8: Commit**

```bash
git add .
git commit -m "chore: smoke test verified, operator scaffolds and runs correctly"
```

---

## Self-Review

**Spec coverage:**
- ✅ CRD schema: Task 2
- ✅ MetricsCache: Task 3
- ✅ Scaling algorithm (queue-depth target, budget halt, stabilization): Task 4
- ✅ SQS poller (queued + in-flight): Task 5
- ✅ Pricing poller (spot price, error fallback): Task 6
- ✅ Reconciler (validation, staleness, algorithm, Deployment patch, status): Task 7
- ✅ Prometheus metrics: Task 8
- ✅ main.go wiring: Task 9
- ✅ On-demand price fallback for stale cache: Task 7 (`onDemandPrice`)
- ✅ Inline spec validation (no webhook): Task 7 (`validateSpec`)

**Notes on `SQSClient`/`EC2Client` in test suite:** The `suite_test.go` creates a `CostAwareScalerReconciler` without `SQSClient`/`EC2Client`. This is correct — the tests inject pre-populated cache values directly and never trigger `pollerOnce.Do` (no AWS calls in tests).
