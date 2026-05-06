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
