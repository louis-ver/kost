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
	StabilizationWindow       int32
	SecondsSinceLastScaleDown int32
}

type Result struct {
	DesiredReplicas     int32
	EstimatedHourlyCost float64
	BudgetHalted        bool
	StabilizationHeld   bool
	Reason              string
}

func Compute(in Input) Result {
	unclamped := in.MinReplicas
	if in.TargetMessagesPerWorker > 0 && in.QueueDepth > 0 {
		raw := int32(math.Ceil(float64(in.QueueDepth) / float64(in.TargetMessagesPerWorker)))
		if raw > unclamped {
			unclamped = raw
		}
	}
	if unclamped < in.MinReplicas {
		unclamped = in.MinReplicas
	}

	rawDesired := unclamped
	if rawDesired > in.MaxReplicas {
		rawDesired = in.MaxReplicas
	}

	estimatedCost := float64(rawDesired) * in.SpotPrice
	budgetHalted := false
	desired := rawDesired
	if in.HourlyBudgetUSD > 0 && estimatedCost > in.HourlyBudgetUSD && rawDesired > in.CurrentReplicas {
		desired = in.CurrentReplicas
		budgetHalted = true
	}

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
	case desired == in.MaxReplicas && unclamped > in.MaxReplicas:
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
