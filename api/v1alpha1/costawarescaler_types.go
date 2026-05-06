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
