package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Common status fields (to be included in the status of resource types).
type CommonStatus struct {
	ObservedGeneration int64              `json:"observedGeneration"`
	LastObservedAt     *metav1.Time       `json:"lastObservedAt,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// Read condition from status; returns nil if condition is not found.
func (s *CommonStatus) GetCondition(condType string) *metav1.Condition {
	var cond *metav1.Condition
	for i := 0; i < len(s.Conditions); i++ {
		if s.Conditions[i].Type == condType {
			if cond != nil {
				panic("this should never happen (encountered condition more than once)")
			}
			cond = &s.Conditions[i]
		}
	}
	return cond
}

// Set (and add, if necessary) condition in status.
// Note: this does not update the condition's ObservedGeneration and LastTransitionTime attributes.
func (s *CommonStatus) SetCondition(condType string, status metav1.ConditionStatus, reason string, message string) *metav1.Condition {
	var cond *metav1.Condition
	for i := 0; i < len(s.Conditions); i++ {
		if s.Conditions[i].Type == condType {
			if cond != nil {
				panic("this should never happen (encountered condition more than once)")
			}
			cond = &s.Conditions[i]
		}
	}
	if cond == nil {
		s.Conditions = append(s.Conditions, metav1.Condition{Type: condType})
		cond = &s.Conditions[len(s.Conditions)-1]
	}
	cond.Status = status
	cond.Reason = reason
	cond.Message = message
	return cond
}

const (
	ReadyCondition = "Ready"
)

// Read 'Ready' condition from status; returns nil if condition is not found.
func (s *CommonStatus) GetReadyCondition() *metav1.Condition {
	return s.GetCondition(ReadyCondition)
}

// Set (and add, if necessary) 'Ready' condition in status.
// Note: this does not update the condition's ObservedGeneration and LastTransitionTime attributes.
func (s *CommonStatus) SetReadyCondition(status metav1.ConditionStatus, reason string, message string) *metav1.Condition {
	return s.SetCondition(ReadyCondition, status, reason, message)
}
