package status

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// these fileds should be used in the status of the objects, which are used in the reconciliation.
type DefaultStatusFields struct {

	// conditions represent the current state of the ClusterObject resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
