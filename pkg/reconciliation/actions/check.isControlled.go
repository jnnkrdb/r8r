package actions

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// this function is used to check whether a specific object exists in the cluster or not.
// It is used to verify the existence of the object before performing any operations on it.
func (rr *ResourceRequest) IsControlled() bool {
	return metav1.IsControlledBy(rr.Resource, rr.OwnerResource)
}
