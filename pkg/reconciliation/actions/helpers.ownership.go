package actions

import (
	"github.com/jnnkrdb/r8r/pkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// this function is used to check whether a specific object exists in the cluster or not.
// It is used to verify the existence of the object before performing any operations on it.
func (rr *ResourceRequest) IsControlled() bool {
	return metav1.IsControlledBy(rr.Resource, rr.OwnerResource)
}

// set the owners reference if an owner resource is specified
//
// this is required for watching the dependent objects
func (rr *ResourceRequest) SetControllerIfAny(statushandler *status.StatusHandler) error {

	var err error = nil

	// set the owners reference if an owner resource is specified
	// this is required for watching the dependent objects
	if rr.OwnerResource != nil {
		err = controllerutil.SetControllerReference(
			rr.OwnerResource,
			rr.Resource,
			statushandler.GetReconciler().GetScheme(),
		)
	}

	return statushandler.ThrowEventWithConditionOnError(
		err,
		nil,
		status.EventType_Warning,
		"OwnerReferenceConfiguration",
		"Error Setting Owner Reference",
		"error setting owner reference: %v", err,
	)
}
