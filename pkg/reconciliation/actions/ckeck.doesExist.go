package actions

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/status"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// this function is used to check whether a specific object exists in the cluster or not.
// It is used to verify the existence of the object before performing any operations on it.
func (rr *ResourceRequest) DoesExist(ctx context.Context, statushandler *status.StatusHandler) (bool, error) {
	doesExist, err := VerifyObjectExistence(
		ctx,
		statushandler.GetReconciler().GetClient(),
		rr.Namespace.Name,
		rr.Resource.GetName(),
		rr.Resource.DeepCopy(),
	)

	return doesExist, statushandler.ThrowEventWithConditionOnError(
		err,
		nil,
		status.EventType_Warning,
		"ClusterObjectFetching",
		"Error Fetching ClusterObject",
		"error fetching cluster object from cluster: %v", err,
	)
}

// this function is used to check whether a specific object exists in the cluster or not.
// It is used to verify the existence of the object before performing any operations on it.
func VerifyObjectExistence(
	ctx context.Context,
	c client.Client,
	namespace, name string,
	object *unstructured.Unstructured) (bool, error) {

	// fail early, when the requested scheme is empty
	if object == nil {
		return false, fmt.Errorf("object scheme cannot be empty")
	}

	// request the object from the cluster
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, object); err != nil {

		return false, client.IgnoreNotFound(err)
	}

	return true, nil
}
