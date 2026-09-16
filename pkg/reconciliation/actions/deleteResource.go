package actions

import (
	"context"

	"github.com/jnnkrdb/r8r/pkg/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (rr *ResourceRequest) Delete(ctx context.Context, statushandler *status.StatusHandler) error {

	// get the logger from the context
	var _log = logf.FromContext(ctx).WithValues("func", "actions.(*RequestedResource).Delete()")

	_log.Info("deleting resource")

	rr.Resource.SetNamespace(rr.Namespace.Name)

	err := statushandler.GetReconciler().GetClient().Delete(ctx, rr.Resource, &client.DeleteOptions{})

	return statushandler.ThrowEventWithConditionOnError(
		client.IgnoreNotFound(err),
		nil,
		status.EventType_Warning,
		"ObjectDeletion",
		"Error Deleting Object",
		"error deleting object in namespace: %v", err,
	)
}
