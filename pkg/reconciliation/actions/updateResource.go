package actions

import (
	"context"

	"github.com/jnnkrdb/r8r/pkg/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (rr *ResourceRequest) Update(ctx context.Context, statushandler *status.StatusHandler) error {

	// get the logger from the context
	var _log = logf.FromContext(ctx).WithValues("func", "actions.(*RequestedResource).Update()")

	_log.Info("updating resource")

	rr.Resource.SetNamespace(rr.Namespace.Name)

	if err := rr.SetControllerIfAny(statushandler); err != nil {
		return err
	}

	err := statushandler.GetReconciler().GetClient().Update(ctx, rr.Resource, &client.UpdateOptions{})

	return statushandler.ThrowEventWithConditionOnError(
		err,
		nil,
		status.EventType_Warning,
		"ObjectUpdate",
		"Error Updating Object",
		"error updating object in namespace: %v", err,
	)
}
