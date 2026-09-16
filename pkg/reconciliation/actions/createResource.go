package actions

import (
	"context"

	"github.com/jnnkrdb/r8r/pkg/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (rr *ResourceRequest) Create(ctx context.Context, statushandler *status.StatusHandler) error {

	// get the logger from the context
	var _log = logf.FromContext(ctx).WithValues("func", "actions.(*RequestedResource).Create()")

	_log.Info("creating resource")

	rr.Resource.SetNamespace(rr.Namespace.Name)

	if err := rr.SetControllerIfAny(statushandler); err != nil {
		return err
	}

	// create the object in the cluster
	err := statushandler.GetReconciler().GetClient().Create(ctx, rr.Resource, &client.CreateOptions{})

	return statushandler.ThrowEventWithConditionOnError(
		err,
		nil,
		status.EventType_Warning,
		"ObjectCreation",
		"Error Creating Object",
		"error creating object in namespace: %v", err,
	)
}
