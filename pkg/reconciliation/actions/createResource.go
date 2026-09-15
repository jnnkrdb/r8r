package actions

import (
	"context"

	"github.com/jnnkrdb/r8r/pkg/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (rr *ResourceRequest) Create(ctx context.Context, statushandler *status.StatusHandler) error {

	// get the logger from the context
	var _log = logf.FromContext(ctx).WithValues("func", "actions.(*RequestedResource).Create()")

	_log.Info("creating resource")

	rr.Resource.SetNamespace(rr.Namespace.Name)

	// set the owners reference if an owner resource is specified
	// this is required for watching the dependent objects
	if rr.OwnerResource != nil {

		if err := controllerutil.SetControllerReference(
			rr.OwnerResource,
			rr.Resource,
			statushandler.GetReconciler().GetScheme(),
		); err != nil {

			return statushandler.ThrowEventWithConditionOnError(
				err,
				nil,
				status.EventType_Warning,
				"OwnerReferenceConfiguration",
				"Error Setting Owner Reference",
				"error setting owner reference: %v", err,
			)
		}
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
