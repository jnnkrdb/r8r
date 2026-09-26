package reconciliation

import (
	"context"

	"github.com/jnnkrdb/r8r/pkg/logger"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// this struct is made to simplify the requests, which will be made during a reconciliation loop
// since there are multiple requests, which will be done during a reconciliation, which may
// may be duplicated, this handler contains a predefined request, which can be used
type ReconciliationHandler struct {
	reconciler Reconciler
	eventLog   logger.EventHandler
	Obj        client.Object
}

// create a new handler instance from a given reconciler
func NewReconciliationHandler(ctx context.Context, rd Reconciler, pObj client.Object) *ReconciliationHandler {

	return &ReconciliationHandler{
		reconciler: rd,
		Obj:        pObj,
		eventLog:   logger.NewEventLogger(rd.GetRecorder(), logf.FromContext(ctx), pObj),
	}
}

// generic list function
func (rh *ReconciliationHandler) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {

	_log := rh.eventLog.WithValues(
		"func", "reconciliation.(*ReconciliationHandler).List()",
		"listObject-GroupVersionKind", list.GetObjectKind().GroupVersionKind().String(),
	)

	if err := rh.reconciler.GetClient().List(ctx, list, opts...); err != nil {

		// throw an event
		_log.ErrorWithEvent(
			err,
			"error fetching objectList",
			logger.Event{
				EventType: logger.Warning,
				Action:    "ObjectListFetching",
			},
		)

		return err
	}

	return nil
}

// generic get function
func (rh *ReconciliationHandler) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {

	err := rh.reconciler.GetClient().Get(ctx, key, obj, opts...)

	return err
}

// generic create function
func (rh *ReconciliationHandler) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {

	err := rh.reconciler.GetClient().Create(ctx, obj, opts...)

	return err
}

// generic update function
func (rh *ReconciliationHandler) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {

	err := rh.reconciler.GetClient().Update(ctx, obj, opts...)

	return err
}

// generic delete function
func (rh *ReconciliationHandler) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {

	err := rh.reconciler.GetClient().Delete(ctx, obj, opts...)

	return err
}
