package reconciliation

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/logger"
	"k8s.io/apimachinery/pkg/types"
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

	var _currLog = logf.FromContext(ctx).WithValues(
		"func", "reconciliation.(*ReconciliationHandler).List()",
		"listObject-GroupVersionKind", list.GetObjectKind().GroupVersionKind().String(),
	)

	if err := rh.reconciler.GetClient().List(ctx, list, opts...); err != nil {

		_currLog.Error(err, "failed to fetch list of object")

		// throw an event
		rh.reconciler.GetRecorder().Eventf(
			rh.Obj, list, "Waring", "FailedObjectListFetching", "FetchingObjectList",
			fmt.Sprintf("Unable to fetch a list of object [%s], due to following error: %s",
				list.GetObjectKind().GroupVersionKind().String(),
				err.Error(),
			),
		)

		return err
	}

	return nil
}

// generic get function
func (rh *ReconciliationHandler) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {

	var _currLog = logf.FromContext(ctx).WithValues(
		"func", "reconciliation.(*ReconciliationHandler).Get()",
		"object-GroupVersionKind", obj.GetObjectKind().GroupVersionKind().String(),
		"object-NamespaceName", key.String(),
	)

	if err := rh.reconciler.GetClient().Get(ctx, key, obj, opts...); err != nil {

		_currLog.Error(err, "failed to fetch object")

		// throw an event
		rh.reconciler.GetRecorder().Eventf(
			rh.Obj, obj, "Waring", "FailedObjectFetching", "FetchingObject",
			fmt.Sprintf("Unable to fetch an object [%s@%s/%s], due to following error: %s",
				obj.GetObjectKind().GroupVersionKind().String(), key.Namespace, key.Name,
				err.Error(),
			),
		)
		return err
	}

	return nil
}

// generic create function
func (rh *ReconciliationHandler) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {

	var _currLog = logf.FromContext(ctx).WithValues(
		"func", "reconciliation.(*ReconciliationHandler).Create()",
		"object-GroupVersionKind", obj.GetObjectKind().GroupVersionKind().String(),
	)

	if err := rh.reconciler.GetClient().Create(ctx, obj, opts...); err != nil {

		_currLog.Error(err, "failed to create object")

		// throw an event
		rh.reconciler.GetRecorder().Eventf(
			rh.Obj, obj, "Waring", "FailedObjectCreation", "CreatingObject",
			fmt.Sprintf("Unable to create an object [%s@%s/%s], due to following error: %s",
				obj.GetObjectKind().GroupVersionKind().String(), obj.GetNamespace(), obj.GetName(),
				err.Error(),
			),
		)
		return err
	}

	return nil
}

// generic update function
func (rh *ReconciliationHandler) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {

	var _currLog = logf.FromContext(ctx).WithValues(
		"func", "reconciliation.(*ReconciliationHandler).Update()",
		"object-GroupVersionKind", obj.GetObjectKind().GroupVersionKind().String(),
	)

	if err := rh.reconciler.GetClient().Update(ctx, obj, opts...); err != nil {

		_currLog.Error(err, "failed to update object")

		// throw an event
		rh.reconciler.GetRecorder().Eventf(
			rh.Obj, obj, "Waring", "FailedObjectUpdate", "UpdatingObject",
			fmt.Sprintf("Unable to update an object [%s@%s/%s], due to following error: %s",
				obj.GetObjectKind().GroupVersionKind().String(), obj.GetNamespace(), obj.GetName(),
				err.Error(),
			),
		)
		return err
	}

	return nil
}

// generic delete function
func (rh *ReconciliationHandler) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {

	var _currLog = logf.FromContext(ctx).WithValues(
		"func", "reconciliation.(*ReconciliationHandler).Delete()",
		"object-GroupVersionKind", obj.GetObjectKind().GroupVersionKind().String(),
	)

	if err := rh.reconciler.GetClient().Delete(ctx, obj, opts...); err != nil {

		_currLog.Error(err, "failed to delete object")

		// throw an event
		rh.reconciler.GetRecorder().Eventf(
			rh.Obj, obj, "Waring", "FailedObjectDeletion", "DeletingObject",
			fmt.Sprintf("Unable to delete an object [%s@%s/%s], due to following error: %s",
				obj.GetObjectKind().GroupVersionKind().String(), obj.GetNamespace(), obj.GetName(),
				err.Error(),
			),
		)
		return err
	}

	return nil
}

// check whether an object exists or not
func (rh *ReconciliationHandler) DoesObjectExist(ctx context.Context, obj client.Object) (bool, error) {

	if err := rh.Get(ctx, types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}, obj); err != nil {

		return false, client.IgnoreNotFound(err)
	}

	return true, nil
}
