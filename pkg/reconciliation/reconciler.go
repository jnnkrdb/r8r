package reconciliation

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// this implements the reconciler interface, which enables some
// methods to get all possible reconcilers, if required
type Reconciler interface {
	GetClient() client.Client
	GetScheme() *runtime.Scheme
	GetRecorder() events.EventRecorder
}

// this struct is made to simplify the requests, which will be made during a reconciliation loop
// since there are multiple requests, which will be done during a reconciliation, which may
// may be duplicated, this handler contains a predefined request, which can be used
type ReconciliationHandler struct {
	reconciler        Reconciler
	controllingObject client.Object
}

// create a new handler instance from a given reconciler
//   - 'related' is the secondary object for more complex actions. E.g. when regarding object triggers a creation or deletion of related object.
//   - 'type' of this event, and can be one of Normal, Warning. New types could be added in future 'reason' is the reason this event is generated.
//   - 'reason' should be short and unique; it should be in UpperCamelCase format (starting with a capital letter).
//     "reason" will be used to automate handling of events, so imagine people writing switch statements to handle them. You want to make that easy.
//   - 'action' explains what happened with regarding/what action did the ReportingController (ReportingController is a type of a Controller reporting an Event,
//     e.g. k8s.io/node-controller, k8s.io/kubelet.) take in regarding's name; it should be in UpperCamelCase format (starting with a capital letter).
//   - 'note' is intended to be human readable.
func NewReconciliationHandler(rd Reconciler, pObj client.Object) *ReconciliationHandler {
	return &ReconciliationHandler{
		reconciler:        rd,
		controllingObject: pObj,
	}
}

// throw an event, that is related to the controllingObject
func (rh *ReconciliationHandler) ThrowEvent(
	related runtime.Object, eventtype string, reason string, action string, note string, args ...interface{}) {
	rh.reconciler.GetRecorder().Eventf(rh.controllingObject, related, eventtype, reason, action, note, args...)
}

// raw list function
func (rh *ReconciliationHandler) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return rh.reconciler.GetClient().List(ctx, list, opts...)
}

// raw get function
func (rh *ReconciliationHandler) Get(
	ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return rh.reconciler.GetClient().Get(ctx, key, obj, opts...)
}

// raw create function
func (rh *ReconciliationHandler) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return rh.reconciler.GetClient().Create(ctx, obj, opts...)
}

// raw update function
func (rh *ReconciliationHandler) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return rh.reconciler.GetClient().Update(ctx, obj, opts...)
}

// raw delete function
func (rh *ReconciliationHandler) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return rh.reconciler.GetClient().Delete(ctx, obj, opts...)
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
