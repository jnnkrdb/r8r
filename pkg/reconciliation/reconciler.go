package reconciliation

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/generics"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
func NewReconciliationHandler(rd Reconciler, pObj client.Object) *ReconciliationHandler {
	return &ReconciliationHandler{
		reconciler:        rd,
		controllingObject: pObj,
	}
}

// throw an event, that is related to the controllingObject
//   - 'related' is the secondary object for more complex actions. E.g. when regarding object triggers a creation or deletion of related object.
//   - 'type' of this event, and can be one of Normal, Warning. New types could be added in future 'reason' is the reason this event is generated.
//   - 'reason' should be short and unique; it should be in UpperCamelCase format (starting with a capital letter).
//     "reason" will be used to automate handling of events, so imagine people writing switch statements to handle them. You want to make that easy.
//   - 'action' explains what happened with regarding/what action did the ReportingController (ReportingController is a type of a Controller reporting an Event,
//     e.g. k8s.io/node-controller, k8s.io/kubelet.) take in regarding's name; it should be in UpperCamelCase format (starting with a capital letter).
//   - 'note' is intended to be human readable.
func (rh *ReconciliationHandler) ThrowEvent(
	related runtime.Object,
	eventtype string,
	reason string,
	action string,
	note string,
	args ...interface{}) {

	rh.reconciler.GetRecorder().Eventf(rh.controllingObject, related, eventtype, reason, action, note, args...)
}

// raw list function
func (rh *ReconciliationHandler) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return rh.reconciler.GetClient().List(ctx, list, opts...)
}

// list with labelselector
func (rh *ReconciliationHandler) ListWithLabelSelector(
	ctx context.Context,
	list client.ObjectList,
	labelSelector *metav1.LabelSelector,
	opts ...client.ListOption) error {

	ls, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return fmt.Errorf("error converting labelselector(incoming: %v): %v", labelSelector, err)
	}
	return rh.List(ctx, list, append(opts, &client.ListOptions{LabelSelector: ls})...)
}

// create an object list but return the HashMap
func (rh *ReconciliationHandler) ListAsHashMap(
	ctx context.Context,
	list client.ObjectList,
	kfn generics.KeyFunc,
	opts ...client.ListOption) (generics.ObjectHashMap, error) {

	// request the objects list from the cluster
	if err := rh.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("failed to fetch the objectList from the cluster [%v]: %v", list, err)
	}
	// create the hashmap from the returned list and return the outcome
	return generics.ObjectListToMap(list, kfn)
}

// create an object list but return the HashMap with a given labelselector
func (rh *ReconciliationHandler) ListAsHashMapWithLabelSelector(
	ctx context.Context,
	list client.ObjectList,
	kfn generics.KeyFunc,
	labelSelector *metav1.LabelSelector,
	opts ...client.ListOption) (generics.ObjectHashMap, error) {

	// request the objects list from the cluster
	if err := rh.ListWithLabelSelector(ctx, list, labelSelector, opts...); err != nil {
		return nil, fmt.Errorf("failed to fetch the objectList from the cluster [%v]: %v", list, err)
	}
	// create the hashmap from the returned list and return the outcome
	return generics.ObjectListToMap(list, kfn)
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

// get ownership labels
func (rh *ReconciliationHandler) GetOwnerLabels(labels map[string]string) map[string]string {
	labels = generics.MapFromNil(labels)
	labels["r8r.jnnkrdb.de/ownedBy"] = string(rh.controllingObject.GetUID())
	return labels
}

// set ownership for given object
func (rh *ReconciliationHandler) SetOwnership(o client.Object) error {
	o.SetLabels(rh.GetOwnerLabels(o.GetLabels()))
	return controllerutil.SetControllerReference(rh.controllingObject, o, rh.reconciler.GetScheme())
}
