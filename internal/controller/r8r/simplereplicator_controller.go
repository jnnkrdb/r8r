/*
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package r8r

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	r8rv1beta1 "github.com/jnnkrdb/r8r/api/r8r/v1beta1"
	"github.com/jnnkrdb/r8r/pkg/generics"
	"github.com/jnnkrdb/r8r/pkg/reconciliation"
	"github.com/jnnkrdb/r8r/pkg/reconciliation/helpers"
)

// SimpleReplicatorReconciler reconciles a SimpleReplicator object
type SimpleReplicatorReconciler struct {
	Client   client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

func (r *SimpleReplicatorReconciler) GetClient() client.Client          { return r.Client }
func (r *SimpleReplicatorReconciler) GetScheme() *runtime.Scheme        { return r.Scheme }
func (r *SimpleReplicatorReconciler) GetRecorder() events.EventRecorder { return r.Recorder }

// SetupWithManager sets up the controller with the Manager.
func (r *SimpleReplicatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&r8rv1beta1.SimpleReplicator{}).
		Named("r8r-simplereplicator").
		WithEventFilter(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				predicate.ResourceVersionChangedPredicate{},
			),
		).
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) (requests []reconcile.Request) {
					var _log = logf.FromContext(ctx)
					// trigger reconciliation for all clusterobjects
					var list = &r8rv1beta1.SimpleReplicatorList{}
					if err := mgr.GetClient().List(ctx, list, &client.ListOptions{}); err != nil {
						_log.Error(err, "error receiving list of simplereplicators, cannot invoke reconciliation")
						return
					}
					for _, simplereplicator := range list.Items {
						requests = append(requests, reconcile.Request{
							NamespacedName: types.NamespacedName{
								Name: simplereplicator.Name,
							},
						})
					}
					return
				},
			),
		).
		WithEventFilter(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				predicate.ResourceVersionChangedPredicate{},
				predicate.LabelChangedPredicate{},
			),
		).
		Complete(r)
}

// +kubebuilder:rbac:groups=r8r.jnnkrdb.de,resources=simplereplicators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=r8r.jnnkrdb.de,resources=simplereplicators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=r8r.jnnkrdb.de,resources=simplereplicators/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the SimpleReplicator object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *SimpleReplicatorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var _baseLog = logf.FromContext(ctx)

	_baseLog.Info("reconciling SimpleReplicator object", "req", req, "ctx", ctx)

	// create cached object of requested resource
	var simpleReplicator = &r8rv1beta1.SimpleReplicator{}
	if err := r.GetClient().Get(ctx, req.NamespacedName, simpleReplicator, &client.GetOptions{}); err != nil {
		_baseLog.Error(err, "error fetching object from cluster")
		return ctrl.Result{}, err
	}
	_baseLog.V(5).Info("simpleReplicator content", "*simpleReplicator", *simpleReplicator)

	// create the reconciliation handler
	var reconciler = reconciliation.NewReconciliationHandler(r, simpleReplicator)
	_baseLog.V(5).Info("initialized reconciler", "*reconciler", *reconciler)

	// request a list of namespaces, to parse through the list and
	// then check every namespace with the give item
	allNamespacesHashMap, err := reconciler.ListAsHashMap(ctx, &corev1.NamespaceList{}, generics.KeyFuncNamespaceName)
	if err != nil {
		return ctrl.Result{}, err
	}
	_baseLog.V(5).Info("allNamespacesHashMap content", "allNamespacesHashMap", allNamespacesHashMap)

	//var allNamespaces = &corev1.NamespaceList{}
	//if err := reconciler.List(ctx, allNamespaces); err != nil {
	//	return ctrl.Result{}, err
	//}
	//_baseLog.V(5).Info("allNamespaces content", "*allNamespaces", *allNamespaces)
	//
	//allNamespacesHashMap, err := generics.ObjectListToMap(allNamespaces, func(o client.Object) string {
	//	return o.GetName()
	//})
	//if err != nil {
	//	return ctrl.Result{}, err
	//}
	//_baseLog.V(5).Info("allNamespacesHashMap content", "allNamespacesHashMap", allNamespacesHashMap)

	// request a list of namespaces, which are required to inherit the defined object
	requiredNamespacesHashMap, err := reconciler.ListAsHashMapWithLabelSelector(
		ctx, &corev1.NamespaceList{}, generics.KeyFuncNamespaceName, simpleReplicator.Spec.Selector.LabelSelector)
	if err != nil {
		return ctrl.Result{}, err
	}
	_baseLog.V(5).Info("requiredNamespacesHashMap content", "requiredNamespacesHashMap", requiredNamespacesHashMap)

	//var requiredNamespaces = &corev1.NamespaceList{}
	//if err := reconciler.ListWithLabelSelector(
	//	ctx, requiredNamespaces, simpleReplicator.Spec.Selector.LabelSelector); err != nil {
	//	return ctrl.Result{}, err
	//}
	//_baseLog.V(5).Info("requiredNamespaces content", "*requiredNamespaces", *requiredNamespaces)
	//
	//requiredNamespacesHashMap, err := generics.ObjectListToMap(requiredNamespaces, func(o client.Object) string {
	//	return o.GetName()
	//})
	//if err != nil {
	//	return ctrl.Result{}, err
	//}
	//_baseLog.V(5).Info("requiredNamespacesHashMap content", "requiredNamespacesHashMap", requiredNamespacesHashMap)

	// for every resource in the reources list, check if it has to be created/updated/deleted in any namespace
	for _, _resource := range simpleReplicator.Spec.Resources {

		var _resourceLog = _baseLog.WithValues(
			"currentResource.GroupVersionKind", _resource.GroupVersionKind().String(),
			"currentResource.Name", _resource.GetName(),
		)
		_resourceLog.Info("handling resource")

		// get resources, that are directly linked to the simplereplicator, due to set labels
		subresourcesHashMap, err := reconciler.ListAsHashMapWithLabelSelector(
			ctx, helpers.UnstructuredListFromObject(_resource), generics.KeyFuncNamespaceName, &metav1.LabelSelector{
				MatchLabels: reconciler.GetOwnerLabels(_resource.GetLabels()),
			})
		if err != nil {
			return ctrl.Result{}, err
		}
		_resourceLog.V(5).Info("subresourcesHashMap content", "subresourcesHashMap", subresourcesHashMap)

		//var unstructuredList = helpers.UnstructuredListFromObject(_resource)
		//if err := reconciler.ListWithLabelSelector(ctx, unstructuredList, &metav1.LabelSelector{
		//	MatchLabels: reconciler.GetOwnerLabels(_resource.GetLabels()),
		//}); err != nil {
		//	return ctrl.Result{}, err
		//}
		//_resourceLog.V(5).Info("unstructuredList content", "*unstructuredList", *unstructuredList)
		//
		//// parse the list to a map type to check like in a hashmap
		//subresourcesHashMap, err := generics.ObjectListToMap(unstructuredList, func(o client.Object) string {
		//	return o.GetNamespace() + "/" + o.GetName()
		//})
		//if err != nil {
		//	return ctrl.Result{}, err
		//}
		//_resourceLog.V(5).Info("subresourcesHashMap content", "subresourcesHashMap", subresourcesHashMap)

		// parse through all namespaces in the cluster and check, whether
		// the object has to be created, updated or deleted from the namespace.
		for _, _namespace := range allNamespacesHashMap {

			var _namespaceLog = _resourceLog.WithValues("destinationNamespace", _namespace.GetName())

			// verify, whether an object already exists in the cluster, that is related to the simpleReplicator object
			var doesExist bool = false
			_, doesExist = subresourcesHashMap[_namespace.GetName()+"/"+_resource.GetName()]

			// verify whether an resource does exist and should exist
			//doesExist, err := reconciler.DoesObjectExist(ctx, subResource)
			//if err != nil {
			//	return ctrl.Result{}, err
			//}

			var shouldExist bool = false
			_, shouldExist = requiredNamespacesHashMap[_namespace.GetName()]

			//for _, _item := range requiredNamespaces.Items {
			//	if shouldExist = (_item.GetName() == _namespace.GetName()); shouldExist {
			//		break
			//	}
			//}

			_namespaceLog.V(3).Info("live-state/desired-state for resource calculated",
				"shouldExist", shouldExist,
				"doesExist", doesExist,
			)

			// case 1: resource should not exist and does not exist -> ignore
			if !shouldExist && !doesExist {
				_namespaceLog.V(3).Info("nothing has to be done")
				continue
			}

			// prepare subResource for cluster write interactions
			subResource, err := reconciler.CreateSubResource(_resource, _namespace.GetName())
			if err != nil {
				return ctrl.Result{}, err
			}
			_namespaceLog.V(5).Info("subResource content", "*subResource", subResource)

			// case 2: resource should exist, but does not exist -> create
			if shouldExist && !doesExist {
				if err := reconciler.Create(ctx, subResource); err != nil {
					return ctrl.Result{}, err
				}
				continue
			}

			// if the object does exist, and either should be updated or deleted,
			// check if the owner is in fact the clusterobject
			// TODO: check whether this is the correct place to check if the object is owned by the simplereplicator object
			if !metav1.IsControlledBy(subResource, simpleReplicator) {
				_namespaceLog.Info("object does not contain ownerreference")
				continue
			}

			// case 3: resource should exist and does exist and is owned by parent resources -> update
			if shouldExist && doesExist {
				if err := reconciler.Update(ctx, subResource); err != nil {
					return ctrl.Result{}, err
				}
				continue
			}

			// case 4: resource should not exist, does exist and is owned by parent resource -> delete
			if !shouldExist && doesExist {
				if err := reconciler.Delete(ctx, subResource); err != nil {
					return ctrl.Result{}, err
				}
				continue
			}
		}
	}

	_baseLog.Info("successfully created resource-replicas in required namespaces")

	r.GetRecorder().Eventf(
		simpleReplicator, nil,
		"Normal", "ResourceReplication",
		"ReplicateResources", "successfully finished reconciliation of simplereplicator resources.")

	return ctrl.Result{}, nil
	//statushandler.SetCondition(metav1.Condition{
	//	Type:    status.Condition_Complete,
	//	Status:  metav1.ConditionTrue,
	//	Reason:  "ReplicatedResource",
	//	Message: "successfully replicated resource among required namespaces",
	//})
}
