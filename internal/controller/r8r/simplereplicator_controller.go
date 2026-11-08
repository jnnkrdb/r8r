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
	"maps"

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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	r8rv1beta1 "github.com/jnnkrdb/r8r/api/r8r/v1beta1"
	"github.com/jnnkrdb/r8r/pkg/generics"
	"github.com/jnnkrdb/r8r/pkg/reconciliation"
	"github.com/jnnkrdb/r8r/pkg/reconciliation/helpers"
	"github.com/jnnkrdb/r8r/pkg/status"
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

	// create a defer function to patch the status fields, even when one resource fails to be reconciled
	defer func() {
		if err := r.GetClient().Status().Patch(ctx, simpleReplicator, client.Merge); err != nil {
			_baseLog.Error(err, "failed patching status field", "simpleReplicator.Status", simpleReplicator.Status)
		}
	}()

	// create the reconciliation handler
	var reconciler = reconciliation.NewReconciliationHandler(r, simpleReplicator)
	_baseLog.V(5).Info("initialized reconciler", "*reconciler", *reconciler)

	// request a list of namespaces, to parse through the list and
	// then check every namespace with the give item
	allNamespacesHashMap_primary, err := reconciler.ListAsHashMap(ctx, &corev1.NamespaceList{}, generics.KeyFuncNamespaceName)
	if err != nil {
		_baseLog.Error(err, "creating allNamespacesHashMap_primary")
		return ctrl.Result{}, err
	}
	_baseLog.V(5).Info("allNamespacesHashMap_primary content", "allNamespacesHashMap_primary", allNamespacesHashMap_primary)

	// request a list of namespaces, which are required to inherit the defined object
	requiredNamespacesHashMap_primary, err := reconciler.ListAsHashMapWithLabelSelector(
		ctx, &corev1.NamespaceList{}, generics.KeyFuncNamespaceName, simpleReplicator.Spec.Selector.LabelSelector)
	if err != nil {
		_baseLog.Error(err, "creating requiredNamespacesHashMap_primary")
		return ctrl.Result{}, err
	}
	_baseLog.V(5).Info("requiredNamespacesHashMap_primary content",
		"requiredNamespacesHashMap_primary", requiredNamespacesHashMap_primary)

	/*
		at this point in time, we have two required hashmaps already:

		1. allNamespacesHashMap_primary
		  -> contains all namespaces from the cluster.

		2. requiredNamespacesHashMap_primary
		  -> contains all namespaces, in which the resources should be deployed.
				 this counts for all resources from the resources list.

		since these two lists affect all resources, they will be fetched outside of the resources loop and
		each resources then receives an own copy of the corresponding hashmap

	*/

	// creating a sub function to utilize defer funcs and return options
	subResourceReconciliation := func(_r unstructured.Unstructured) (ctrl.Result, error) {

		var _resourceLog = _baseLog.WithValues(
			"currentResource.GroupVersionKind", _r.GroupVersionKind().String(),
			"currentResource.Name", _r.GetName(),
		)
		_resourceLog.Info("handling resource")

		// prepare the required hashmaps for the currently active sub resource
		// with maps.Clone() we are able to copy the maps data, without touching the
		// original map.
		// with the defer function we ensure, that the reference can be collected safely
		// from the garbage collector and will not be used after the function drops

		//var allNamespacesHashMap = maps.Clone(allNamespacesHashMap_primary)
		var requiredNamespacesHashMap = maps.Clone(requiredNamespacesHashMap_primary)
		defer func() {
			//allNamespacesHashMap = nil
			requiredNamespacesHashMap = nil
		}()

		// get resources, that are directly linked to the simplereplicator, due to set labels
		existingSubresourcesHashMap, err := reconciler.ListAsHashMapWithLabelSelector(
			ctx, helpers.UnstructuredListFromObject(_r), generics.KeyFuncNamespaceName, &metav1.LabelSelector{
				MatchLabels: reconciler.GetOwnerLabels(_r.GetLabels()),
			})
		if err != nil {
			_resourceLog.Error(err, "creating existingSubresourcesHashMap")
			return ctrl.Result{}, err
		}
		_resourceLog.V(5).Info("existingSubresourcesHashMap content",
			"existingSubresourcesHashMap", existingSubresourcesHashMap)

		/*
			after the successful request for the existingSubresourcesHashMap we have the three hashmaps, to
			run all possible functions for the current subresource

			1. create or update
					to check whether we have to create or update a resource, we just parse through the required namespaces hashmap and check,
					with the given resource, whether there already is something deployed in the cluster (via the existingSubresourcesHashMap).
					if something is already deployed, we update the subresource in the cluster, if the subresource is not found in the hashmap,
					then we create a new subresource in the cluster.
					after this process we remove the resource from the existingSubresourcesHashMap and the namespace from the requiredNamespacesHashMap

					after that, we will have left the required namespaces hashmap, which shouldnt hold any objects anymore, and the existingSubresourcesHashMap
					which could still contain items within. those items then will be deleted in step two, since the will not be required in the cluster anymore

			2. delete (if the labelselector changes, and the namespace shouldn't hold the item anymore)
					delete (if there were changes in the resources list of the simplereplicator, we need to check the status deployments)
		*/

		// parse through the required namespaces hashmap (the original), to handle create and update requests
		for requiredNSKey := range requiredNamespacesHashMap_primary {

			var possibleResource = types.NamespacedName{
				Namespace: requiredNamespacesHashMap[requiredNSKey].GetName(),
				Name:      _r.GetName(),
			}

			_requiredNamespaceLog := _resourceLog.WithValues(
				"requiredNSKey", requiredNSKey,
				"possibleResource", possibleResource,
			)

			_requiredNamespaceLog.V(3).Info("handle required namespace")

			// create the new/updated version of the to-deploy subresource
			toDeploySubResource, err := reconciler.CreateSubResource(_r, possibleResource.Namespace)
			if err != nil {
				_resourceLog.Error(err, "failed to create subresource", "toDeploySubResource", toDeploySubResource)
				return ctrl.Result{}, err
			}

			_, alreadyExists := existingSubresourcesHashMap[possibleResource.String()]
			_requiredNamespaceLog.V(3).Info("does subresource exist?", "alreadyExists", alreadyExists)

			// handle either the update or creation of the toDeploySubresource
			if alreadyExists {
				// update
				if err := reconciler.Update(ctx, toDeploySubResource); err != nil {
					_requiredNamespaceLog.Error(err, "updating subresource failed")
					return ctrl.Result{}, err
				}
				_requiredNamespaceLog.V(3).Info("updated subresource")
			} else {
				// create
				if err := reconciler.Create(ctx, toDeploySubResource); err != nil {
					_requiredNamespaceLog.Error(err, "creating subresource failed")
					return ctrl.Result{}, err
				}
				_requiredNamespaceLog.V(3).Info("created subresource")
			}

			// update the state of the created/updated resource in the status
			var objRef = status.FromObject(toDeploySubResource)
			if simpleReplicator.Status.Deployed == nil {
				simpleReplicator.Status.Deployed = make(map[string]status.ObjectReference)
			}
			simpleReplicator.Status.Deployed[objRef.String()] = objRef

			// remove the items from the hashmaps
			delete(requiredNamespacesHashMap, requiredNSKey)
			delete(existingSubresourcesHashMap, possibleResource.String())
		}

		_resourceLog.V(5).Info("following items are left in the important hashmaps",
			"requiredNamespacesHashMap", requiredNamespacesHashMap,
			"existingSubresourcesHashMap", existingSubresourcesHashMap,
		)

		// TODO: implement the deletion steps of the sub resources, which shouldn't exist anymore

		return ctrl.Result{}, nil
	}

	// for every resource in the reources list, check if it has to be created/updated/deleted in any namespace
	for _, _resource := range simpleReplicator.Spec.Resources {

		// run the subresource reconciliation function
		if result, err := subResourceReconciliation(_resource); err != nil {
			return result, err
		}

		// parse through all namespaces in the cluster and check, whether
		// the object has to be created, updated or deleted from the namespace.

		// this handles the deletion now
		/*
			for _, _namespace := range allNamespacesHashMap {

				var _namespaceLog = _resourceLog.WithValues("destinationNamespace", _namespace.GetName())

				// if the object does exist, and either should be updated or deleted,
				// check if the owner is in fact the clusterobject
				// TODO: check whether this is the correct place to check if the object is owned by the simplereplicator object
				if !metav1.IsControlledBy(subResource, simpleReplicator) {
					_namespaceLog.Info("object does not contain ownerreference")
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
		*/
	}
	_baseLog.Info("successfully created resource-replicas in required namespaces")

	r.GetRecorder().Eventf(
		simpleReplicator, nil,
		"Normal", "ResourceReplication",
		"ReplicateResources", "successfully finished reconciliation of simplereplicator resources.")

	return ctrl.Result{}, nil
}
