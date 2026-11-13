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
	_baseLog = _baseLog.WithValues(
		"simpleReplicator", map[string]any{
			"metadata": map[string]any{
				"uid":             simpleReplicator.GetUID(),
				"resourceversion": simpleReplicator.GetResourceVersion(),
			},
			"spec": map[string]any{
				"resourcesCount": len(simpleReplicator.Spec.Resources),
			},
			"status": map[string]any{
				"deployedResourcesCount": len(simpleReplicator.Status.DeployedItems),
			},
		},
	)

	// prepare the status field of the simplereplicator resource, so the deployed map is ready for usage
	// this is required, since there may be the chance its the first time the simplereplicator object
	// gets reconciled
	if simpleReplicator.Status.DeployedItems == nil {
		simpleReplicator.Status.DeployedItems = make(map[types.UID]status.ObjectReference)
	}

	// create a defer function to update the status fields, even when one resource fails to be reconciled
	defer func() {
		if err := r.GetClient().Status().Update(ctx, simpleReplicator); err != nil {
			_baseLog.Error(err, "failed patching status field", "simpleReplicator.Status", simpleReplicator.Status)
		}
	}()

	// create the reconciliation handler
	var reconciler = reconciliation.NewReconciliationHandler(r, simpleReplicator)
	_baseLog.V(5).Info("initialized reconciler", "reconciler", reconciler)

	// request a list of namespaces, which are required to inherit the defined object
	requiredNamespacesHashMap_primary, err := reconciler.ListAsHashMapWithLabelSelector(
		ctx, &corev1.NamespaceList{}, generics.KeyFuncNamespaceName, simpleReplicator.Spec.Selector.LabelSelector)
	if err != nil {
		_baseLog.Error(err, "creating requiredNamespacesHashMap_primary")
		return ctrl.Result{}, err
	}
	_baseLog.V(5).Info("requiredNamespacesHashMap_primary content",
		"requiredNamespacesHashMap_primary", requiredNamespacesHashMap_primary)

	// creating a sub function to utilize defer funcs and return options
	subResourceReconciliation := func(_r unstructured.Unstructured) (ctrl.Result, error) {

		var _resourceLog = _baseLog.WithValues(
			"currentResource", map[string]any{
				"metadata": map[string]any{
					"group-version-kind": _r.GroupVersionKind().String(),
					"name":               _r.GetName(),
				},
			},
		)
		_resourceLog.Info("handling resource")

		// prepare the required hashmaps for the currently active sub resource
		// with maps.Clone() we are able to copy the maps data, without touching the
		// original map.
		// with the defer function we ensure, that the reference can be collected safely
		// from the garbage collector and will not be used after the function drops

		var requiredNamespacesHashMap = maps.Clone(requiredNamespacesHashMap_primary)
		defer func() {
			//allNamespacesHashMap = nil
			requiredNamespacesHashMap = nil
		}()

		// get resources, that are directly linked to the simplereplicator, due to set labels
		existingSubresourcesHashMap, err := reconciler.ListAsHashMapWithLabelSelector(
			ctx, generics.UnstructuredListFromObject(_r), generics.KeyFuncNamespaceName, &metav1.LabelSelector{
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
				"requiredNamespaceHashMap", map[string]any{
					"key":              requiredNSKey,
					"possibleResource": possibleResource,
				},
			)

			_requiredNamespaceLog.V(3).Info("handle required namespace")

			// create the new/updated version of the to-deploy subresource
			toDeploySubResource, err := reconciler.CreateSubResource(_r, possibleResource.Namespace)
			if err != nil {
				_resourceLog.Error(err, "failed to create subresource in cache", "toDeploySubResource", toDeploySubResource)
				return ctrl.Result{}, err
			}

			objectInCluster, alreadyExists := existingSubresourcesHashMap[possibleResource.String()]
			_requiredNamespaceLog.V(3).Info("creating/updating subresource",
				"alreadyExists", alreadyExists,
				"objectInCluster", objectInCluster,
			)

			// handle either the update or creation of the toDeploySubresource
			if alreadyExists {
				// set resource version (required for non-builtin types)
				toDeploySubResource.SetResourceVersion(objectInCluster.GetResourceVersion())

				// update
				if err := reconciler.Update(ctx, toDeploySubResource); err != nil {
					_requiredNamespaceLog.Error(err, "updating subresource in cluster failed")
					return ctrl.Result{}, err
				}
				_requiredNamespaceLog.V(3).Info("updated subresource")
			} else {
				// create
				if err := reconciler.Create(ctx, toDeploySubResource); err != nil {
					_requiredNamespaceLog.Error(err, "creating subresource in cluster failed")
					return ctrl.Result{}, err
				}
				_requiredNamespaceLog.V(3).Info("created subresource")
			}

			// update the state of the created/updated resource in the status
			simpleReplicator.Status.DeployedItems[toDeploySubResource.GetUID()] = status.ObjectReferenceFromObject(toDeploySubResource)

			// remove the items from the hashmaps
			delete(requiredNamespacesHashMap, requiredNSKey)
			delete(existingSubresourcesHashMap, possibleResource.String())
		}

		_resourceLog.V(5).Info("following items are left in the important hashmaps",
			"requiredNamespacesHashMap", requiredNamespacesHashMap,
			"existingSubresourcesHashMap", existingSubresourcesHashMap,
		)

		// TODO: implement the deletion steps of the sub resources, which shouldn't exist anymore
		// parse through all the items, that are left in the existingSubresourcesHashMap. those
		// objects should be removed from the cluster
		for toDeleteObjectKey, toDeleteObject := range existingSubresourcesHashMap {

			_toDeleteObjectLog := _resourceLog.WithValues(
				"toDelete", map[string]any{
					"key":    toDeleteObjectKey,
					"object": toDeleteObject,
				},
			)
			_toDeleteObjectLog.V(5).Info("removing object from cluster")

			// this check can probably be skipped, since the existingSubresourcesHashMap already should only contain
			// items that are registered as a subresource of the simplereplicator object. The only problem is, that
			// this is defined by labels and not ownerreferences, which is why there is this check in place
			if !metav1.IsControlledBy(toDeleteObject, simpleReplicator) {
				_toDeleteObjectLog.V(3).Info("object does not contain ownerreference")
				continue
			}

			// remove the actual object from the cluster
			if err := reconciler.Delete(ctx, toDeleteObject); err != nil {
				_toDeleteObjectLog.Error(err, "deleting subresource from cluster failed")
				return ctrl.Result{}, err
			}

			// since the object might be inside of the statusfield, we have to remove it there as well
			delete(simpleReplicator.Status.DeployedItems, toDeleteObject.GetUID())
		}

		return ctrl.Result{}, nil
	}

	// for every resource in the reources list, check if it has to be created/updated/deleted in any namespace
	for _, _resource := range simpleReplicator.Spec.Resources {
		// run the subresource reconciliation function
		if result, err := subResourceReconciliation(_resource); err != nil {
			return result, err
		}
	}

	// now parse through all objects within the status filed, to determine, whether there are objects in the cluster
	// which shouldn't exist from the specifications of the current state of the simplereplicator object
	for uid, or := range simpleReplicator.Status.DeployedItems {

		// get the client.Object interface from the stored object reference
		var deployedItemObject = or.GetObject()
		//var shouldBeDeleted bool = false

		var _deployedItemsLog = _baseLog.WithValues(
			"deployedItem", map[string]any{
				"metadata": map[string]any{
					"group-version-kind": deployedItemObject.GetObjectKind().GroupVersionKind().String(),
					"namespace":          deployedItemObject.GetNamespace(),
					"name":               deployedItemObject.GetName(),
					"uid":                uid,
				},
			},
		)
		_deployedItemsLog.Info("checking resource from status.deployedItems")

		// trying to fetch the object from the cluster
		if err := reconciler.Get(ctx, types.NamespacedName{
			Namespace: deployedItemObject.GetNamespace(),
			Name:      deployedItemObject.GetName(),
		}, deployedItemObject); err != nil {

			// if this is an notFound error return nil
			// that will remove the object from the statusfield
			if client.IgnoreNotFound(err) == nil {
				_deployedItemsLog.Info("object not found")
				continue
			}

			_deployedItemsLog.Error(err, "fetching statusDeployedResource failed")
			return ctrl.Result{}, err
		}
		_deployedItemsLog.V(5).Info("received statusDeployedObject from cluster",
			"deployedItemObject", deployedItemObject)

		// if the object does exist, check if the owner is in fact the simplereplicator resource
		if isControlledBy := metav1.IsControlledBy(deployedItemObject, simpleReplicator); !isControlledBy {
			_deployedItemsLog.V(3).Info("object does not contain ownerreference -> ignoring",
				"isControlledBy", isControlledBy)
			continue
		}

		// similar object in the .spec.resources list?
		var objectInSpecResources bool = generics.UnstructuredListContainsObject(
			simpleReplicator.Spec.Resources, deployedItemObject)
		_deployedItemsLog.V(3).Info("similar item in .spec.resources list -> ignoring",
			"objectInSpecResources", objectInSpecResources,
		)
		if objectInSpecResources {
			continue
		}

		_, containsNamespace := requiredNamespacesHashMap_primary["/"+deployedItemObject.GetNamespace()]
		if containsNamespace {
			_deployedItemsLog.V(3).Info("object is found in correct namespace",
				"requiredNamespacesHashMap_primary", requiredNamespacesHashMap_primary,
				"containsNamespace", containsNamespace)
			continue
		}

		// if we came this far, the object probably should be removed from the cluster
		if err := reconciler.Delete(ctx, deployedItemObject); err != nil {
			return ctrl.Result{}, err
		}
		delete(simpleReplicator.Status.DeployedItems, uid)
	}

	_baseLog.Info("successfully created resource-replicas in required namespaces")

	r.GetRecorder().Eventf(
		simpleReplicator, nil,
		"Normal", "ResourceReplication",
		"ReplicateResources", "successfully finished reconciliation of simplereplicator resources.")

	return ctrl.Result{}, nil
}
