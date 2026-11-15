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
	"fmt"
	"maps"
	"slices"

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
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *SimpleReplicatorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var _baseLog = logf.FromContext(ctx)
	_baseLog.Info("reconciling SimpleReplicator object")

	// create cached object of requested resource
	var simpleReplicator = &r8rv1beta1.SimpleReplicator{}
	if err := r.GetClient().Get(ctx, req.NamespacedName, simpleReplicator); err != nil {
		_baseLog.Error(err, "error fetching object from cluster")
		return ctrl.Result{}, err
	}
	_baseLog.V(3).Info("simpleReplicator content", "*simpleReplicator", *simpleReplicator)
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
		simpleReplicator.Status.DeployedItems = make(map[string]status.ObjectReference)
	}

	// create a defer function to update the status fields, even when one resource fails to be reconciled
	defer func() {
		if err := r.GetClient().Status().Update(ctx, simpleReplicator); err != nil {
			_baseLog.Error(err, "failed patching status field", "simpleReplicator.Status", simpleReplicator.Status)
		}
	}()

	// create the reconciliation handler for the given simplereplicator
	//var sri = &SimpleReplicatorInterface{
	//	SimpleReplicator: simpleReplicator,
	//	Reconciler:       reconciliation.NewReconciliationHandler(r, simpleReplicator),
	//}

	/*

		since multiple steps of the reconciliation need a full list of namespaces, which should be kept up to date
		with the given resources, we need to fetch them from the cluster. after fetching the list, we create a sort
		of hashmap from the list, since that simplifies the requirememt-checks during deletion, creation and updates.

	*/

	// request all required namespaces, according to the simplereplicators namespaceSelector
	ls, err := metav1.LabelSelectorAsSelector(simpleReplicator.Spec.Selector.LabelSelector)
	if err != nil {
		_baseLog.Error(err, "error creating the labelselector from config",
			"labelSelector", map[string]any{
				"source": simpleReplicator.Spec.Selector.LabelSelector,
				"result": ls,
			})
		return ctrl.Result{}, err
	}
	var nsList = &corev1.NamespaceList{}
	if err := r.GetClient().List(ctx, nsList, &client.ListOptions{LabelSelector: ls}); err != nil {
		_baseLog.Error(err, "error fetching namespaces from cluster")
		return ctrl.Result{}, err
	}
	// request a list of namespaces, which are required to inherit the defined object
	requiredNamespaces, err := generics.ObjectListToMap(nsList, generics.KeyFuncNamespaceName)
	if err != nil {
		_baseLog.Error(err, "error parsing map from list", "nsList", nsList)
		return ctrl.Result{}, err
	}
	// with the defer function we ensure, that the reference can be collected safely
	// from the garbage collector and will not be used after the function drops
	defer func() {
		requiredNamespaces = nil
	}()
	_baseLog.V(3).Info("requiredNamespaces content", "requiredNamespaces", requiredNamespaces)

	/*

		with the given list of required namespaces, we will start with the garbage collection.

	*/

	for deployedItemKey, or := range simpleReplicator.Status.DeployedItems {

		var deployedItemObject = or.GetObject()
		var _deployedItemsLog = _baseLog.WithValues(
			"deployedItem", map[string]any{
				"metadata": map[string]any{
					"group-version-kind": deployedItemObject.GetObjectKind().GroupVersionKind().String(),
					"namespace":          deployedItemObject.GetNamespace(),
					"name":               deployedItemObject.GetName(),
					"deployedItemKey":    deployedItemKey,
				},
			},
		)
		_deployedItemsLog.V(3).Info("checking resource from status.deployedItems")

		// trying to fetch the object from the cluster
		if err := r.GetClient().Get(ctx, types.NamespacedName{
			Namespace: deployedItemObject.GetNamespace(),
			Name:      deployedItemObject.GetName(),
		}, deployedItemObject); err != nil {
			// if this is an notFound error return nil
			// that will remove the object from the statusfield
			if client.IgnoreNotFound(err) == nil {
				_deployedItemsLog.V(3).Info("object not found")
				delete(simpleReplicator.Status.DeployedItems, deployedItemKey)
				continue
			}
			_deployedItemsLog.Error(err, "error fetching statusDeployedResource")
			return ctrl.Result{}, err
		}
		_deployedItemsLog.V(3).Info("received statusDeployedObject from cluster",
			"deployedItemObject", deployedItemObject)

		// if the object does exist, check if the owner is in fact the simplereplicator resource
		if isControlledBy := metav1.IsControlledBy(deployedItemObject, simpleReplicator); !isControlledBy {
			_deployedItemsLog.V(3).Info("object doesn't contain ownerref -> ignoring", "isControlledBy", isControlledBy)
			// remove the item from the deployedItems pool, since it is not related
			delete(simpleReplicator.Status.DeployedItems, deployedItemKey)
			continue
		}

		var hasRefInSpecs bool = slices.ContainsFunc(
			simpleReplicator.Spec.Resources, func(u unstructured.Unstructured) bool {
				return u.GetObjectKind().GroupVersionKind() == deployedItemObject.GetObjectKind().GroupVersionKind() &&
					u.GetName() == deployedItemObject.GetName()
			})
		_deployedItemsLog.V(3).Info("similar item in .spec.resources -> ignoring", "hasRefInSpecs", hasRefInSpecs)
		if hasRefInSpecs {
			// if the item is referenced in the .spec.resources pool, then we will not remove it from the cluster
			continue
		}

		// check whether the existing object, with the correct objectkind, version and name
		// has its namespace in the requiredNamespaces map
		if _, containsNamespace := requiredNamespaces["/"+deployedItemObject.GetNamespace()]; containsNamespace {
			_deployedItemsLog.V(3).Info("object is found in correct namespace",
				"requiredNamespaces", requiredNamespaces, "containsNamespace", containsNamespace)
			continue
		}

		// if we came this far, the object probably should be removed from the cluster
		if err := r.GetClient().Delete(ctx, deployedItemObject); err != nil {
			_deployedItemsLog.Error(err, "failed removing objectRef from cluster")
			return ctrl.Result{}, err
		}
		delete(simpleReplicator.Status.DeployedItems, deployedItemKey)
	}
	_baseLog.Info("finished garbage collection from the cluster")

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
		_resourceLog.Info("reconciling sub-resource")

		// prepare the required hashmaps for the currently active sub resource
		// with maps.Clone() we are able to copy the maps data, without touching the
		// original map.
		// with the defer function we ensure, that the reference can be collected safely
		// from the garbage collector and will not be used after the function drops
		var requiredNamespacesHashMap = maps.Clone(requiredNamespaces)
		defer func() {
			requiredNamespacesHashMap = nil
		}()

		/*

			fetch all objects from the cluster, which are related to the simplereplicator resource

		*/

		var ownerLabels = generics.GetOwnerLabels(simpleReplicator, _r.GetLabels())
		// request all required namespaces, according to the simplereplicators namespaceSelector
		ls, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: ownerLabels})
		if err != nil {
			_resourceLog.Error(err, "error creating labelSelector from controllerobject",
				"labelSelector", map[string]any{
					"source": ownerLabels,
					"result": ls,
				})
			return ctrl.Result{}, err
		}
		var refObjList = generics.ObjectToObjectList(&_r)
		if err := r.GetClient().List(ctx, refObjList, &client.ListOptions{LabelSelector: ls}); err != nil {
			_resourceLog.Error(err, "error fetching referenced objects from cluster", "refObjList", refObjList)
			return ctrl.Result{}, err
		}
		// request a list of referenced objects, which are required to inherit the defined object
		subresources, err := generics.ObjectListToMap(refObjList, generics.KeyFuncNamespaceName)
		if err != nil {
			_resourceLog.Error(err, "error parsing map from list", "subresources", subresources)
			return ctrl.Result{}, err
		}
		_resourceLog.V(3).Info("subresources content", "subresources", subresources)

		// parse through the required namespaces hashmap (the original), to handle create and update requests
		for requiredNSKey := range requiredNamespaces {

			var possibleResource = types.NamespacedName{
				Namespace: requiredNamespaces[requiredNSKey].GetName(),
				Name:      _r.GetName(),
			}

			_requiredNamespaceLog := _resourceLog.WithValues(
				"requiredNamespaceHashMap", map[string]any{
					"key": requiredNSKey, "possibleResource": possibleResource,
				},
			)
			_requiredNamespaceLog.V(3).Info("handle required namespace")

			// create the new/updated version of the to-deploy subresource
			var toDeploySubResource client.Object = _r.DeepCopy()
			toDeploySubResource.SetNamespace(possibleResource.Namespace)

			// check, whether subresource already exists and is related
			objectInCluster, relatedObjectAlreadyExists := subresources[possibleResource.String()]

			var unrelatedObjectAlreadyExists bool = false
			if !relatedObjectAlreadyExists {
				// check whether there already is an object, with the same groupversionkind and name in that namespace
				// trying to fetch the object from the cluster
				if err := r.GetClient().Get(ctx, types.NamespacedName{
					Namespace: toDeploySubResource.GetNamespace(),
					Name:      toDeploySubResource.GetName(),
				}, toDeploySubResource); client.IgnoreNotFound(err) != nil {
					_requiredNamespaceLog.Error(err, "failed to fetch object from cluster")
					return ctrl.Result{}, err
				}
				_requiredNamespaceLog.V(3).Info("received toDeploySubResource from cluster",
					"toDeploySubResource", toDeploySubResource)

				// there is an objct, which could be overwritten
				unrelatedObjectAlreadyExists = true

				// the object may be already owned by the simplereplicator
				relatedObjectAlreadyExists = metav1.IsControlledBy(toDeploySubResource, simpleReplicator)
			}

			_requiredNamespaceLog.V(3).Info("decision on create/update/ignore for given object",
				"decisionDriver", map[string]any{
					"related": map[string]any{
						"exists": relatedObjectAlreadyExists,
						"object": objectInCluster,
					},
					"unrelated": map[string]any{
						"exists": unrelatedObjectAlreadyExists,
						"object": toDeploySubResource,
					},
				},
			)

			_update := func(destination client.Object) error {
				// set resource version (required for non-builtin types)
				toDeploySubResource.SetResourceVersion(destination.GetResourceVersion())

				// set ownership
				if err := generics.TakeOwnership(simpleReplicator, toDeploySubResource, r.GetScheme()); err != nil {
					_requiredNamespaceLog.Error(err, "failed setting ownership of sub resource")
					return err
				}

				// update
				if err := r.GetClient().Update(ctx, toDeploySubResource); err != nil {
					_requiredNamespaceLog.Error(err, "updating subresource in cluster failed")
					return err
				}
				_requiredNamespaceLog.V(3).Info("updated subresource")
				return nil
			}

			switch {
			case relatedObjectAlreadyExists && !unrelatedObjectAlreadyExists:
				// given object already exists and is related to the simplereplicator resource -> simple update
				if err := _update(objectInCluster); err != nil {
					return ctrl.Result{}, err
				}
				break
			case !relatedObjectAlreadyExists && unrelatedObjectAlreadyExists:
				// given object already exists but is unrelated to the simplereplicator resource
				// -> check whether we should set ownership and update or ignore object
				_requiredNamespaceLog.V(3).Info("overwrite existing resource?",
					"overwrite", *simpleReplicator.Spec.OverwriteExisting)
				if !*simpleReplicator.Spec.OverwriteExisting {
					// this leaves the currently checked item from requiredNamespaces, for the given object
					// because we can completly ignore it now
					continue
				}

				// updated and set ownership
				if err := _update(toDeploySubResource); err != nil {
					return ctrl.Result{}, err
				}
				break
			case !relatedObjectAlreadyExists && !unrelatedObjectAlreadyExists:
				// set ownership
				if err := generics.SetOwnership(simpleReplicator, toDeploySubResource, r.GetScheme()); err != nil {
					_requiredNamespaceLog.Error(err, "failed setting ownership of sub resource")
					return ctrl.Result{}, err
				}
				// create the given object, since there is no object with the given metadata specs
				if err := r.GetClient().Create(ctx, toDeploySubResource); err != nil {
					_requiredNamespaceLog.Error(err, "creating subresource in cluster failed")
					return ctrl.Result{}, err
				}
				_requiredNamespaceLog.V(3).Info("created subresource", "toDeploySubResource", toDeploySubResource)
				break
			default:
				// case relatedObjectAlreadyExists && unrelatedObjectAlreadyExists:
				var e error = fmt.Errorf("possible coexistence found: relatedObjectAlreadyExists = %t, unrelatedObjectAlreadyExists = %t",
					relatedObjectAlreadyExists, unrelatedObjectAlreadyExists)
				_resourceLog.Error(e, "related and unrelated objects cannot coexist, there must be an issue with the relation checks")
				return ctrl.Result{}, e
			}

			// remove the items from the hashmaps
			delete(requiredNamespacesHashMap, requiredNSKey)
			_resourceLog.V(3).Info("removed item from requiredNamespacesHashMap",
				"deleteRequest", map[string]any{
					"key":          requiredNSKey,
					"currentState": requiredNamespacesHashMap,
				},
			)
			delete(subresources, possibleResource.String())
			_resourceLog.V(3).Info("removed item from subresources",
				"deleteRequest", map[string]any{
					"key":          possibleResource.String(),
					"currentState": subresources,
				},
			)

			// update the state of the created/updated resource in the status
			var objectRef = status.ObjectReferenceFromObject(toDeploySubResource)
			simpleReplicator.Status.DeployedItems[objectRef.GetKey()] = objectRef
			_resourceLog.V(3).Info("added objectRef to .status.deployedItems",
				"deployedItems", map[string]any{
					"key":          objectRef.GetKey(),
					"content":      objectRef,
					"currentState": simpleReplicator.Status.DeployedItems,
				},
			)
		}

		_resourceLog.V(3).Info("following items are left in the important hashmaps",
			"requiredNamespacesHashMap", requiredNamespacesHashMap,
			"subresources", subresources,
		)
		return ctrl.Result{}, nil
	}

	_baseLog.Info("starting reconciliation of subresources")

	// for every resource in the reources list, check if it has to be created/updated/deleted in any namespace
	for _, _resource := range simpleReplicator.Spec.Resources {
		// run the subresource reconciliation function
		if result, err := subResourceReconciliation(_resource); err != nil {
			return result, err
		}
	}

	_baseLog.Info("finished reconciliation of subresources")

	r.GetRecorder().Eventf(
		simpleReplicator, nil, "Normal", "ResourceReplication", "ReplicateResources",
		"successfully finished reconciliation of simplereplicator resources.")

	return ctrl.Result{}, nil
}
