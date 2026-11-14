package r8r

import (
	"context"

	"github.com/jnnkrdb/r8r/api/r8r/v1beta1"
	"github.com/jnnkrdb/r8r/pkg/generics"
	"github.com/jnnkrdb/r8r/pkg/reconciliation"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// this is an interface struct, which could be used to handle object related functions,
// which are not implemented at in object relation, due to dependency requirements
type SimpleReplicatorInterface struct {
	SimpleReplicator *v1beta1.SimpleReplicator
	Reconciler       *reconciliation.ReconciliationHandler
}

// handle the garbagecollection part for items from the status fields
func (sri *SimpleReplicatorInterface) GarbageCollection(ctx context.Context) error {

	var _baseLog = logf.FromContext(ctx).WithValues(
		"status.deployedItems-length", len(sri.SimpleReplicator.Status.DeployedItems))
	_baseLog.Info("starting garbage collection from the cluster")

	// request a map of namespaces from the cluster, which are claculated by the simplereplicators namespace selector
	requiredNamespaces, err := sri.Reconciler.ListAsHashMapWithLabelSelector(
		ctx, &corev1.NamespaceList{}, generics.KeyFuncNamespaceName, sri.SimpleReplicator.Spec.Selector.LabelSelector)
	if err != nil {
		_baseLog.Error(err, "creating requiredNamespaces")
		return err
	}
	_baseLog.V(3).Info("requiredNamespaces content", "requiredNamespaces", requiredNamespaces)

	// with the defer function we ensure, that the reference can be collected safely
	// from the garbage collector and will not be used after the function drops
	defer func() {
		requiredNamespaces = nil
	}()

	// now parse through all objects within the status filed, to determine, whether there are objects in the cluster
	// which shouldn't exist from the specifications of the current state of the simplereplicator object
	for deployedItemKey, or := range sri.SimpleReplicator.Status.DeployedItems {

		// get the client.Object interface from the stored object reference
		var deployedItemObject = or.GetObject()
		//var shouldBeDeleted bool = false

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
		if err := sri.Reconciler.Get(ctx, types.NamespacedName{
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
			return err
		}
		_deployedItemsLog.V(3).Info("received statusDeployedObject from cluster",
			"deployedItemObject", deployedItemObject)

		// if the object does exist, check if the owner is in fact the simplereplicator resource
		if isControlledBy := metav1.IsControlledBy(deployedItemObject, sri.SimpleReplicator); !isControlledBy {
			_deployedItemsLog.V(3).Info("object does not contain ownerreference -> ignoring",
				"isControlledBy", isControlledBy)
			// remove the item from the deployedItems pool, since it is not related
			delete(sri.SimpleReplicator.Status.DeployedItems, deployedItemKey)
			continue
		}

		// similar object in the .spec.resources list?
		var objectInSpecResources bool = generics.UnstructuredListContainsObject(
			sri.SimpleReplicator.Spec.Resources, deployedItemObject)
		_deployedItemsLog.V(3).Info("similar item in .spec.resources list -> ignoring",
			"objectInSpecResources", objectInSpecResources,
		)
		if objectInSpecResources {
			continue
		}

		_, containsNamespace := requiredNamespaces["/"+deployedItemObject.GetNamespace()]
		if containsNamespace {
			_deployedItemsLog.V(3).Info("object is found in correct namespace",
				"requiredNamespaces", requiredNamespaces,
				"containsNamespace", containsNamespace)
			continue
		}

		// if we came this far, the object probably should be removed from the cluster
		if err := sri.Reconciler.Delete(ctx, deployedItemObject); err != nil {
			_deployedItemsLog.Error(err, "failed removing objectRef from cluster")
			return err
		}
		delete(sri.SimpleReplicator.Status.DeployedItems, deployedItemKey)
	}

	_baseLog.Info("finished garbage collection from the cluster")
	// update the status field of the simplereplicator object in the cluster
	return sri.Reconciler.Client().Status().Update(ctx, sri.SimpleReplicator)
}
