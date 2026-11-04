package helpers

import (
	validator "github.com/jnnkrdb/r8r/pkg/reconciliation/labels"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// get the corresponding unstructuredList type, from a given unstructured Object,
// holding the information for an actual object in the cluster
func UnstructuredListFromObject(referringObject unstructured.Unstructured) *unstructured.UnstructuredList {

	var unstructuredList = &unstructured.UnstructuredList{}

	unstructuredList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   referringObject.GroupVersionKind().Group,
		Version: referringObject.GroupVersionKind().Version,
		Kind:    referringObject.GroupVersionKind().Kind + "List",
	})

	return unstructuredList
}

// create subresource from parent object, with owner labels and namespace
func CreateOwnedObjectWithNamespace(owner client.Object, object unstructured.Unstructured, namespace string) *unstructured.Unstructured {

	// create a copy of the given object, that can be used with client.Object interface
	var owned = object.DeepCopy()

	// set the namespace
	owned.SetNamespace(namespace)

	// set the ownerlabels
	owned.SetLabels(validator.OwnerLabels(object.GetLabels(), owner))

	return owned
}
