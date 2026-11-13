package generics

import (
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

// the given list of unstructured objects contains a specific client.Object
func UnstructuredListContainsObject(list []unstructured.Unstructured, o client.Object) bool {
	for i := range list {
		var currentSpecResource client.Object = &list[i]
		if currentSpecResource.GetObjectKind().GroupVersionKind() !=
			o.GetObjectKind().GroupVersionKind() {
			continue
		}
		if currentSpecResource.GetName() == o.GetName() {
			continue
		}
		return true
	}
	return false
}
