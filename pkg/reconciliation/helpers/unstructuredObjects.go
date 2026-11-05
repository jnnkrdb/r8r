package helpers

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
