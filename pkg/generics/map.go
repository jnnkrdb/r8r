package generics

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// check whether given map is empty or not, if empty -> create new one
// returns m, or a new empty map if m is nil.
func MapFromNil[M ~map[K]V, K comparable, V any](m M) M {
	if m == nil {
		return make(M)
	}
	return m
}

// create hashmap from unstructured.UnstructuredList, uses a function to determine the key
func UnstructuredListToHashmap(list *unstructured.UnstructuredList, fnKey func(unstructured.Unstructured) string) map[string]client.Object {
	// create the map from with the items list
	out := make(map[string]client.Object, len(list.Items))
	for _, item := range list.Items {
		out[fnKey(item)] = &item
	}
	return out
}
