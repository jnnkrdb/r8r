package generics

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
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

// defines the function, which calculates the key for the created hashmap item.
// comes in handy, if you need different keys from an item as the hashmaps item key
type KeyFunc func(client.Object) string

// some default function generally used
var (
	KeyFuncNamespaceName = KeyFunc(func(o client.Object) string {
		return o.GetNamespace() + "/" + o.GetName()
	})
)

// defines the hashmap of the a given object from the cluster. can for example be converted from
// a given objectList, to simplify routine actions on the objectlists items
type ObjectHashMap map[string]client.Object

// create a map from an objectList
func ObjectListToMap(list client.ObjectList, fnKey KeyFunc) (ObjectHashMap, error) {
	// create the map from with the items list
	out := make(map[string]client.Object)

	err := meta.EachListItem(list, func(o runtime.Object) error {

		obj, ok := o.(client.Object)
		if !ok {
			return fmt.Errorf("item %T is not a client.Object", o)
		}

		// the object to the map, with the given key function
		out[fnKey(obj)] = obj

		return nil // returning an error stops the iteration
	})

	return out, err
}
