package status

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// the map containing the references
type ObjectReferenceMap map[types.UID]ObjectReference

// append objectreference to map
func (orm ObjectReferenceMap) Append(o client.Object) {
	orm[o.GetUID()] = ObjectReferenceFromObject(o)
}

// remove the given object, if there is any with that uid
func (orm ObjectReferenceMap) Delete(o client.Object) {
	delete(orm, o.GetUID())
}

// walkfunction to parse through all the items in the map
func (orm ObjectReferenceMap) WalkFn(do func(id types.UID, o client.Object) error) error {
	for id, or := range orm {
		o, err := or.GetObject()
		if err != nil {
			return err
		}
		do(id, o)
	}
	return nil
}

// the actual reference from the object in the cluster
type ObjectReference string

// get a reference from a given cliet.Object
func ObjectReferenceFromObject(o client.Object) ObjectReference {
	return ObjectReference(fmt.Sprintf("%s:%s:%s:%s",
		o.GetObjectKind().GroupVersionKind().GroupVersion().String(),
		o.GetObjectKind().GroupVersionKind().GroupKind().Kind,
		o.GetNamespace(),
		o.GetName(),
	))
}

// receive client.Object from the object reference
func (or ObjectReference) GetObject() (client.Object, error) {
	parts := strings.SplitN(string(or), ":", 4) // limit 4: the name keeps any ':' in it
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid key [%s]", string(or))
	}

	var o = &unstructured.Unstructured{}
	o.SetAPIVersion(parts[0])
	o.SetKind(parts[1])
	o.SetNamespace(parts[2])
	o.SetName(parts[3])

	return o, nil
}
