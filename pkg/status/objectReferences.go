package status

import (
	"github.com/jnnkrdb/r8r/pkg/generics"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// the actual reference from the object in the cluster
type ObjectReference struct {
	ApiVersion string `json:"apiversion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
}

// get a reference from a given cliet.Object
func ObjectReferenceFromObject(o client.Object) ObjectReference {
	return ObjectReference{
		ApiVersion: o.GetObjectKind().GroupVersionKind().GroupVersion().String(),
		Kind:       o.GetObjectKind().GroupVersionKind().GroupKind().Kind,
		Namespace:  o.GetNamespace(),
		Name:       o.GetName(),
	}
}

// receive client.Object from the object reference
func (or ObjectReference) GetObject() client.Object {
	var o = &unstructured.Unstructured{}
	o.SetAPIVersion(or.ApiVersion)
	o.SetKind(or.Kind)
	o.SetNamespace(or.Namespace)
	o.SetName(or.Name)
	return o
}

// get an id key, that is identical to the contents of the ObjectReference instance
func (or ObjectReference) GetKey() string {
	return generics.CreateIdentifierHashFromObject(or.GetObject())
}
