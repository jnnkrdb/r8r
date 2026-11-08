package status

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// this object store holds the bare minimum information to get an
// unstructured.Unstructured{} object from the cluster
// the UID in this case is optional
type ObjectReference struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace,omitempty"`
	Name       string    `json:"name"`
	UID        types.UID `json:"uid,omitempty"`
}

// get the required values as a string. this can be used to identify an object in the cluster
func (or ObjectReference) String() string {
	return or.APIVersion + "/" + or.Kind + "/" + or.Namespace + "/" + or.Name
}

// parse the object reference into an client.Object interface, to use it for cluster interactions
func (or ObjectReference) IntoObject(o client.Object) {
	var temp = unstructured.Unstructured{}
	temp.SetAPIVersion(or.APIVersion)
	temp.SetKind(or.Kind)
	temp.SetNamespace(or.Namespace)
	temp.SetName(or.Name)
	o = &temp
}

// get the key, e.g. for a map, from a given object, to check whether a given object has a
// corresponding ObjectReference
func KeyFromObject(o client.Object) string {
	return FromObject(o).String()
}

// get the ObjectReference infos from a given client Object
func FromObject(o client.Object) ObjectReference {
	return ObjectReference{
		APIVersion: o.GetObjectKind().GroupVersionKind().GroupVersion().String(),
		Kind:       o.GetObjectKind().GroupVersionKind().GroupKind().Kind,
		Namespace:  o.GetNamespace(),
		Name:       o.GetName(),
		UID:        o.GetUID(),
	}
}
