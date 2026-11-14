package status

import (
	"crypto/sha256"
	"fmt"

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

// return the string instance of the related objectreference
func (or ObjectReference) String() string {
	return fmt.Sprintf("%s;%s;%s;%s", or.ApiVersion, or.Kind, or.Namespace, or.Name)
}

// get an id key, that is identical to the contents of the ObjectReference instance
func (or ObjectReference) GetKey() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(or.String())))
}
