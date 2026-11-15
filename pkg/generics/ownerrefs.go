package generics

import (
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// get ownership labels
func GetOwnerLabels(owner client.Object, labels map[string]string) map[string]string {
	labels = MapFromNil(labels)
	labels["r8r.jnnkrdb.de/ownedBy"] = CreateIdentifierHashFromObject(owner)
	return labels
}

// set ownership for given object
func SetOwnership(owner, obj client.Object, scheme *runtime.Scheme) error {
	obj.SetLabels(GetOwnerLabels(owner, obj.GetLabels()))
	return controllerutil.SetControllerReference(owner, obj, scheme)
}

// take owner ship from an object
func TakeOwnership(owner, obj client.Object, scheme *runtime.Scheme) error {
	refs := slices.DeleteFunc(obj.GetOwnerReferences(), func(r metav1.OwnerReference) bool {
		return r.Controller != nil && *r.Controller
	})
	obj.SetOwnerReferences(refs)
	return SetOwnership(owner, obj, scheme)
}
