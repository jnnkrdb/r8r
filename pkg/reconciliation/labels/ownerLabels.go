package validator

import (
	"github.com/jnnkrdb/r8r/pkg/generics"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// get ownership labels
func OwnerLabels(labels map[string]string, owner client.Object) map[string]string {
	labels = generics.MapFromNil(labels)
	labels["r8r.jnnkrdb.de/ownedBy"] = string(owner.GetUID())
	return labels
}
