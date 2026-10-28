package validator

import "sigs.k8s.io/controller-runtime/pkg/client"

// get ownership labels
func OwnerLabels(labels map[string]string, owner client.Object) map[string]string {
	labels["r8r.jnnkrdb.de/ownedBy"] = string(owner.GetUID())
	return labels
}
