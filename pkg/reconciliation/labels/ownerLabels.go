package validator

import (
	"github.com/jnnkrdb/r8r/pkg/generics"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// get ownership labels
func OwnerLabels(labels map[string]string, owner client.Object) map[string]string {
	labels = generics.MapFromNil(labels)
	labels["r8r.jnnkrdb.de/ownedBy"] = string(owner.GetUID())
	return labels
}

// get listoptions, that fetch object from cluster, according to the owner labels
func GetListOptionWithLabelSelector(labels map[string]string) (*client.ListOptions, error) {
	var lo = &client.ListOptions{}

	if labelSelector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: labels,
	}); err != nil {

		return nil, err

	} else {

		lo.LabelSelector = labelSelector
	}

	return lo, nil
}
