package selector

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NamespaceSelector is an object, to calculate required namespaces for a given object.
// It can be used to select namespaces based on labels or a list of namespaces.
type NamespaceSelector struct {
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty" protobuf:"bytes,4,opt,name=labelSelector"`

	// +optional
	Namespaces *[]string `json:"namespaces,omitempty"`
}

// this function is used to get the list of namespaces based on the
// label selector or the list of namespaces provided in the NamespaceSelector object.
func (ns *NamespaceSelector) GetNamespaces(ctx context.Context, c client.Client, namespaceList *corev1.NamespaceList) error {

	// create the labelseclector from the labelselector provided in the NamespaceSelector object.
	labelselector, err := metav1.LabelSelectorAsSelector(ns.LabelSelector)
	if err != nil {
		return fmt.Errorf("error converting label selector: %w", err)
	}

	// request the namespaces list from the cluster based on the label selector provided in the NamespaceSelector object.
	if err := c.List(ctx, namespaceList, &client.ListOptions{LabelSelector: labelselector}); err != nil {
		return fmt.Errorf("error listing namespaces: %w", err)
	}

	return nil
}
