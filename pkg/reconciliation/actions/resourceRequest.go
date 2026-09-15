package actions

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type ResourceRequest struct {
	RequiredNamespaces *corev1.NamespaceList
	Namespace          corev1.Namespace
	OwnerResource      metav1.Object
	Resource           *unstructured.Unstructured
}
