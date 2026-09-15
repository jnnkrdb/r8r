package checks

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
)

// this function is used to check whether a specific object exists in the cluster or not.
// It is used to verify the existence of the object before performing any operations on it.
func VerifyObjectExistence(
	ctx context.Context,
	c client.Client,
	namespace, name string,
	object *unstructured.Unstructured) (bool, error) {

	// fail early, when the requested scheme is empty
	if object == nil {
		return false, fmt.Errorf("object scheme cannot be empty")
	}

	// request the object from the cluster
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, object); err != nil {

		return false, client.IgnoreNotFound(err)
	}

	return true, nil
}

// check whether an object should exist in a given namespace or not
func ShouldObjectExist(
	namespace corev1.Namespace,
	requiredNamespaces *corev1.NamespaceList) (bool, error) {

	if requiredNamespaces == nil {
		return false, fmt.Errorf("requiredNamespaces cannot be nil")
	}

	for _, checkingNamespace := range requiredNamespaces.Items {

		if checkingNamespace.GetName() == namespace.GetName() {

			return true, nil
		}
	}

	return false, nil
}

func ShouldExistInList[O comparable](o O, list []O) bool {
	return false
}
