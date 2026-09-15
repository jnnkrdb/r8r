package actions

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/status"

	corev1 "k8s.io/api/core/v1"
)

// check whether an object should exist in a given namespace or not
func (rr *ResourceRequest) ShouldExist(ctx context.Context, statushandler *status.StatusHandler) (bool, error) {
	shouldExist, err := ShouldObjectExist(rr.Namespace, rr.RequiredNamespaces)

	return shouldExist, statushandler.ThrowEventWithConditionOnError(
		err,
		nil,
		status.EventType_Warning,
		"ClusterObjectFetching",
		"Error Fetching ClusterObject",
		"error checking if the object should exist in the namespace: %v", err,
	)
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
