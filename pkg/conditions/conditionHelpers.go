package conditions

import (
	"context"
	"fmt"

	clusterv1alpha1 "github.com/jnnkrdb/r8r/api/v1alpha1"
	"github.com/jnnkrdb/r8r/pkg/reconciliation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	Condition_Ready = "Ready"
)

// handling conditions
func Find(
	r reconciliation.Reconciler,
	ctx context.Context,
	co *clusterv1alpha1.ClusterObject,
	conditionType string) *metav1.Condition {

	for i := range co.Status.Conditions {

		if co.Status.Conditions[i].Type == conditionType {

			return &co.Status.Conditions[i]
		}
	}

	return nil
}

// set conditions
func Set(
	r reconciliation.Reconciler,
	ctx context.Context,
	co *clusterv1alpha1.ClusterObject,
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
	msgf string,
	a ...any) error {

	// if there is no condition with the specified type, then create a new condition and
	// add it to the list of conditions
	if _condition := Find(r, ctx, co, conditionType); _condition == nil {

		c := metav1.Condition{
			Type:               conditionType,
			Status:             status,
			ObservedGeneration: co.GetGeneration(),
			LastTransitionTime: metav1.Now(),
			Reason:             reason,
			Message:            fmt.Sprintf(msgf, a...),
		}

		co.Status.Conditions = append(co.Status.Conditions, c)

	} else {

		// change values of the given condition
		_condition.LastTransitionTime = func() metav1.Time {
			if _condition.Status != status {
				return metav1.Now()
			}
			return _condition.LastTransitionTime
		}()
		_condition.Status = status
		_condition.ObservedGeneration = co.GetGeneration()
		_condition.Reason = reason
		_condition.Message = fmt.Sprintf(msgf, a...)
	}

	return r.GetClient().Status().Update(ctx, co, &client.SubResourceUpdateOptions{})
}
