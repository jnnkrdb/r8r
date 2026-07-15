package conditions

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/jnnkrdb/r8r/api/v1alpha1"
	"github.com/jnnkrdb/r8r/pkg/reconciliation"
)

// This function is used to handle the errors, whichget thrown by the reconciliation.
// It packs together the log, the conditions and the events.
//
// parameters:
//   - ctx context.Contex -> this is the default given context
//   - err error          -> this is the thrown error, which should be handled
func OnError(
	r reconciliation.Reconciler,
	ctx context.Context,
	co *clusterv1alpha1.ClusterObject,
	err error,
	event,
	msg string) error {

	// if the error is in fact nil, then leave early
	if err == nil {
		return err
	}

	var _log = log.FromContext(ctx)

	// log the message with the error in the binary logs
	_log.Error(err, msg)

	// throw the event to the object
	r.GetRecorder().Eventf(
		co,
		co,
		"Warning",
		fmt.Sprintf("%sError", event),
		"an error occurred during reconciliation",
		"%s: %v", msg, err,
	)

	// set the condition if any
	if e := Set(r, ctx,
		co,
		Condition_Ready,
		metav1.ConditionFalse,
		fmt.Sprintf("Failed%s", event),
		"%s: %v", msg, err,
	); e != nil {
		return e
	}
	return err
}
