package status

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/reconciliation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// status handler object, to handle everything around an objects status, like events, conditions, etc.
type StatusHandler struct {

	// current active context, which is used to get the client and the recorder
	ctx context.Context

	// access the reconciler, which is used to get the client and the recorder
	reconciler reconciliation.Reconciler

	// this contains the parent object, which is referred to in the events
	parent client.Object

	// default statusfields, which are used to handle the status of the object
	defaultStatusFields *DefaultStatusFields
}

// create a new event handler, which is used to handle the events, which get thrown by the reconciliation.
func NewStatusHandler(
	ctx context.Context,
	r reconciliation.Reconciler,
	po client.Object,
	dsf *DefaultStatusFields) *StatusHandler {

	return &StatusHandler{
		ctx:                 ctx,
		reconciler:          r,
		parent:              po,
		defaultStatusFields: dsf,
	}
}

// throw an event to the parent object, which is referred to in the events
func (sh *StatusHandler) ThrowEvent(
	relatedobject client.Object,
	eventtype, reason, action, note string,
	args ...any) {

	sh.reconciler.GetRecorder().Eventf(sh.parent, relatedobject, string(eventtype), reason, action, note, args...)
}

// throws an event with a condtion afterwards
//
// parameters:
//   - err error                   -> this is the thrown error, which should be handled
func (sh *StatusHandler) ThrowEventWithConditionOnError(
	err error,
	relatedobject client.Object,
	eventtype, reason, action, note string,
	args ...any) error {

	// if the error is in fact nil, then leave early
	if err == nil {
		return err
	}

	sh.ThrowEvent(relatedobject, eventtype, reason, action, note, append(args, err)...)

	if e := sh.SetCondition(metav1.Condition{
		Type:    Condition_Complete,
		Status:  metav1.ConditionFalse,
		Reason:  fmt.Sprintf("Failed%s", reason),
		Message: fmt.Sprintf("%s: error: %v // args: %v", note, err, args),
	}); e != nil {
		return e
	}
	return err
}

// find a specific confition
func (sh *StatusHandler) FindCondition(conditionType string) *metav1.Condition {

	for i := range sh.defaultStatusFields.Conditions {

		if sh.defaultStatusFields.Conditions[i].Type == conditionType {

			return &sh.defaultStatusFields.Conditions[i]
		}
	}

	return nil
}

// set a specific condition
func (sh *StatusHandler) SetCondition(condition metav1.Condition) error {

	condition.ObservedGeneration = sh.parent.GetGeneration()

	if _condition := sh.FindCondition(condition.Type); _condition == nil {

		condition.LastTransitionTime = metav1.Now()
		sh.defaultStatusFields.Conditions = append(sh.defaultStatusFields.Conditions, condition)

	} else {

		if _condition.Status != condition.Status || _condition.Reason != condition.Reason || _condition.Message != condition.Message {
			condition.LastTransitionTime = metav1.Now()
		} else {
			condition.LastTransitionTime = _condition.LastTransitionTime
		}

		*_condition = condition
	}
	return sh.reconciler.GetClient().Status().Update(sh.ctx, sh.parent, &client.SubResourceUpdateOptions{})
}
