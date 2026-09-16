package logger

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type EventType string

const (
	Normal  EventType = "Normal"
	Warning EventType = "Warning"
)

// this struct is used to give eventOpts to the logger, to throw an event with specific values
type EventOpts struct {
	regarding runtime.Object
	related   runtime.Object
	EventType EventType
	Reason    string
	Action    string
	note      string
	args      []interface{}
}

type IEventOpts interface {
	throw(object client.Object, recorder events.EventRecorder)
}

// throw the event for the given object
func (eo EventOpts) throw(object client.Object, recorder events.EventRecorder) {
	if recorder != nil && object != nil {
		recorder.Eventf(
			object,
			eo.related,
			string(eo.EventType),
			eo.Reason,
			eo.Action,
			eo.note,
			eo.args...,
		)
	}
}
