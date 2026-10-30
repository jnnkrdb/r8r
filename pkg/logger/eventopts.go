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

type EventOpts interface {
	GetRelated() runtime.Object
	GetEventType() EventType
	GetAction() string
	Throw(recorder events.EventRecorder, object client.Object, reason, note string, args ...any)
}

// this struct is used to give eventOpts to the logger, to throw an event with specific values
type Event struct {
	Related   runtime.Object
	EventType EventType
	Action    string
}

// get the related object
func (e Event) GetRelated() runtime.Object {
	return e.Related
}

// get the related object
func (e Event) GetEventType() EventType {
	return e.EventType
}

// get the related object
func (e Event) GetAction() string {
	return e.Action
}

// throw the event for the given object
func (e Event) Throw(recorder events.EventRecorder, object client.Object, reason, note string, args ...any) {
	if recorder != nil && object != nil {
		recorder.Eventf(
			object,
			e.Related,
			string(e.EventType),
			reason,
			e.Action,
			note,
			args...,
		)
	}
}
