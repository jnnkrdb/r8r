package logger

import (
	"fmt"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"

	"k8s.io/client-go/tools/events"
)

// default implementation of an eventhandler
type eventLogger struct {
	log logr.Logger
	rec events.EventRecorder
	obj client.Object
}

// create an eventlogger from context and the reffering object
func NewEventLogger(recorder events.EventRecorder, logger logr.Logger, object client.Object) EventHandler {
	return &eventLogger{
		log: logger,
		rec: recorder,
		obj: object,
	}
}

// print normal INFO event
func (l *eventLogger) Info(msg string, keysAndValues ...any) {
	l.log.Info(msg, keysAndValues...)
}

// print normal INFO event, with additional event
func (l *eventLogger) InfoWithEvent(msg string, eventOpts EventOpts, keysAndValues ...any) {
	l.Info(msg, keysAndValues...)

	if eventOpts != nil {
		eventOpts.Throw(
			l.rec,
			l.obj,
			strings.ReplaceAll(fmt.Sprintf("Successful%s", eventOpts.GetAction()), " ", ""),
			msg,
			keysAndValues...,
		)
	}
}

// print normal ERROR event
func (l *eventLogger) Error(err error, msg string, keysAndValues ...any) {
	l.log.Error(err, msg, keysAndValues...)
}

// print normal ERROR event, with additional event
func (l *eventLogger) ErrorWithEvent(err error, msg string, eventOpts EventOpts, keysAndValues ...any) {
	l.Error(err, msg, keysAndValues...)

	if eventOpts != nil {
		eventOpts.Throw(
			l.rec,
			l.obj,
			strings.ReplaceAll(fmt.Sprintf("Failed%s", eventOpts.GetAction()), " ", ""),
			msg,
			append(keysAndValues, err)...,
		)

	}
}

// level based logging
func (l *eventLogger) V(level int) EventHandler {
	return &eventLogger{
		log: l.log.V(level),
		rec: l.rec,
		obj: l.obj,
	}
}

// return the eventhandler with specific keys and values
func (l *eventLogger) WithValues(keysAndValues ...any) EventHandler {
	return &eventLogger{
		log: l.log.WithValues(keysAndValues...),
		rec: l.rec,
		obj: l.obj,
	}
}
