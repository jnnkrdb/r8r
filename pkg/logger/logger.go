package logger

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"

	"k8s.io/client-go/tools/events"
)

// default implementation of an eventhandler
type eventLogger struct {
	log logr.Logger
	rec events.EventRecorder
	obj client.Object

	eventOpts IEventOpts
}

// create an eventlogger from context and the reffering object
func NewEventLogger(
	recorder events.EventRecorder,
	logger logr.Logger,
	object client.Object,
) EventHandler {
	return &eventLogger{
		log:       logger,
		rec:       recorder,
		obj:       object,
		eventOpts: nil,
	}
}

// print normal INFO event
func (l *eventLogger) Info(msg string, keysAndValues ...any) {
	l.log.Info(msg, keysAndValues...)
}

// print normal INFO event, with additional event
func (l *eventLogger) InfoWithEvent(msg string, eventOpts IEventOpts, keysAndValues ...any) {
	l.Info(msg, keysAndValues...)

	if eventOpts != nil {
		eventOpts.throw(l.obj, l.rec)
	}
}

// print normal ERROR event
func (l *eventLogger) Error(err error, msg string, keysAndValues ...any) {
	l.log.Error(err, msg, keysAndValues...)
}

// print normal ERROR event, with additional event
func (l *eventLogger) ErrorWithEvent(err error, msg string, eventOpts IEventOpts, keysAndValues ...any) {
	l.Error(err, msg, keysAndValues...)

	if eventOpts != nil {
		eventOpts.throw(l.obj, l.rec)
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

// this is a passing function, that just creates a new instance of a looger,
// that throws the next log line as an event as well as a normal log
func (l *eventLogger) WithEvent(e EventType, reason string) EventHandler {

}
