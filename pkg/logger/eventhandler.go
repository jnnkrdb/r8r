package logger

import "context"

type eventhandlerCtxKey string

const ctxKey eventhandlerCtxKey = "EventHandler"

// the typical event handler interface with all required functions
type EventHandler interface {
	Info(msg string, keysAndValues ...any)
	InfoWithEvent(msg string, eventOpts EventOpts, keysAndValues ...any)
	Error(err error, msg string, keysAndValues ...any)
	ErrorWithEvent(err error, msg string, eventOpts EventOpts, keysAndValues ...any)
	WithValues(keysAndValues ...any) EventHandler
	V(level int) EventHandler
}

// get an eventlooger into a context
func IntoContext(ctx context.Context, e EventHandler) context.Context {
	return context.WithValue(ctx, ctxKey, e)
}

// get an eventlogger from the context
func FromContext(ctx context.Context) (EventHandler, bool) {
	e, ok := ctx.Value(ctxKey).(EventHandler)
	return e, ok
}
