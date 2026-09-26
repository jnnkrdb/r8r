package logger

// the typical event handler interface with all required functions
type EventHandler interface {
	Info(msg string, keysAndValues ...any)
	InfoWithEvent(msg string, eventOpts EventOpts, keysAndValues ...any)
	Error(err error, msg string, keysAndValues ...any)
	ErrorWithEvent(err error, msg string, eventOpts EventOpts, keysAndValues ...any)
	WithValues(keysAndValues ...any) EventHandler
	V(level int) EventHandler
}
