package logger

// the typical event handler interface with all required functions
type EventHandler interface {
	Info(msg string, keysAndValues ...any)
	InfoWithEvent(msg string, eventOpts EventOpts, keysAndValues ...any)
	Error(err error, msg string, keysAndValues ...any)
	ErrorWithEvent(err error, eventOpts EventOpts, msg string, keysAndValues ...any)
	WithValues(keysAndValues ...any) EventHandler
	WithEvent(EventOpts) EventHandler
}
