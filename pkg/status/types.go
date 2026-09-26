package status

// default event types, which are used in the reconciliation.
const (
	EventType_Normal  = "Normal"
	EventType_Warning = "Warning"
)

// specific condition types, which are used in the status of the objects, which are used in the reconciliation.
const (
	Condition_Ready       = "Ready"
	Condition_Progressing = "Progressing"
	Condition_Complete    = "Complete"
)
