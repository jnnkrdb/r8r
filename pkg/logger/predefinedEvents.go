package logger

var (
	// default event for failing namespace gathering
	Event_FailedNamespaceGathering = Event{
		EventType: Warning,
		Action:    "NamespaceGathering",
	}

	// default event for finishing a reconciliation
	Event_SuccessfullResourceReplication = Event{
		EventType: Normal,
		Action:    "ResourceReplication",
	}
)
