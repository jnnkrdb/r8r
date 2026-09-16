package replication

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ReplicationHandler interface {
}

type DefaultReplicationHandler struct {
	c        client.Client
	scheme   *runtime.Scheme
	recorder events.EventRecorder
}
