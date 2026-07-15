package reconciliation

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// this implements the reconciler interface, which enables some
// methods to get all possible reconcilers, if required
type Reconciler interface {
	SetupWithManager(ctrl.Manager) error
	GetClient() client.Client
	GetScheme() *runtime.Scheme
	GetRecorder() events.EventRecorder
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}
