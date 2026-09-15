package actions

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/status"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (rr *ResourceRequest) Update(ctx context.Context, statushandler *status.StatusHandler) error {

	// get the logger from the context
	var _log = logf.FromContext(ctx).WithValues("func", "actions.(*RequestedResource).Update()")

	_log.Info("updating resource")

	var _newResource = rr.Resource.DeepCopy()
	_newResource.SetNamespace(rr.Namespace.Name)

	return fmt.Errorf("not implemented")
}
