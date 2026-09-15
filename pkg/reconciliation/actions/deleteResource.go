package actions

import (
	"context"
	"fmt"

	"github.com/jnnkrdb/r8r/pkg/status"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (rr *ResourceRequest) Delete(ctx context.Context, statushandler *status.StatusHandler) error {

	// get the logger from the context
	var _log = logf.FromContext(ctx).WithValues("func", "actions.(*RequestedResource).Delete()")

	_log.Info("deleting resource")

	var _newResource = rr.Resource.DeepCopy()
	_newResource.SetNamespace(rr.Namespace.Name)

	return fmt.Errorf("not implemented")
}
