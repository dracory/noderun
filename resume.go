package noderunner

import (
	"context"
	"fmt"
	"time"

	nodestore "github.com/dracory/nodestore"
)

// ResumeDeployed re-activates every workflow whose stored deployed
// flag is set — the boot-time continuation path (durability is a
// semantic, not a flag). Called in the background by the host on
// mount (e.g. nodeadmin), it returns one error per workflow that
// failed to redeploy; an empty result means a clean resume.
//
// A crash mid-traversal is a full redeploy of that workflow — the
// engine restarts it from activation. Durable inbox rows left
// undelivered are drained by the deploy path itself (requeueDurable
// runs at the tail of executeWorkflow), so persisted-but-undelivered
// messages resume processing after restart. Node-level durable state
// (queue-dir backends) reconciles when the store is opened.
func (e *WorkflowEngine) ResumeDeployed(ctx context.Context, store nodestore.StoreInterface) []error {
	workflows, err := store.WorkflowList(ctx, nodestore.WorkflowQuery().WithDeployed(true))
	if err != nil {
		return []error{fmt.Errorf("list deployed workflows: %w", err)}
	}
	var errs []error
	for _, wf := range workflows {
		if wf == nil || !wf.GetDeployed() {
			continue
		}
		if _, err := e.DeployWorkflow(ctx, wf, nodestore.NewMessage().SetTimestamp(time.Now())); err != nil {
			errs = append(errs, fmt.Errorf("resume workflow %q: %w", wf.GetID(), err))
		}
	}
	return errs
}
