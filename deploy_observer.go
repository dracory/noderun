package noderunner

import (
	nodestore "github.com/dracory/nodestore"
)

import "net/http"

// DeployObserver is the engine's generic run-lifecycle seam: an
// optional hook the host wires (like HTTPAuthenticator, FileRoot or
// nodes.ContextStore) to observe deploys and stops. Nil — the default —
// means no observation; a consumer that installs nothing gets zero
// overhead and zero behavior change. pkg/flowdashboard installs
// itself through it; other subsystems (metrics, audit) could too.
//
// Both events are raised inline on the deploy/stop path, so
// implementations must be cheap and must not call back into
// DeployWorkflow/StopWorkflow (deadlock). The engine holds no locks
// it does not already hold at those points — see the call sites.
type DeployObserver interface {
	// WorkflowActivated fires after a run's activation succeeded —
	// initial deploy, full redeploy and partial redeploy alike. The
	// argument is the just-activated workflow definition; treat it
	// as read-only.
	WorkflowActivated(workflow nodestore.WorkflowInterface)
	// WorkflowStopped fires when a run's registration is torn down:
	// StopWorkflow and abandonRun (failed activation) alike. The id
	// may refer to a run the observer never saw activate.
	WorkflowStopped(workflowID string)
}

// SetDeployObserver installs the engine's run lifecycle observer;
// pass nil to remove it. Wire it before the first DeployWorkflow —
// the field is not synchronized against mid-run swaps (same contract
// as HTTPAuthenticator).
func (e *WorkflowEngine) SetDeployObserver(o DeployObserver) {
	e.deployObserver = o
}

// RunningWorkflows returns the definitions of all currently
// registered runs. A late-installed DeployObserver uses it to replay
// WorkflowActivated for runs that predate it (e.g. a dashboard hub
// mounted after workflows were already deployed). Definitions are
// the live run copies — treat as read-only.
func (e *WorkflowEngine) RunningWorkflows() []nodestore.WorkflowInterface {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	out := make([]nodestore.WorkflowInterface, 0, len(e.flows))
	for _, wf := range e.flows {
		out = append(out, wf)
	}
	return out
}

// HTTPAuthenticatorFunc returns the engine-configured HTTP
// authenticator (see the HTTPAuthenticator field); nil when unset.
// Lets optional packages reuse the engine's credentials without
// reaching into the struct field through an interface seam.
func (e *WorkflowEngine) HTTPAuthenticatorFunc() func(r *http.Request) (username string, ok bool) {
	return e.HTTPAuthenticator
}
