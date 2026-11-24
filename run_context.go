package noderunner

import (
	"context"
)

// runContextKey is the private key type for values the engine stamps
// onto a run's context.
type runContextKey struct{ name string }

// workflowIDKey carries the running workflow's id.
var workflowIDKey = runContextKey{"workflowID"}

// ContextWithWorkflowID returns ctx carrying the run's workflow id.
// The engine stamps it once when a run context is created (startRun),
// so every Execute/Activate — deploy-time activation, wired delivery,
// inject and emit traversals alike — observes the same value.
func ContextWithWorkflowID(ctx context.Context, workflowID string) context.Context {
	return context.WithValue(ctx, workflowIDKey, workflowID)
}

// WorkflowIDFromContext returns the id of the workflow run ctx belongs
// to. nodestore.NodeInterface packs whose factories receive only the engine (the frozen
// nodes.NodeFactory signature has no workflow parameter) use it to attribute
// side effects — e.g. a dashboard widget pushing state to the hub —
// to the correct run. ok is false outside a run traversal (e.g. a node
// executed directly in a test).
func WorkflowIDFromContext(ctx context.Context) (workflowID string, ok bool) {
	id, ok := ctx.Value(workflowIDKey).(string)
	return id, ok && id != ""
}
