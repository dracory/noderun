package noderunner

import (
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"time"
)

// This file implements the engine side of the Node-RED event nodes
// (catch / status / complete). The node executors themselves are
// pass-throughs (registered in executorForNode); all the work is
// routing engine events into the outgoing edges of in-scope nodes:
//
//   - catch:     a node executor error pushes an error message
//     {error: {message, source:{id,type,name}}, originalData} into
//     every catch node whose scope covers the faulting node.
//   - status:    every per-node status transition (running|ok|error)
//     pushes {status:{nodeId,status}, nodebase.NodeName} into status nodes
//     whose scope covers the reporting node.
//   - complete:  when an injected message's downstream traversal
//     drains, complete nodes whose scope covers the triggering node
//     receive {complete: true}.
//
// Recursion guards: emitted event messages carry a Metadata flag
// ("fromCatch"/"fromStatus"/"fromComplete") so an event node reacting
// to its own event cannot loop forever.

// NodesInScope returns the ids of nodes of nodeType in the running
// workflow whose scope covers sourceNodeID. A node's scope is
// configured via Data["scope"] ("all" — default/empty — or
// "selected") and Data["nodes"] (a []interface{} of ids or a
// comma-separated string). The source node itself is never returned.
func (e *WorkflowEngine) NodesInScope(workflowID, sourceNodeID, nodeType string) []string {
	e.runMu.Lock()
	workflow, ok := e.flows[workflowID]
	e.runMu.Unlock()
	if !ok {
		return nil
	}

	var ids []string
	for _, node := range workflow.GetNodes() {
		if node.GetType() != nodeType || node.GetID() == sourceNodeID {
			continue
		}
		if scopeCovers(node, sourceNodeID) {
			ids = append(ids, node.GetID())
		}
	}
	return ids
}

// scopeCovers reports whether node's scope configuration includes
// sourceNodeID: empty/"all" covers every node, "selected" covers only
// the configured node ids.
func scopeCovers(node nodestore.NodeInterface, sourceNodeID string) bool {
	scope, _ := node.GetData()["scope"].(string)
	if scope != "selected" {
		return true // "all", empty or unknown scopes default to flow-wide
	}
	for _, id := range nodebase.ParseLinkList(node.GetData()["nodes"]) {
		if id == sourceNodeID {
			return true
		}
	}
	return false
}

// metadataFlag reports whether the payload's metadata carries a true
// boolean under key (the recursion-guard markers).
func metadataFlag(p nodestore.MessageInterface, key string) bool {
	if p == nil || p.GetMetadata() == nil {
		return false
	}
	v, _ := p.GetMetadata()[key].(bool)
	return v
}

// dispatchCatch routes a node executor error to every catch node in
// scope. The error message inherits the original msgId (via the
// preserveMsgId flag honoured by inject) so a downstream httpResponse
// still answers the request that triggered the failed flow. Messages
// that already came from a catch node are not re-reported — that
// would loop on a failing catch branch. It reports whether at least
// one catch node claimed the failure; callers use that to fall back
// to the problem queue (RouteFailure).
func (e *WorkflowEngine) dispatchCatch(workflowID string, node nodestore.NodeInterface, in nodestore.MessageInterface, cause error) bool {
	if metadataFlag(in, "fromCatch") {
		return false
	}
	catchIDs := e.NodesInScope(workflowID, node.GetID(), "catch")
	if len(catchIDs) == 0 {
		return false
	}

	var originalData interface{}
	var msgID string
	if in != nil {
		originalData = in.GetPayload()
		msgID, _ = in.GetMetadata()["msgId"].(string)
	}

	for _, catchID := range catchIDs {
		payload := nodestore.NewMessage().SetPayload(map[string]interface{}{
			"error": map[string]interface{}{
				"message": cause.Error(),
				"source": map[string]interface{}{
					"id":   node.GetID(),
					"type": node.GetType(),
					"name": nodebase.NodeName(node),
				},
			},
			"originalData": originalData,
		}).SetMetadata(map[string]interface{}{"fromCatch": true}).SetTimestamp(time.Now())
		if msgID != "" {
			payload.GetMetadata()["msgId"] = msgID
			payload.GetMetadata()["preserveMsgId"] = true
		}
		// Best effort: a missing/dead catch branch must not mask the
		// original failure, and the failed branch still aborts.
		_ = e.Emit(workflowID, catchID, payload, nil)
	}
	return true
}

// notifyStatus routes a node status transition to every status node in
// scope. Status nodes never report their own transitions (they are
// event sources with no input), and messages produced by a status
// node carry "fromStatus" so their downstream transitions do not
// re-trigger the same status node.
func (e *WorkflowEngine) notifyStatus(workflowID string, node nodestore.NodeInterface, status string, in nodestore.MessageInterface) {
	if node.GetType() == "status" || metadataFlag(in, "fromStatus") {
		return
	}
	for _, statusID := range e.NodesInScope(workflowID, node.GetID(), "status") {
		payload := nodestore.NewMessage().SetPayload(map[string]interface{}{
			"status": map[string]interface{}{
				"nodeId": node.GetID(),
				"status": status,
			},
			"nodebase.NodeName": nodebase.NodeName(node),
		}).SetMetadata(map[string]interface{}{"fromStatus": true}).SetTimestamp(time.Now())
		_ = e.Emit(workflowID, statusID, payload, nil)
	}
}

// notifyComplete fires when an injected message's downstream traversal
// has drained: every complete node whose scope covers the triggering
// node receives {complete: true}. Messages produced by a complete node
// carry "fromComplete" so their own traversal end cannot re-fire it.
func (e *WorkflowEngine) notifyComplete(workflowID, fromNodeID string, in nodestore.MessageInterface) {
	if metadataFlag(in, "fromComplete") {
		return
	}
	for _, completeID := range e.NodesInScope(workflowID, fromNodeID, "complete") {
		payload := nodestore.NewMessage().SetPayload(map[string]interface{}{"complete": true}).SetMetadata(map[string]interface{}{"fromComplete": true}).SetTimestamp(time.Now())
		_ = e.Emit(workflowID, completeID, payload, nil)
	}
}
