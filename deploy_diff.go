package noderunner

import (
	"bytes"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
)

// DeployMode selects how a deploy treats a workflow that is already
// running (Node-RED's deploy modes: full / modified flows / modified
// nodes — GoFlow offers full and partial).
type DeployMode int

const (
	// DeployFull restarts every node: all stoppers are shut down and
	// re-activated. This is the default and matches a cold deploy.
	DeployFull DeployMode = iota
	// DeployPartial diffs the incoming flow against the running one
	// and only stops/starts the changed nodes. Unchanged Stoppers
	// (tcpIn/mqttIn listeners, inject timers, queue drainers) keep
	// running — Node-RED "modified nodes" semantics.
	DeployPartial
)

// flowDiff returns the set of node IDs that changed between the
// running expanded flow and the incoming expanded flow: added,
// removed, reconfigured, or touched by a wiring change. Both ends of
// a changed edge are marked dirty so wiring rewires restart the
// affected nodes. nodestore.NodeInterface Position is ignored (a canvas move is not a
// redeploy-worthy change); a change to workflow-level View defaults
// (inbox capacity/durability) marks every node dirty. Callers pass
// expanded graphs, so a subflow definition edit shows up as changed
// namespaced internal nodes.
func flowDiff(oldFlow, newFlow nodestore.WorkflowInterface) map[string]bool {
	dirty := make(map[string]bool)

	oldNodes := make(map[string]nodestore.NodeInterface, len(oldFlow.GetNodes()))
	newNodes := make(map[string]nodestore.NodeInterface, len(newFlow.GetNodes()))
	for _, n := range oldFlow.GetNodes() {
		oldNodes[n.GetID()] = n
	}
	for _, n := range newFlow.GetNodes() {
		newNodes[n.GetID()] = n
		o, ok := oldNodes[n.GetID()]
		if !ok || !sameNode(o, n) {
			dirty[n.GetID()] = true // added or reconfigured
		}
	}
	for _, n := range oldFlow.GetNodes() {
		if _, ok := newNodes[n.GetID()]; !ok {
			dirty[n.GetID()] = true // removed
		}
	}

	// Wiring: multiset-diff edges by (source, port, target, port).
	// nodestore.EdgeInterface IDs are editor artifacts — rewiring keeps semantics. An
	// edge that appears more often on one side than the other is an
	// added/removed wire: both its ends restart.
	oldCounts := edgeCounts(oldFlow.GetEdges())
	for _, e := range newFlow.GetEdges() {
		k := edgeKey(e)
		oldCounts[k]--
		if oldCounts[k] < 0 {
			dirty[e.GetSource()] = true
			dirty[e.GetTarget()] = true
		}
	}
	newCounts := edgeCounts(newFlow.GetEdges())
	for _, e := range oldFlow.GetEdges() {
		k := edgeKey(e)
		newCounts[k]--
		if newCounts[k] < 0 {
			dirty[e.GetSource()] = true
			dirty[e.GetTarget()] = true
		}
	}

	// nodestore.WorkflowInterface-level defaults affect every node's runtime behavior.
	if !jsonEqual(oldFlow.GetView(), newFlow.GetView()) {
		for id := range oldNodes {
			dirty[id] = true
		}
		for id := range newNodes {
			dirty[id] = true
		}
	}
	return dirty
}

// sameNode reports whether two node definitions are equivalent for
// deploy purposes: same type and same configuration Data. Position
// is deliberately excluded.
func sameNode(a, b nodestore.NodeInterface) bool {
	return a.GetType() == b.GetType() && jsonEqual(a.GetData(), b.GetData())
}

// edgeKey identifies an edge by its endpoints and ports, ignoring
// the editor-assigned edge ID.
func edgeKey(e nodestore.EdgeInterface) string {
	return e.GetSource() + "\x00" + e.GetSourceKey() + "\x00" + e.GetTarget() + "\x00" + e.GetTargetKey()
}

// edgeCounts builds a multiset of edge keys.
func edgeCounts(edges []nodestore.EdgeInterface) map[string]int {
	counts := make(map[string]int, len(edges))
	for _, e := range edges {
		counts[edgeKey(e)]++
	}
	return counts
}

// jsonEqual compares two decoded-JSON values by their canonical
// marshaling (map keys sort deterministically). A nil map and an
// empty map count as equal — storage round-trips can flip one into
// the other.
func jsonEqual(a, b interface{}) bool {
	ea, eb := emptyJSONValue(a), emptyJSONValue(b)
	if ea || eb {
		return ea == eb
	}
	ra, _ := json.Marshal(a)
	rb, _ := json.Marshal(b)
	return bytes.Equal(ra, rb)
}

// emptyJSONValue reports whether v is nil or an empty map.
func emptyJSONValue(v interface{}) bool {
	if v == nil {
		return true
	}
	if m, ok := v.(map[string]interface{}); ok {
		return len(m) == 0
	}
	return false
}
