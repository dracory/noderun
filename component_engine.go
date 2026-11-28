package noderunner

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Component engine support: ExpandComponents rewrites component instance nodes
// into namespaced copies of the definition's internal nodes at deploy time.
// The domain-side component helpers live in package nodestore (component.go).

const maxComponentDepth = 16

// componentWire is one endpoint inside a definition's in/out port:
// {id: internalNodeId, port: sourcePortIndex (out only, 0-based)}.
type componentWire struct {
	ID   string `json:"id"`
	Port int    `json:"port"`
}

// componentPort is one entry of a definition's in/out array.
type componentPort struct {
	X     float64         `json:"x"`
	Y     float64         `json:"y"`
	Wires []componentWire `json:"wires"`
}

// componentEnvVar is one declared env entry: {name, type, value}.
type componentEnvVar struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// componentDef is the parsed component definition node.
type componentDef struct {
	id    string
	in    []componentPort
	out   []componentPort
	env   []componentEnvVar
	nodes []nodestore.NodeInterface
	edges []nodestore.EdgeInterface
}

// componentInstance holds one instance's expansion result: the
// namespaced internal nodes and edges plus the port endpoint lists
// used to graft external edges onto the copy.
type componentInstance struct {
	nodes []nodestore.NodeInterface
	edges []nodestore.EdgeInterface
	// inTargets[i] lists the namespaced node ids an edge into the
	// instance's input_{i+1} port must feed.
	inTargets [][]string
	// outSources[i] lists the namespaced {nodeId, port} pairs feeding
	// the instance's output_{i+1} port.
	outSources [][]componentWire
}

// nodestore.IsComponentInstance reports whether nodeType names a component instance
// ("component:<defId>").

// maxExpandedNodes bounds the total node count component expansion may
// produce (shared budget across all nesting levels). Depth is capped
// at maxComponentDepth, but nothing bounds the product — K nested
// instances per level multiply to K^depth copies, so a crafted flow
// could exhaust memory at deploy. The counter is decremented as each
// instance's inner graph is produced, so the runaway is caught while
// expanding, not after.
const maxExpandedNodes = 100000

// ExpandComponents rewrites w in place: every "component:<defId>" instance
// node is replaced by a namespaced copy of its definition's internal
// graph and the definition nodes themselves are removed (they are
// templates, not executable nodes). Definitions are parsed from nodes
// of type "component" living in the same workflow. allowOSEnv permits
// "env"-typed env vars to read the process environment (see
// WorkflowEngine.AllowOSEnv). Returns a descriptive error for unknown
// definitions, out-of-range ports, recursive nesting and expansion
// beyond maxExpandedNodes.
func ExpandComponents(w nodestore.WorkflowInterface, allowOSEnv bool) error {
	defs := map[string]*componentDef{}
	var rest []nodestore.NodeInterface
	for _, n := range w.GetNodes() {
		if n.GetType() != componentDefNodeType {
			rest = append(rest, n)
			continue
		}
		def, err := parseComponentDef(n)
		if err != nil {
			return fmt.Errorf("component definition %q: %w", n.GetID(), err)
		}
		defs[n.GetID()] = def
	}
	// Expand into locals and commit only on success — a failed
	// expansion must leave the caller's workflow untouched. There is
	// no len(defs) shortcut: an orphaned "component:<id>" instance must
	// still fail here with the clear "definition not found" error
	// instead of dying later as an unknown node type.
	budget := maxExpandedNodes
	expandedNodes, edges, _, err := expandComponentInstances(rest, w.GetEdges(), defs, nil, nil, allowOSEnv, &budget)
	if err != nil {
		return err
	}
	w.SetNodes(expandedNodes)
	w.SetEdges(edges)
	return nil
}

// parseComponentDef decodes a type:"component" node's Data into a
// componentDef. The definition payload uses the Node-RED field names:
// name, in, out, env, meta, plus the engine's storage keys "nodes" and
// "edges" for the internal graph. Internal nodes accept both the
// backend shape ({id,type,position:{x,y},data:{...}}) and the
// Node-RED flat shape ({id,type,x,y,...config}).
func parseComponentDef(n nodestore.NodeInterface) (*componentDef, error) {
	raw, err := json.Marshal(n.GetData())
	if err != nil {
		return nil, fmt.Errorf("invalid definition data: %w", err)
	}
	var aux struct {
		In    []componentPort   `json:"in"`
		Out   []componentPort   `json:"out"`
		Env   []componentEnvVar `json:"env"`
		Nodes []json.RawMessage `json:"nodes"`
		Edges []json.RawMessage `json:"edges"`
	}
	if err := json.Unmarshal(raw, &aux); err != nil {
		return nil, fmt.Errorf("invalid definition data: %w", err)
	}

	def := &componentDef{id: n.GetID(), in: aux.In, out: aux.Out, env: aux.Env}
	for i, rawNode := range aux.Nodes {
		node, err := parseComponentInnerNode(rawNode)
		if err != nil {
			return nil, fmt.Errorf("internal node %d: %w", i, err)
		}
		def.nodes = append(def.nodes, node)
	}
	for i, rawEdge := range aux.Edges {
		var j edgeJSON
		if err := json.Unmarshal(rawEdge, &j); err != nil {
			return nil, fmt.Errorf("internal edge %d: %w", i, err)
		}
		def.edges = append(def.edges, edgeFromJSON(j))
	}
	return def, nil
}

// parseComponentInnerNode decodes one internal node of a definition,
// accepting both the backend ({position, data}) and the Node-RED flat
// ({x, y, ...config}) shapes.

// expandComponentInstances replaces every component instance in nodes by
// the namespaced copy produced by def.expand, then rewires the edges:
// an edge into an instance feeds the endpoint nodes of its in port,
// an edge out of an instance carries the source nodes of its out port,
// and an edge between two instances connects inner sources to inner
// targets directly. Edges between ordinary nodes pass through.
// Nested instances are expanded recursively inside def.expand so
// their inner env resolves against the enclosing instance's env
// (env inheritance). The returned map is keyed by instance node id
// and carries each instance's port endpoint lists — the caller needs
// them to resolve ports pointing at a (now vanished) instance node.
func expandComponentInstances(srcNodes []nodestore.NodeInterface, edges []nodestore.EdgeInterface, defs map[string]*componentDef, parentEnv map[string]interface{}, ancestry []string, allowOSEnv bool, budget *int) ([]nodestore.NodeInterface, []nodestore.EdgeInterface, map[string]*componentInstance, error) {
	instanceIDs := map[string]bool{}
	for _, n := range srcNodes {
		if nodestore.IsComponentInstance(n.GetType()) {
			instanceIDs[n.GetID()] = true
		}
	}
	if len(instanceIDs) == 0 {
		// Still reject edges pointing at definition srcNodes — they are
		// removed by the caller and would dangle silently.
		for _, e := range edges {
			if id, bad := edgeTouchesComponentDef(e, defs); bad {
				return nil, nil, nil, fmt.Errorf("edge %q references component definition %q, which is not a runnable node", e.GetID(), id)
			}
		}
		return srcNodes, edges, nil, nil
	}
	if len(ancestry) >= maxComponentDepth {
		return nil, nil, nil, fmt.Errorf("component expansion exceeds %d nested levels", maxComponentDepth)
	}

	instances := map[string]*componentInstance{}
	var outNodes []nodestore.NodeInterface
	var outEdges []nodestore.EdgeInterface
	for _, n := range srcNodes {
		if !instanceIDs[n.GetID()] {
			outNodes = append(outNodes, n)
			continue
		}
		defID := strings.TrimPrefix(n.GetType(), componentTypePrefix)
		def, ok := defs[defID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("component instance %q: definition %q not found", n.GetID(), defID)
		}
		for _, a := range ancestry {
			if a == defID {
				return nil, nil, nil, fmt.Errorf("component instance %q: recursive expansion of component %q is not allowed", n.GetID(), defID)
			}
		}
		inst, err := def.expand(n, defs, parentEnv, append(ancestry, defID), allowOSEnv, budget)
		if err != nil {
			return nil, nil, nil, err
		}
		instances[n.GetID()] = inst
		outNodes = append(outNodes, inst.nodes...)
		outEdges = append(outEdges, inst.edges...)
	}

	// Wire the expanded copies: for each original edge, resolve its
	// endpoints against the port endpoint lists of any instance it
	// touches, and emit the concrete inner edges.
	for _, e := range edges {
		srcInst := instances[e.GetSource()]
		dstInst := instances[e.GetTarget()]
		switch {
		case srcInst == nil && dstInst == nil:
			if id, bad := edgeTouchesComponentDef(e, defs); bad {
				return nil, nil, nil, fmt.Errorf("edge %q references component definition %q, which is not a runnable node", e.GetID(), id)
			}
			outEdges = append(outEdges, e)
		case srcInst != nil && dstInst == nil:
			port := outputPortIndex(e.GetSourceKey())
			if port >= len(srcInst.outSources) {
				return nil, nil, nil, fmt.Errorf("component instance %q: output port %d exceeds the definition's %d output(s)", e.GetSource(), port+1, len(srcInst.outSources))
			}
			for _, s := range srcInst.outSources[port] {
				outEdges = append(outEdges, nodestore.NewEdge(nodebase.GenerateID(), s.ID, e.GetTarget()).SetSourceKey(fmt.Sprintf("output_%d", s.Port+1)).SetTargetKey(e.GetTargetKey()))
			}
		case srcInst == nil && dstInst != nil:
			port := inputPortIndex(e.GetTargetKey())
			if port >= len(dstInst.inTargets) {
				return nil, nil, nil, fmt.Errorf("component instance %q: input port %d exceeds the definition's %d input(s)", e.GetTarget(), port+1, len(dstInst.inTargets))
			}
			for _, target := range dstInst.inTargets[port] {
				outEdges = append(outEdges, nodestore.NewEdge(nodebase.GenerateID(), e.GetSource(), target).SetSourceKey(e.GetSourceKey()).SetTargetKey("input_1"))
			}
		default:
			if srcInst == nil || dstInst == nil {
				return nil, nil, nil, fmt.Errorf("edge %q has inconsistent component endpoint state", e.GetID())
			}
			// Instance → instance: connect the source instance's
			// inner out-port srcNodes to the target instance's inner
			// in-port srcNodes.
			outPort := outputPortIndex(e.GetSourceKey())
			if outPort >= len(srcInst.outSources) {
				return nil, nil, nil, fmt.Errorf("component instance %q: output port %d exceeds the definition's %d output(s)", e.GetSource(), outPort+1, len(srcInst.outSources))
			}
			inPort := inputPortIndex(e.GetTargetKey())
			if inPort >= len(dstInst.inTargets) {
				return nil, nil, nil, fmt.Errorf("component instance %q: input port %d exceeds the definition's %d input(s)", e.GetTarget(), inPort+1, len(dstInst.inTargets))
			}
			for _, s := range srcInst.outSources[outPort] {
				for _, target := range dstInst.inTargets[inPort] {
					outEdges = append(outEdges, nodestore.NewEdge(nodebase.GenerateID(), s.ID, target).SetSourceKey(fmt.Sprintf("output_%d", s.Port+1)).SetTargetKey("input_1"))
				}
			}
		}
	}
	return outNodes, outEdges, instances, nil
}

// edgeTouchesComponentDef reports whether e has an endpoint on a
// definition node. Definitions are templates removed by expansion, so
// such an edge would dangle in the deployed graph.
func edgeTouchesComponentDef(e nodestore.EdgeInterface, defs map[string]*componentDef) (string, bool) {
	if _, ok := defs[e.GetSource()]; ok {
		return e.GetSource(), true
	}
	if _, ok := defs[e.GetTarget()]; ok {
		return e.GetTarget(), true
	}
	return "", false
}

// expand produces the namespaced internal copy for one instance: node
// clones (internal ids prefixed "<instanceID>/", resolved env stamped
// into Data["_env"]), rewired internal edges — nested instances
// inside the copy expanded recursively — and the per-port endpoint
// lists used to graft external edges. A port wire pointing at a
// nested instance resolves through that instance's own port lists.
func (def *componentDef) expand(inst nodestore.NodeInterface, defs map[string]*componentDef, parentEnv map[string]interface{}, ancestry []string, allowOSEnv bool, budget *int) (*componentInstance, error) {
	if len(def.nodes) == 0 {
		return nil, fmt.Errorf("component instance %q: definition %q has no internal nodes", inst.GetID(), def.id)
	}
	env := resolveComponentEnv(def, inst, parentEnv, allowOSEnv)

	idMap := make(map[string]string, len(def.nodes))
	for _, inner := range def.nodes {
		idMap[inner.GetID()] = inst.GetID() + "/" + inner.GetID()
	}

	// Copy the internal graph into the instance namespace; internal
	// node refs (catch scope, link targets) follow the namespace and
	// every copy carries the resolved env overlay.
	var innerNodes []nodestore.NodeInterface
	var innerEdges []nodestore.EdgeInterface
	for _, inner := range def.nodes {
		data := cloneDataMap(inner.GetData())
		data[componentEnvKey] = env
		remapComponentIDRefs(data, idMap)
		innerNodes = append(innerNodes, nodestore.NewNode(idMap[inner.GetID()], inner.GetType()).SetPosition(inner.GetPositionX(), inner.GetPositionY()).SetData(data))
	}
	for _, e := range def.edges {
		src, okS := idMap[e.GetSource()]
		dst, okT := idMap[e.GetTarget()]
		if !okS || !okT {
			return nil, fmt.Errorf("component definition %q: internal edge %q references unknown node(s) %q -> %q", def.id, e.GetID(), e.GetSource(), e.GetTarget())
		}
		innerEdges = append(innerEdges, nodestore.NewEdge(idMap[e.GetSource()]+"->"+idMap[e.GetTarget()]+":"+e.GetID(), src, dst).SetSourceKey(e.GetSourceKey()).SetTargetKey(e.GetTargetKey()))
	}

	// Nested instances expand recursively; edges referencing them are
	// rewired through their own ports by the recursive call.
	innerNodes, innerEdges, innerInstances, err := expandComponentInstances(innerNodes, innerEdges, defs, env, ancestry, allowOSEnv, budget)
	if err != nil {
		return nil, err
	}
	// Spend the expansion budget on the produced inner graph — the
	// recursive call above already spent its own nested copies, so
	// count only this level's surviving nodes.
	*budget -= len(innerNodes)
	if *budget < 0 {
		return nil, fmt.Errorf("component expansion exceeds %d expanded nodes", maxExpandedNodes)
	}

	result := &componentInstance{nodes: innerNodes, edges: innerEdges}

	// Resolve the in/out port endpoints. A wire usually lands on an
	// ordinary internal node; when it lands on a nested instance the
	// endpoint set is that instance's own port endpoints (in wires
	// carry no port — input 0; out wires use their port index).
	live := make(map[string]bool, len(innerNodes))
	for _, n := range innerNodes {
		live[n.GetID()] = true
	}
	for _, port := range def.in {
		var targets []string
		for _, w := range port.Wires {
			nsID := idMap[w.ID]
			if live[nsID] {
				targets = append(targets, nsID)
				continue
			}
			if nested, ok := innerInstances[nsID]; ok {
				p := w.Port
				if p >= len(nested.inTargets) {
					p = 0
				}
				if p < len(nested.inTargets) {
					targets = append(targets, nested.inTargets[p]...)
				}
			}
		}
		result.inTargets = append(result.inTargets, targets)
	}
	for _, port := range def.out {
		var sources []componentWire
		for _, w := range port.Wires {
			nsID := idMap[w.ID]
			if live[nsID] {
				sources = append(sources, componentWire{ID: nsID, Port: w.Port})
				continue
			}
			if nested, ok := innerInstances[nsID]; ok && w.Port < len(nested.outSources) {
				sources = append(sources, nested.outSources[w.Port]...)
			}
		}
		result.outSources = append(result.outSources, sources)
	}
	return result, nil
}

// resolveComponentEnv computes an instance's effective env: parent scope
// (env inheritance) < definition defaults < instance overrides.
func resolveComponentEnv(def *componentDef, inst nodestore.NodeInterface, parent map[string]interface{}, allowOSEnv bool) map[string]interface{} {
	env := make(map[string]interface{}, len(parent)+len(def.env))
	for k, v := range parent {
		env[k] = v
	}
	for _, ev := range def.env {
		if ev.Name == "" {
			continue
		}
		env[ev.Name] = componentEnvValue(ev.Value, ev.Type, allowOSEnv)
	}
	for _, ev := range parseInstanceEnv(inst.GetData()["env"]) {
		if ev.Name == "" {
			continue
		}
		env[ev.Name] = componentEnvValue(ev.Value, ev.Type, allowOSEnv)
	}
	return env
}

// parseInstanceEnv reads the instance node's env overrides, accepting
// the Node-RED array shape [{name,type,value}] and the shorthand map
// {NAME: value} / {NAME: {type,value}}.
func parseInstanceEnv(raw interface{}) []componentEnvVar {
	var out []componentEnvVar
	switch env := raw.(type) {
	case []interface{}:
		for _, item := range env {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			out = append(out, componentEnvVar{
				Name:  nodebase.StringOf(m["name"]),
				Type:  nodebase.StringOf(m["type"]),
				Value: m["value"],
			})
		}
	case map[string]interface{}:
		for name, v := range env {
			if m, ok := v.(map[string]interface{}); ok {
				out = append(out, componentEnvVar{
					Name:  name,
					Type:  nodebase.StringOf(m["type"]),
					Value: m["value"],
				})
				continue
			}
			out = append(out, componentEnvVar{Name: name, Value: v})
		}
	}
	return out
}

// componentEnvValue applies the Node-RED env var typing: "num" and
// "bool" parse the string form, "json" parses JSON text, "env" reads a
// process environment variable when allowOSEnv permits it (otherwise
// resolves empty); "str"/"cred"/empty keep the raw value.
func componentEnvValue(v interface{}, typ string, allowOSEnv bool) interface{} {
	str := ""
	switch t := v.(type) {
	case string:
		str = t
	case nil:
	default:
		str = fmt.Sprint(t)
	}
	switch typ {
	case "num":
		if f, err := strconv.ParseFloat(str, 64); err == nil {
			return f
		}
	case "bool":
		if b, err := strconv.ParseBool(str); err == nil {
			return b
		}
	case "json":
		var parsed interface{}
		if err := json.Unmarshal([]byte(str), &parsed); err == nil {
			return parsed
		}
	case "env":
		if !allowOSEnv {
			return ""
		}
		return os.Getenv(str)
	case "str", "cred":
		// String-typed values are coerced so a numeric or boolean
		// config value still reaches env.get as a string.
		return str
	}
	if v == nil {
		return str
	}
	return v
}

// remapComponentIDRefs rewrites node references inside a copied node's
// Data so ids pointing at internal nodes follow the instance
// namespace: catch/status/complete "nodes" lists, linkOut "links" and
// linkCall "link".
func remapComponentIDRefs(data map[string]interface{}, idMap map[string]string) {
	for _, key := range []string{"links", "nodes"} {
		raw, ok := data[key]
		if !ok {
			continue
		}
		switch v := raw.(type) {
		case []interface{}:
			for i, item := range v {
				s, ok := item.(string)
				if !ok {
					continue
				}
				if mapped, hit := idMap[s]; hit {
					v[i] = mapped
				}
			}
		case []string:
			for i, s := range v {
				if mapped, hit := idMap[s]; hit {
					v[i] = mapped
				}
			}
		case string:
			parts := strings.Split(v, ",")
			for i, p := range parts {
				if mapped, hit := idMap[strings.TrimSpace(p)]; hit {
					parts[i] = mapped
				}
			}
			data[key] = strings.Join(parts, ",")
		}
	}
	if s, ok := data["link"].(string); ok {
		if mapped, hit := idMap[s]; hit {
			data["link"] = mapped
		}
	}
}

// inputPortIndex maps an edge TargetKey ("input_1", "", "input_N") to a
// zero-based index into the definition's in array.
func inputPortIndex(targetKey string) int {
	if targetKey == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(targetKey, "input_%d", &n); err == nil && n >= 1 {
		return n - 1
	}
	return 0
}

// outputPortIndex maps an edge SourceKey ("output_1", "", "output_N")
// to a zero-based index into the definition's out array.

// cloneDataMap deep-copies a node Data map via JSON so every instance
// owns its config (and its _env overlay) independently.

// NodeEnv resolves an env var for a node inside a running workflow
// using the Node-RED scoping chain (task env_scoping):
//
//	group env (view.groups[].env) → component overlay (node.GetData()["_env"])
//	→ flow env (view.env) → global-config (engine.globalEnv)
//
// The caller (the function node's env.get) falls back to os.Getenv
// when every layer misses. Group and flow entries use the Node-RED
// env-var shape [{name,type,value}] — the same typing rules as
// component env (num/bool/json/str/env).
func (e *WorkflowEngine) NodeEnv(workflowID, nodeID, name string) (interface{}, bool) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		return nil, false
	}

	// 1. Group env: the node's view group wins over everything else.
	if v, ok := groupEnvValue(workflow, nodeID, name, e.AllowOSEnv); ok {
		return v, true
	}

	// 2. Component env overlay stamped by ExpandComponents.
	for i := range workflow.GetNodes() {
		if workflow.GetNodes()[i].GetID() != nodeID {
			continue
		}
		if env, _ := workflow.GetNodes()[i].GetData()[componentEnvKey].(map[string]interface{}); env != nil {
			if v, ok := env[name]; ok {
				return v, true
			}
		}
		break
	}

	// 3. Flow env: workflow-level table stored in view.env.
	if workflow.GetView() != nil {
		if v, ok := envListValue(workflow.GetView()["env"], name, e.AllowOSEnv); ok {
			return v, true
		}
	}

	// 4. Global-config env (deployment-wide singleton).
	if v, ok := e.globalEnv[name]; ok {
		return v, true
	}
	return nil, false
}

// groupEnvValue finds name in the env table of the view group that
// contains nodeID. A component-expanded node id ("<instance>/<inner>")
// inherits the env of any group holding its top-level ancestor.
func groupEnvValue(workflow nodestore.WorkflowInterface, nodeID, name string, allowOSEnv bool) (interface{}, bool) {
	if workflow.GetView() == nil {
		return nil, false
	}
	groups, _ := workflow.GetView()["groups"].([]interface{})
	for _, raw := range groups {
		g, _ := raw.(map[string]interface{})
		if g == nil {
			continue
		}
		if !groupContains(g, nodeID) {
			continue
		}
		if v, ok := envListValue(g["env"], name, allowOSEnv); ok {
			return v, true
		}
	}
	return nil, false
}

// groupContains reports whether the group object lists nodeID as a
// member (or an ancestor of nodeID for component-expanded ids).
func groupContains(g map[string]interface{}, nodeID string) bool {
	for _, raw := range arrayOf(g["nodeIds"]) {
		member := nodebase.StringOf(raw)
		if member == nodeID {
			return true
		}
		// Component expansion namespaces inner ids "inst/inner" — a
		// grouped instance's env reaches its inner nodes too.
		if strings.HasPrefix(nodeID, member+"/") {
			return true
		}
	}
	return false
}

// envListValue resolves one name out of a raw [{name,type,value}]
// env list, applying the Node-RED env typing (componentEnvValue).
func envListValue(raw interface{}, name string, allowOSEnv bool) (interface{}, bool) {
	for _, ev := range parseInstanceEnv(raw) {
		if ev.Name == name {
			return componentEnvValue(ev.Value, ev.Type, allowOSEnv), true
		}
	}
	return nil, false
}

// SetGlobalEnv installs the resolved global-config env map consulted
// by env.get after group/component/flow and before the OS environment.
// raw is the env entry list ([{name,type,value}] or {NAME: value} —
// the same shapes the editor and Node-RED produce). Passing nil/empty
// clears the layer.
func (e *WorkflowEngine) SetGlobalEnv(raw interface{}) {
	resolved := map[string]interface{}{}
	for _, ev := range parseInstanceEnv(raw) {
		if ev.Name == "" {
			continue
		}
		resolved[ev.Name] = componentEnvValue(ev.Value, ev.Type, e.AllowOSEnv)
	}
	e.runMu.Lock()
	e.globalEnv = resolved
	e.runMu.Unlock()
}
