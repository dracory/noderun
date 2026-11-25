package noderunner

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Local copies of small helpers that are unexported in package nodestore.

func arrayOf(v any) []any {
	arr, _ := v.([]any)
	return arr
}

func truthyFlag(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1", "yes":
			return true
		}
	}
	return false
}

func flowLabel(w nodestore.WorkflowInterface, ref string) string {
	if w == nil || w.GetView() == nil || ref == "" {
		return ref
	}
	for _, m := range arrayOf(w.GetView()["childFlows"]) {
		mm, ok := m.(map[string]any)
		if !ok {
			continue
		}
		if nodebase.StringOf(mm["id"]) == ref {
			if label := nodebase.StringOf(mm["label"]); label != "" {
				return label
			}
		}
	}
	return ref
}

func flowRefMatches(w nodestore.WorkflowInterface, a, b any) bool {
	ra, rb := nodebase.StringOf(a), nodebase.StringOf(b)
	if ra == "" || rb == "" {
		return false
	}
	return ra == rb || flowLabel(w, ra) == flowLabel(w, rb)
}

func cloneDataMap(data map[string]any) map[string]any {
	out := map[string]any{}
	if len(data) == 0 {
		return out
	}
	if raw, err := json.Marshal(data); err == nil {
		if json.Unmarshal(raw, &out) == nil {
			return out
		}
	}
	for k, v := range data {
		out[k] = v
	}
	return out
}

func edgeFromJSON(j edgeJSON) nodestore.EdgeInterface {
	return nodestore.NewEdge(j.ID, j.Source, j.Target).SetSourceKey(j.SourceKey).SetTargetKey(j.TargetKey)
}

type edgeJSON struct {
	ID        string `json:"id"`
	Source    string `json:"source"`
	SourceKey string `json:"sourceKey,omitempty"`
	Target    string `json:"target"`
	TargetKey string `json:"targetKey,omitempty"`
}

func outputPortIndex(sourceKey string) int {
	if sourceKey == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(sourceKey, "output_%d", &n); err == nil && n >= 1 {
		return n - 1
	}
	return 0
}

func parseComponentInnerNode(raw json.RawMessage) (nodestore.NodeInterface, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	node := nodestore.NewNode(nodebase.StringOf(m["id"]), nodebase.StringOf(m["type"]))
	if node.GetID() == "" {
		return nil, fmt.Errorf("internal node without id")
	}
	if pos, ok := m["position"].(map[string]any); ok {
		node.SetPosition(floatOf(pos["x"]), floatOf(pos["y"]))
	} else {
		node.SetPosition(floatOf(m["x"]), floatOf(m["y"]))
	}
	if data, ok := m["data"].(map[string]any); ok {
		node.SetData(data)
	} else {
		// Node-RED flat shape: every key other than the structural
		// ones is node configuration.
		data := map[string]any{}
		for k, v := range m {
			switch k {
			case "id", "type", "x", "y", "z", "wires", "position", "data":
				continue
			}
			data[k] = v
		}
		node.SetData(data)
	}
	return node, nil
}

const componentDefNodeType = "component"

const componentEnvKey = "_env"

const componentTypePrefix = "component:"

func floatOf(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}
