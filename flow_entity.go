package noderunner

import (
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

// Module identity helpers (ported from flowrunner module.go).
// flowLabel and flowRefMatches live in module_exchange.go.

// flowEntityDisabled reports whether the view.modules entity matching
// ref (by id or label) is flagged enabled:false.
func flowEntityDisabled(w nodestore.WorkflowInterface, ref string) bool {
	if w == nil || w.GetView() == nil || ref == "" {
		return false
	}
	for _, m := range arrayOf(w.GetView()["childFlows"]) {
		mm, ok := m.(map[string]interface{})
		if !ok {
			continue
		}
		if enabled, isBool := mm["enabled"].(bool); isBool && !enabled {
			if nodebase.StringOf(mm["id"]) == ref || nodebase.StringOf(mm["label"]) == ref {
				return true
			}
		}
	}
	return false
}
