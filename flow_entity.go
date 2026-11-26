package noderunner

import (
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Child flow identity helpers (ported from the legacy runner).
// flowLabel and flowRefMatches live in the child flow exchange layer.

// flowEntityDisabled reports whether the view.childFlows entity matching
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
