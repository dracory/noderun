package noderunner

import (
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Child flow identity helpers (ported from the legacy runner).

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
