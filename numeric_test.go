package noderunner_test

import (
	"math"
	"github.com/dracory/nodebase"
	"testing"
)

// TestParseNumericNaN — BUG-8: "NaN" must not coerce to a number
// (the doc contract already promised ok == false for NaN).
func TestParseNumericNaN(t *testing.T) {
	if _, ok := nodebase.ParseNumeric("NaN"); ok {
		t.Fatal("expected nodebase.ParseNumeric(\"NaN\") to fail")
	}
	if _, ok := nodebase.ParseNumeric(math.NaN()); ok {
		t.Fatal("expected nodebase.ParseNumeric(NaN) to fail")
	}
	if f, ok := nodebase.ParseNumeric("Infinity"); !ok || !math.IsInf(f, 1) {
		t.Fatal("expected Infinity to parse (JS Number() parity)")
	}
	if _, ok := nodebase.ParseNumeric("42.5"); !ok {
		t.Fatal("expected 42.5 to parse")
	}
}
