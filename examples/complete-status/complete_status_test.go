package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestRunPrintsMainStatusAndCompleteLines(t *testing.T) {
	output := captureStdout(t, func() {
		if err := run(); err != nil {
			t.Errorf("run failed: %v", err)
		}
	})

	for _, want := range []string{
		"doing work",
		`"status":"running"`,
		`"status":"ok"`,
		"main branch finished",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, output)
		}
	}

	// The complete observer is scoped to the inject trigger: the status
	// observers' own traversal completions must NOT re-fire it.
	if count := strings.Count(output, "main branch finished"); count != 1 {
		t.Fatalf("expected %q exactly once (scope-selected complete), got %d times in %q", "main branch finished", count, output)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = writer

	outputCh := make(chan string, 1)
	go func() {
		output, _ := io.ReadAll(reader)
		outputCh <- string(output)
	}()

	defer func() {
		os.Stdout = original
		_ = reader.Close()
	}()

	fn()

	_ = writer.Close()
	os.Stdout = original
	return <-outputCh
}
