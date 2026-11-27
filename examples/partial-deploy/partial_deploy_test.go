package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestRunPartialDeployKeepsListener(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = original }()

	runErr := run()
	_ = writer.Close()
	os.Stdout = original

	output, _ := io.ReadAll(reader)
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}
	for _, want := range []string{"Hello v1!", "Hello v2!"} {
		if !bytes.Contains(output, []byte(want)) {
			t.Fatalf("expected %s in output, got: %s", want, output)
		}
	}
}
