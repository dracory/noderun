package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestRunPrintsHelloWorld(t *testing.T) {
	output := captureStdout(t, func() {
		if err := run(); err != nil {
			t.Errorf("run failed: %v", err)
		}
	})

	if !strings.Contains(output, "Hello world!") {
		t.Fatalf("expected output to contain %q, got %q", "Hello world!", output)
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
