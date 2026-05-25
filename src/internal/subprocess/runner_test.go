package subprocess

import (
	"context"
	"testing"
)

func TestRunCapturesStdout(t *testing.T) {
	result, err := Run(context.Background(), "", "sh", "-c", "printf hello")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Stdout != "hello" {
		t.Fatalf("Stdout = %q, want hello", result.Stdout)
	}
}
