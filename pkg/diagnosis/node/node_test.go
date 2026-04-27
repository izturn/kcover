package node

import (
	"testing"

	kcoverconfig "github.com/baizeai/kcover/cmd/agent/config"
	"github.com/baizeai/kcover/pkg/events"
)

type stubSink struct{}

func (stubSink) RecordEvent(events.Event) error {
	return nil
}

func TestNewDiagnosticRejectsNilSink(t *testing.T) {
	t.Parallel()

	if _, err := NewDiagnostic("node-a", MetaX, 5, kcoverconfig.MetaX{}, nil); err == nil {
		t.Fatal("NewDiagnostic error = nil, want non-nil for nil sink")
	}
}

func TestNewDiagnosticRejectsUnknownVendor(t *testing.T) {
	t.Parallel()

	if _, err := NewDiagnostic("node-a", Vendor(99), 5, kcoverconfig.MetaX{}, stubSink{}); err == nil {
		t.Fatal("NewDiagnostic error = nil, want non-nil for unknown vendor")
	}
}

func TestNewDiagnosticReturnsRunner(t *testing.T) {
	t.Parallel()

	diag, err := NewDiagnostic("node-a", Nvidia, 5, kcoverconfig.MetaX{}, stubSink{})
	if err != nil {
		t.Fatalf("NewDiagnostic returned error: %v", err)
	}
	if diag == nil {
		t.Fatal("NewDiagnostic result = nil, want non-nil")
	}
}
