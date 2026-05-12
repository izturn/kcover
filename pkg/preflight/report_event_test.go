package preflight

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
)

func TestLoadReportFile(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "default")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	reportPath := filepath.Join(path, "worker-0.json")
	if err := os.WriteFile(reportPath, []byte(`{"version":1,"result":2,"node_name":"node-a","check":{"gpu":1,"nic":1,"storage":1,"node_check":1,"network":{"result":2,"target":{"node-b":2}}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	report, err := LoadReportFile(baseDir, "default", "worker-0")
	if err != nil {
		t.Fatalf("LoadReportFile() error = %v", err)
	}

	if report.NodeName != "node-a" {
		t.Fatalf("report.NodeName = %q, want %q", report.NodeName, "node-a")
	}
	if report.Checks.Network.Target["node-b"] != CheckResultFail {
		t.Fatalf("report.Checks.Network.Target[node-b] = %v, want %v", report.Checks.Network.Target["node-b"], CheckResultFail)
	}
}

func TestReportPath(t *testing.T) {
	t.Parallel()

	path := ReportPath("/var/lib/kcover/preflight", "default", "worker-0")
	if path != "/var/lib/kcover/preflight/default/worker-0.json" {
		t.Fatalf("ReportPath(...) = %q, want %q", path, "/var/lib/kcover/preflight/default/worker-0.json")
	}
}

func TestReportDeliveryEvent(t *testing.T) {
	t.Parallel()

	event := ReportDeliveryEvent("default", "node-a", "job-a", `{"version":1}`)
	if event.ResourceType != events.Node {
		t.Fatalf("event.ResourceType = %s, want %s", event.ResourceType, events.Node)
	}
	if event.Name != "node-a" {
		t.Fatalf("event.Name = %q, want %q", event.Name, "node-a")
	}
	if event.Annotations[constants.PreflightReportAnnotation] != constants.True {
		t.Fatalf("preflight annotation = %q, want %q", event.Annotations[constants.PreflightReportAnnotation], constants.True)
	}
	if event.Annotations[constants.KubeflowJobLabel] != "job-a" {
		t.Fatalf("job annotation = %q, want %q", event.Annotations[constants.KubeflowJobLabel], "job-a")
	}
}
