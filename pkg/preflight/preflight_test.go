package preflight

import "testing"

func TestParseReportSupportsTargets(t *testing.T) {
	t.Parallel()

	report, err := parseReport(`{"version":1,"workload":"resnet50","world_size":4,"rank":0,"result":2,"node_name":"node-a","check":{"storage":1,"gpu":1,"node_check":0,"nic":1,"network":{"result":2,"target":{"node-b":2,"node-c":1}}}}`)
	if err != nil {
		t.Fatalf("ParseReport returned error: %v", err)
	}

	if report.Result != CheckResultFail {
		t.Fatalf("report.Result = %v, want %v", report.Result, CheckResultFail)
	}
	if report.Checks.NodeCheck != CheckResultSkip {
		t.Fatalf("report.Checks.NodeCheck = %v, want %v", report.Checks.NodeCheck, CheckResultSkip)
	}
	if report.Checks.Storage != CheckResultPass {
		t.Fatalf("report.Checks.Storage = %v, want %v", report.Checks.Storage, CheckResultPass)
	}
}

func TestParseReportRejectsMissingNodeName(t *testing.T) {
	t.Parallel()

	if _, err := parseReport(`{"version":1,"result":2,"check":{"network":{"result":2,"target":{"node-b":2}}}}`); err == nil {
		t.Fatal("ParseReport error = nil, want non-nil when node_name is missing")
	}
}
