package preflight

import "testing"

func TestParseReportUsesLatestFields(t *testing.T) {
	t.Parallel()

	report, err := parseReport(`{"version":1,"workload":"resnet50","workload_size":4,"rank":0,"node_name":"node-a","gpu_check":2,"storage_check":1}`)
	if err != nil {
		t.Fatalf("ParseReport returned error: %v", err)
	}

	if report.Workload != "resnet50" {
		t.Fatalf("report.Workload = %q, want %q", report.Workload, "resnet50")
	}
	if report.GPUCheck != CheckResultFail {
		t.Fatalf("report.GPUCheck = %v, want %v", report.GPUCheck, CheckResultFail)
	}
	if report.StorageCheck != CheckResultPass {
		t.Fatalf("report.StorageCheck = %v, want %v", report.StorageCheck, CheckResultPass)
	}
}

func TestParseReportRejectsMissingNodeName(t *testing.T) {
	t.Parallel()

	if _, err := parseReport(`{"version":1,"gpu_check":2,"storage_check":1}`); err == nil {
		t.Fatal("ParseReport error = nil, want non-nil when node_name is missing")
	}
}

func TestParseReportTreatsOmittedChecksAsSkip(t *testing.T) {
	t.Parallel()

	report, err := parseReport(`{"version":1,"workload":"resnet50","workload_size":4,"rank":0,"node_name":"node-a","result":2,"check":{"storage":1,"gpu":1,"node_check":0}}`)
	if err != nil {
		t.Fatalf("ParseReport returned error: %v", err)
	}
	if report.GPUCheck != CheckResultSkip {
		t.Fatalf("report.GPUCheck = %v, want %v when gpu_check is omitted", report.GPUCheck, CheckResultSkip)
	}
	if report.StorageCheck != CheckResultSkip {
		t.Fatalf("report.StorageCheck = %v, want %v when storage_check is omitted", report.StorageCheck, CheckResultSkip)
	}
}
