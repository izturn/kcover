package preflight

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
)

func ReportPath(baseDir, namespace, reportName string) string {
	return filepath.Join(baseDir, namespace, reportName+".json")
}

func LoadReportPayload(baseDir, namespace, reportName string) (string, string, error) {
	path := ReportPath(baseDir, namespace, reportName)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("read %s: %w", path, err)
	}

	payload, nodeName, err := compactReport(string(data))
	if err != nil {
		return "", "", fmt.Errorf("parse %s: %w", path, err)
	}

	if nodeName == "" {
		return "", "", fmt.Errorf("parse compacted %s: report node name is empty", path)
	}

	return payload, nodeName, nil
}

func ReportToEvent(namespace, nodeName, workloadName, reportText string) (events.Event, error) {
	if workloadName == "" {
		return events.Event{}, fmt.Errorf("preflight workload name is empty")
	}

	annotations := map[string]string{
		constants.PreflightWorkloadAnnotation: workloadName,
	}

	return events.Event{
		ResourceType: events.Node,
		Namespace:    namespace,
		Name:         nodeName,
		Annotations:  annotations,
		Message:      reportText,
	}, nil
}

func compactReport(reportText string) (string, string, error) {
	report, err := parseReportText(reportText)
	if err != nil {
		return "", "", err
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(reportText), &raw); err != nil {
		return "", "", fmt.Errorf("unmarshal preflight payload: %w", err)
	}

	compact := map[string]any{
		"workload_size": report.WorkloadSize,
		"rank":          report.Rank,
		"node_name":     report.NodeName,
		"result":        report.Result,
		"check":         report.Checks,
	}
	if threshold, ok := raw["node_check_busbw_threshold_gbps"]; ok {
		compact["node_check_busbw_threshold_gbps"] = threshold
	}

	batchRaw, _ := raw["batches"].([]any)
	batches := make([]map[string]any, 0, len(batchRaw))
	for _, item := range batchRaw {
		batch, ok := item.(map[string]any)
		if !ok {
			return "", "", fmt.Errorf("invalid batch payload type %T", item)
		}

		compactedBatch := make(map[string]any, 8)
		copyIfPresent(compactedBatch, batch, "batch_idx")
		copyIfPresent(compactedBatch, batch, "pair")
		copyIfPresent(compactedBatch, batch, "self_ip")
		copyIfPresent(compactedBatch, batch, "local_rank")
		copyIfPresent(compactedBatch, batch, "status")
		copyIfPresent(compactedBatch, batch, "allreduce_ms")
		copyIfPresent(compactedBatch, batch, "world_size")
		copyIfPresent(compactedBatch, batch, "allreduce_shape")
		copyIfPresent(compactedBatch, batch, "dtype_bytes")
		batches = append(batches, compactedBatch)
	}
	compact["batches"] = batches

	encoded, err := json.Marshal(compact)
	if err != nil {
		return "", "", fmt.Errorf("marshal compact preflight report: %w", err)
	}

	return string(encoded), report.NodeName, nil
}

func copyIfPresent(dst, src map[string]any, key string) {
	value, ok := src[key]
	if !ok {
		return
	}

	dst[key] = value
}
