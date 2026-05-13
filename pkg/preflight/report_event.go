package preflight

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
)

func ReportPath(baseDir, namespace, podName string) string {
	return filepath.Join(baseDir, namespace, podName+".json")
}

func LoadReportFile(baseDir, namespace, podName string) (Report, error) {
	report, _, err := LoadReportPayload(baseDir, namespace, podName)
	if err != nil {
		return Report{}, err
	}

	return report, nil
}

func LoadReportPayload(baseDir, namespace, podName string) (Report, string, error) {
	path := ReportPath(baseDir, namespace, podName)
	data, err := os.ReadFile(path)
	if err != nil {
		return Report{}, "", fmt.Errorf("read %s: %w", path, err)
	}

	payload, err := CompactReportPayload(string(data))
	if err != nil {
		return Report{}, "", fmt.Errorf("parse %s: %w", path, err)
	}

	report, err := parseReport(payload)
	if err != nil {
		return Report{}, "", fmt.Errorf("parse compacted %s: %w", path, err)
	}

	return *report, payload, nil
}

func ReportDeliveryEvent(namespace, nodeName, jobName, reportText string) events.Event {
	annotations := map[string]string{
		constants.PreflightReportAnnotation: constants.True,
	}
	if jobName != "" {
		annotations[constants.KubeflowJobLabel] = jobName
	}

	return events.Event{
		ResourceType: events.Node,
		Namespace:    namespace,
		Name:         nodeName,
		Annotations:  annotations,
		EventType:    events.Error,
		Message:      reportText,
	}
}

func CompactReportPayload(reportText string) (string, error) {
	report, layout, observations, err := extractBatchObservations(reportText, DefaultConfig())
	if err != nil {
		return "", err
	}

	type compactBatch struct {
		BatchIdx int       `json:"batch_idx"`
		Pair     [2]string `json:"pair"`
		SelfIP   string    `json:"self_ip,omitempty"`
		Status   string    `json:"status"`
	}
	type compactReport struct {
		Version   int            `json:"version"`
		Workload  string         `json:"workload,omitempty"`
		WorldSize int            `json:"world_size,omitempty"`
		Rank      int            `json:"rank,omitempty"`
		Result    CheckResult    `json:"result"`
		NodeName  string         `json:"node_name"`
		Check     Check          `json:"check"`
		Batches   []compactBatch `json:"batches"`
	}

	batches := make([]compactBatch, 0, len(observations))
	for _, observation := range observations {
		status := "pass"
		if observation.Failed {
			status = "fail"
		}
		batches = append(batches, compactBatch{
			BatchIdx: observation.BatchIdx,
			Pair:     [2]string{observation.PairA, observation.PairB},
			SelfIP:   observation.SelfID,
			Status:   status,
		})
	}

	compact := compactReport{
		Version:   report.Version,
		Workload:  strings.TrimSpace(report.Workload),
		WorldSize: layout.reports,
		Rank:      report.Rank,
		Result:    report.Result,
		NodeName:  report.NodeName,
		Check:     report.Checks,
		Batches:   batches,
	}

	encoded, err := json.Marshal(compact)
	if err != nil {
		return "", fmt.Errorf("marshal compact preflight report: %w", err)
	}

	return string(encoded), nil
}
