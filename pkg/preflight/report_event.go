package preflight

import (
	"fmt"
	"os"
	"path/filepath"

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

	report, err := parseReport(string(data))
	if err != nil {
		return Report{}, "", fmt.Errorf("parse %s: %w", path, err)
	}

	return *report, string(data), nil
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
