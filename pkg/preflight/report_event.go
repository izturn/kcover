package preflight

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/baizeai/kcover/pkg/events"
)

func ReportPath(baseDir, namespace, podName string) string {
	return filepath.Join(baseDir, namespace, podName+".json")
}

func LoadReportFile(baseDir, namespace, podName string) (Report, error) {
	path := ReportPath(baseDir, namespace, podName)
	data, err := os.ReadFile(path)
	if err != nil {
		return Report{}, fmt.Errorf("read %s: %w", path, err)
	}

	report, err := parseReport(string(data))
	if err != nil {
		return Report{}, fmt.Errorf("parse %s: %w", path, err)
	}

	return *report, nil
}

// TODO: 将此函数构建成Report的方法
func NodeEvents(namespace, podName string, report Report) []events.Event {
	nodeEvents := make([]events.Event, 0)
	seen := make(map[string]struct{})

	if report.NodeName != "" {
		nodeEvents = append(nodeEvents, events.Event{
			ResourceType: events.Node,
			Name:         report.NodeName,
			EventType:    events.Error,
			Message:      fmt.Sprintf("pod %s/%s preflight failed on node %s", namespace, podName, report.NodeName),
		})
		seen[report.NodeName] = struct{}{}
	}

	targets := make([]string, 0, len(report.Checks.Network.Target))
	for nodeName, result := range report.Checks.Network.Target {
		if nodeName == "" || result != CheckResultFail {
			continue
		}
		if _, exists := seen[nodeName]; exists {
			continue
		}
		targets = append(targets, nodeName)
	}
	sort.Strings(targets)

	for _, nodeName := range targets {
		nodeEvents = append(nodeEvents, events.Event{
			ResourceType: events.Node,
			Name:         nodeName,
			EventType:    events.Error,
			Message:      fmt.Sprintf("pod %s/%s preflight reported network failure to node %s", namespace, podName, nodeName),
		})
	}

	return nodeEvents
}
