package preflight

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type reportWire struct {
	Version         int         `json:"version"`
	WorkloadSize    any         `json:"workload_size,omitempty"`
	LegacyWorldSize any         `json:"world_size,omitempty"`
	Rank            any         `json:"rank,omitempty"`
	Result          CheckResult `json:"result"`
	Checks          Check       `json:"check"`
	NodeName        string      `json:"node_name,omitempty"`
}

func parseReport(text string) (*Report, error) {
	wire := &reportWire{}
	if err := json.Unmarshal([]byte(text), wire); err != nil {
		return nil, fmt.Errorf("unmarshal preflight report is failed: %w", err)
	}

	workloadSizeSource := wire.WorkloadSize
	workloadSizeFieldName := "workload_size"
	if workloadSizeSource == nil {
		workloadSizeSource = wire.LegacyWorldSize
		workloadSizeFieldName = "world_size"
	}
	workloadSize, err := parseIntField(workloadSizeSource)
	if err != nil && workloadSizeSource != nil {
		return nil, fmt.Errorf("invalid %s: %w", workloadSizeFieldName, err)
	}
	rank, err := parseIntField(wire.Rank)
	if err != nil && wire.Rank != nil {
		return nil, fmt.Errorf("invalid rank: %w", err)
	}

	report := &Report{
		Version:      wire.Version,
		WorkloadSize: workloadSize,
		Rank:         rank,
		Result:       wire.Result,
		Checks:       wire.Checks,
		NodeName:     wire.NodeName,
	}

	if report.NodeName == "" {
		return nil, errors.New("report node name is empty")
	}

	return report, nil
}

func parseIntField(value any) (int, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case string:
		if v == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported type %T", value)
	}
}

// parseReportText 解析单份 preflight JSON 报告。
func parseReportText(text string) (Report, error) {
	report, err := parseReport(text)
	if err != nil {
		return Report{}, err
	}

	return *report, nil
}
