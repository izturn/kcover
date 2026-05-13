package preflight

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// parseReport 解析单份 preflight JSON 报告。
func parseReport(text string) (*Report, error) {
	type reportWire struct {
		Version   int         `json:"version"`
		Workload  string      `json:"workload,omitempty"`
		WorldSize any         `json:"world_size,omitempty"`
		Rank      any         `json:"rank,omitempty"`
		Result    CheckResult `json:"result"`
		Checks    Check       `json:"check"`
		NodeName  string      `json:"node_name,omitempty"`
	}

	wire := &reportWire{}
	if err := json.Unmarshal([]byte(text), wire); err != nil {
		return nil, fmt.Errorf("unmarshal preflight report is failed: %w", err)
	}

	worldSize, err := parseIntField(wire.WorldSize)
	if err != nil && wire.WorldSize != nil {
		return nil, fmt.Errorf("invalid world_size: %w", err)
	}
	rank, err := parseIntField(wire.Rank)
	if err != nil && wire.Rank != nil {
		return nil, fmt.Errorf("invalid rank: %w", err)
	}

	report := &Report{
		Version:   wire.Version,
		Workload:  wire.Workload,
		WorldSize: worldSize,
		Rank:      rank,
		Result:    wire.Result,
		Checks:    wire.Checks,
		NodeName:  wire.NodeName,
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

// ParseReportText 解析单份 preflight JSON 报告。
func ParseReportText(text string) (Report, error) {
	report, err := parseReport(text)
	if err != nil {
		return Report{}, err
	}

	return *report, nil
}
