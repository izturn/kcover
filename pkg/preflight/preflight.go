package preflight

import (
	"encoding/json"
	"errors"
	"fmt"
)

func parseReport(text string) (Report, error) {
	report := Report{}
	if err := json.Unmarshal([]byte(text), &report); err != nil {
		return Report{}, fmt.Errorf("unmarshal preflight report is failed: %w", err)
	}

	if report.NodeName == "" {
		return Report{}, errors.New("report node name is empty")
	}

	return report, nil
}
