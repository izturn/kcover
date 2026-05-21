package preflight

import (
	"encoding/json"
	"errors"
	"fmt"
)

func parseReport(text string) (Report, error) {
	report := Report{}
	if err := json.Unmarshal([]byte(text), &report); err != nil {
		return Report{}, fmt.Errorf("failed to unmarshal preflight report: %w", err)
	}

	if report.NodeName == "" {
		return Report{}, errors.New("preflight report node name is empty")
	}

	return report, nil
}
