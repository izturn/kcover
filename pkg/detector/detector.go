package detector

import (
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/runner"
)

type Detector interface {
	runner.Runner
	events.Stream
}
