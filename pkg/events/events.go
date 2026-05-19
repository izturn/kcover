package events

import (
	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/runner"
)

type ResourceType string

const (
	Pod  ResourceType = "pod"
	Node ResourceType = "node"
)

type EventType int

const (
	_ EventType = iota
	Error
	Warning
)

type Event struct {
	ResourceType
	Namespace   string
	Name        string
	Annotations map[string]string

	EventType
	Message string
}

type Bridge interface {
	runner.Runner

	Sink
	Stream
}

type Sink interface {
	RecordEvent(e Event) error
}

type Stream interface {
	EventChan() <-chan Event
}

func IsPreflightEvent(annotations map[string]string) bool {
	return annotations[constants.PreflightWorkloadAnnotation] != ""
}

func copyAnnotations(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}

	return dst
}
