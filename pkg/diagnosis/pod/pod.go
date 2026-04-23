package pod

import (
	"fmt"

	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/runner"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var _ runner.Runner = (*diagnostic)(nil)

type diagnostic struct {
	diagnostics []diagnosis.Diagnostic
	eventSink   events.Sink
}

// NewDiagnostic 创建由 pod 诊断管理的诊断器，目前只支持 Pod 诊断。
func NewDiagnostic(cli kubernetes.Interface, sink events.Sink) (runner.Runner, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink can not be nil")
	}

	diags := make([]diagnosis.Diagnostic, 0)

	diagPodCollector, err := newPodStatusCollector(cli)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod status collector: %v", err)
	}

	diags = append(diags, diagPodCollector) // 目前只支持针对pod的诊断

	return &diagnostic{
		diagnostics: diags,
		eventSink:   sink,
	}, nil
}

func (c *diagnostic) Start() error {
	for _, d := range c.diagnostics {
		if err := d.Start(); err != nil {
			return err
		}
	}
	for _, d := range c.diagnostics {
		go func() {
			for e := range d.EventChan() {
				err := c.eventSink.RecordEvent(e)
				if err != nil {
					klog.Errorf("failed to record event of %T: %v", d, err)
				}
			}
		}()
	}

	klog.Info("the pod diagnostic is started")
	return nil
}

func (c *diagnostic) Stop() {
	for _, d := range c.diagnostics {
		d.Stop()
	}
}
