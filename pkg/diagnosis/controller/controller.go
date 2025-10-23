package controller

import (
	"fmt"

	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/runner"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var _ runner.Runner = (*controllerDiagnostic)(nil)

type controllerDiagnostic struct {
	diagnostics []diagnosis.Diagnostic
	writer      events.Writer
}

// NewDiagnostic 创建由controller(kcover)所管理的诊断,目前只支持pod诊断
func NewDiagnostic(cli kubernetes.Interface, w events.Writer) (runner.Runner, error) {
	if w == nil {
		return nil, fmt.Errorf("recorder can not be nil")
	}

	diags := make([]diagnosis.Diagnostic, 0)

	diagPodCollector, err := newPodStatusCollector(cli)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod status collector: %v", err)
	}

	diags = append(diags, diagPodCollector) // 目前只支持针对pod的诊断

	return &controllerDiagnostic{
		diagnostics: diags,
		writer:      w,
	}, nil
}
func (c *controllerDiagnostic) Start() error {
	for _, d := range c.diagnostics {
		if err := d.Start(); err != nil {
			return err
		}
	}
	for _, d := range c.diagnostics {
		go func() {
			for e := range d.EventChan() {
				err := c.writer.RecordEvent(e)
				if err != nil {
					klog.Errorf("failed to record event of %T: %v", d, err)
				}
			}
		}()
	}

	klog.Info("the controllerDiagnostic is started")
	return nil
}

func (c *controllerDiagnostic) Stop() {
	for _, d := range c.diagnostics {
		d.Stop()
	}
}
