package node

import (
	"fmt"

	"github.com/baizeai/kcover/cmd/agent/config"
	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/diagnosis/node/metax"
	"github.com/baizeai/kcover/pkg/diagnosis/node/nvidia"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/runner"

	"k8s.io/klog/v2"
)

type Vendor int

const (
	_ Vendor = iota
	MetaX
	Nvidia
)

var _ runner.Runner = (*diagnostic)(nil)

type diagnostic struct {
	eventSink   events.Sink
	diagnostics []diagnosis.Diagnostic
}

func NewDiagnostic(nodeName string, vendor Vendor, interval int, metaxConfig config.MetaX, sink events.Sink) (runner.Runner, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink can not be nil")
	}

	var d diagnosis.Diagnostic
	switch vendor {
	case MetaX:
		metaxConfig.NodeName = nodeName
		d = metax.NewDiagnosis(metaxConfig, interval)
	case Nvidia:
		d = nvidia.NewDiagnosis(nodeName, interval)
	default:
		return nil, fmt.Errorf("unsupported vendor: %d", vendor)
	}

	return &diagnostic{
		eventSink:   sink,
		diagnostics: []diagnosis.Diagnostic{d},
	}, nil
}

func (d *diagnostic) Start() error {
	for _, v := range d.diagnostics {
		if err := v.Start(); err != nil {
			return err
		}
		klog.Infof("diag: %v is started", v)
	}

	for _, v := range d.diagnostics {
		go func(diag diagnosis.Diagnostic) {
			for evt := range diag.EventChan() {
				if err := d.eventSink.RecordEvent(evt); err != nil {
					klog.Errorf("failed to record event of %T: %v", diag, err)
				}
			}
		}(v)
	}

	return nil
}

func (d *diagnostic) Stop() {
	for _, diag := range d.diagnostics {
		diag.Stop()
	}
}
