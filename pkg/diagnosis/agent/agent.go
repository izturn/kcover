package agent

import (
	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/diagnosis/agent/metax"
	"github.com/baizeai/kcover/pkg/diagnosis/agent/nvidia"
	"github.com/baizeai/kcover/pkg/events"

	"k8s.io/klog/v2"
)

type Vendor int

const (
	_ Vendor = iota
	MetaX
	Nvidia
)

type diag struct {
	w           events.Writer
	diagnostics []diagnosis.Diagnostic
}

func MustNewDiagnosis(nodeName string, vendor Vendor, w events.Writer) *diag {
	var d diagnosis.Diagnostic
	switch vendor {
	case MetaX:
		d = metax.NewDiagnosis(nodeName)
	case Nvidia:
		d = nvidia.NewDiagnosis(nodeName)
	default:
		panic("never happen")
	}

	return &diag{
		w:           w,
		diagnostics: []diagnosis.Diagnostic{d},
	}
}

func (d *diag) Start() {
	for _, v := range d.diagnostics {
		if err := v.Start(); err != nil {
			panic(err)
		}
		klog.Infof("diag: %v is started", v)
	}

	for _, v := range d.diagnostics {
		go func() {
			for evt := range v.EventChan() {
				_ = d.w.RecordEvent(evt)
			}
		}()
	}
}

func (d *diag) Stop() {
	for _, d := range d.diagnostics {
		d.Stop()
	}
}
