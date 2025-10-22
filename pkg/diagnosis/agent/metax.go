//go:build !nvidia

package agent

import (
	"time"

	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/runner"

	"k8s.io/klog/v2"
)

var _ runner.Runner = (*metaxDiag)(nil)
var _ diagnosis.Diagnostic = (*metaxDiag)(nil)

type metaxDiag struct {
	nodeName string
	events   chan events.CollectorEvent
	stop     chan struct{}
}

func NewDiagnosis(nodeName string) (diagnosis.Diagnostic, error) {
	klog.Info("for vendor: metax")
	return &metaxDiag{
		events:   make(chan events.CollectorEvent),
		stop:     make(chan struct{}),
		nodeName: nodeName,
	}, nil
}

func (d *metaxDiag) Start() error {
	go func() {
		t := time.NewTicker(time.Second * 30)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				// run dcgmi
				// parse results
				klog.Infof("start dcgmi diag -r 1")
				//d.events <- events.CollectorEvent{
				//	TargetType: events.Node,
				//	Name:       "worker-a800-2",
				//	EventType:  events.Error,
				//	Message:    "test event for worker-a800-2",
				//}
			case <-d.stop:
				return
			}
		}
	}()
	return nil
}

func (d *metaxDiag) Stop() {
	close(d.stop)
}

func (d *metaxDiag) EventChan() <-chan events.CollectorEvent {
	return d.events
}
