package nvidia

import (
	"time"

	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"

	"k8s.io/klog/v2"
)

var _ diagnosis.Diagnostic = (*diag)(nil)

type diag struct {
	nodeName string
	events   chan events.Event
	stop     chan struct{}
	interval int
}

func NewDiagnosis(nodeName string, interval int) *diag {
	klog.Info("for vendor: nvidia")
	return &diag{
		events:   make(chan events.Event),
		stop:     make(chan struct{}),
		nodeName: nodeName,
		interval: interval,
	}
}

func (d *diag) Start() error {
	go func() {
		t := time.NewTicker(time.Second * time.Duration(d.interval))
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

func (d *diag) Stop() {
	close(d.stop)
	close(d.events)
}

func (d *diag) EventChan() <-chan events.Event {
	return d.events

}

func (d *diag) String() string {
	return "Nvidia"
}
