package nvidia

import (
	"time"

	d "github.com/baizeai/kcover/pkg/detector"
	"github.com/baizeai/kcover/pkg/events"

	"k8s.io/klog/v2"
)

var _ d.Detector = (*detector)(nil)

type detector struct {
	nodeName string
	events   chan events.Event
	stop     chan struct{}
	interval int
}

func NewDetector(nodeName string, interval int) *detector {
	klog.Info("for vendor: nvidia")
	return &detector{
		events:   make(chan events.Event),
		stop:     make(chan struct{}),
		nodeName: nodeName,
		interval: interval,
	}
}

func (d *detector) Start() error {
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

func (d *detector) Stop() {
	close(d.stop)
	close(d.events)
}

func (d *detector) EventChan() <-chan events.Event {
	return d.events

}

func (d *detector) String() string {
	return "Nvidia"
}
