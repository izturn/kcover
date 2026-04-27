package nvidia

import (
	"time"

	"github.com/baizeai/kcover/pkg/detector"
	"github.com/baizeai/kcover/pkg/events"

	"k8s.io/klog/v2"
)

var _ detector.Detector = (*detectorImpl)(nil)

type detectorImpl struct {
	nodeName string
	events   chan events.Event
	stop     chan struct{}
	interval int
}

func NewDetector(nodeName string, interval int) *detectorImpl {
	klog.Info("for vendor: nvidia")
	return &detectorImpl{
		events:   make(chan events.Event),
		stop:     make(chan struct{}),
		nodeName: nodeName,
		interval: interval,
	}
}

func (d *detectorImpl) Start() error {
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

func (d *detectorImpl) Stop() {
	close(d.stop)
	close(d.events)
}

func (d *detectorImpl) EventChan() <-chan events.Event {
	return d.events

}

func (d *detectorImpl) String() string {
	return "Nvidia"
}
