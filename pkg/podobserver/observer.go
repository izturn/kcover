package podobserver

import (
	"fmt"
	"time"

	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/runner"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type PodRule interface {
	OnAdd(pod *corev1.Pod) []events.Event
	OnUpdate(oldPod, newPod *corev1.Pod) []events.Event
}

type observer struct {
	client  kubernetes.Interface
	sink    events.Sink
	rules   []PodRule
	stopCh  chan struct{}
	logName string
}

var _ runner.Runner = (*observer)(nil)

func New(cli kubernetes.Interface, sink events.Sink, logName string, rules ...PodRule) (runner.Runner, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink can not be nil")
	}

	return &observer{
		client:  cli,
		sink:    sink,
		rules:   rules,
		stopCh:  make(chan struct{}),
		logName: logName,
	}, nil
}

func (o *observer) Start() error {
	factory := informers.NewSharedInformerFactory(o.client, time.Minute)
	informer := factory.Core().V1().Pods().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			o.onAdd(pod)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPod, ok := oldObj.(*corev1.Pod)
			if !ok {
				return
			}
			newPod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			if newPod.ResourceVersion == oldPod.ResourceVersion {
				return
			}
			o.onUpdate(oldPod, newPod)
		},
	})
	if err != nil {
		return err
	}

	go informer.Run(o.stopCh)

	klog.Infof("%s started", o.logName)
	return nil
}

func (o *observer) Stop() {
	close(o.stopCh)
}

func (o *observer) onAdd(pod *corev1.Pod) {
	for _, rule := range o.rules {
		o.publish(rule, rule.OnAdd(pod))
	}
}

func (o *observer) onUpdate(oldPod, newPod *corev1.Pod) {
	for _, rule := range o.rules {
		o.publish(rule, rule.OnUpdate(oldPod, newPod))
	}
}

func (o *observer) publish(rule PodRule, eventsToPublish []events.Event) {
	for _, event := range eventsToPublish {
		if err := o.sink.RecordEvent(event); err != nil {
			klog.Errorf("failed to record event from %T: %v", rule, err)
		}
	}
}
