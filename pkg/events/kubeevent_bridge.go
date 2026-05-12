package events

import (
	"time"

	"github.com/baizeai/kcover/pkg/constants"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type kubeEventBridge struct {
	*kubeEventSink
	eventCh chan Event
	stopCh  chan struct{}
}

const eventMaxAge = 3 * time.Minute

func NewKubeEventBridge(cli kubernetes.Interface) Bridge {
	sink := NewKubeEventSink(cli).(*kubeEventSink)

	return &kubeEventBridge{
		kubeEventSink: sink,
		eventCh:       make(chan Event),
		stopCh:        make(chan struct{}),
	}
}

func (bridge *kubeEventBridge) Start() error {
	factory := informers.NewSharedInformerFactory(bridge.client, time.Minute)
	informer := factory.Core().V1().Events().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: bridge.handleK8sEventAdd,
	})
	if err != nil {
		return err
	}

	go informer.Run(bridge.stopCh)
	klog.Info("the kubeEventBridge is started")
	return nil
}

func (bridge *kubeEventBridge) handleK8sEventAdd(obj any) {
	event, ok := obj.(*corev1.Event)
	if !ok {
		return
	}

	if bridge.isExpiredEvent(event, time.Now()) {
		return
	}

	evt, ok := bridge.toInternalEvent(event)
	if !ok {
		return
	}

	bridge.eventCh <- evt
}

func (bridge *kubeEventBridge) isExpiredEvent(event *corev1.Event, now time.Time) bool {
	eventTimestamp := event.LastTimestamp
	if eventTimestamp.IsZero() {
		eventTimestamp = event.CreationTimestamp
	}

	if eventTimestamp.Add(eventMaxAge).Before(now) {
		klog.Infof("event %s is too old %s against %s, ignore it", event.Name, eventTimestamp.String(), now.String())
		return true
	}

	return false
}

func (bridge *kubeEventBridge) toInternalEvent(event *corev1.Event) (Event, bool) {
	if event.Annotations[constants.PreflightReportAnnotation] == constants.True {
		obj := event.InvolvedObject
		if obj.GroupVersionKind() != (schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"}) {
			return Event{}, false
		}

		return Event{
			ResourceType: Node,
			Namespace:    obj.Namespace,
			Name:         obj.Name,
			EventType:    Error,
			Message:      event.Message,
			Annotations:  copyAnnotations(event.Annotations),
		}, true
	}

	if event.Annotations[constants.NeedRecoveryAnnotation] != constants.True {
		return Event{}, false
	}

	obj := event.InvolvedObject
	switch obj.GroupVersionKind() {
	case schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}:
		return Event{
			ResourceType: Pod,
			Namespace:    obj.Namespace,
			Name:         obj.Name,
			EventType:    Error,
			Message:      event.Message,
		}, true
	case schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"}:
		return Event{
			ResourceType: Node,
			Name:         obj.Name,
			EventType:    Error,
			Message:      event.Message,
		}, true
	default:
		return Event{}, false
	}
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

func (bridge *kubeEventBridge) Stop() {
	close(bridge.stopCh)
	close(bridge.eventCh)
}

func (bridge *kubeEventBridge) EventChan() <-chan Event {
	return bridge.eventCh
}
