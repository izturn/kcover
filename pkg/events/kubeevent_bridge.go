package events

import (
	"fmt"
	"strings"
	"time"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/kube"

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
	factory := informers.NewSharedInformerFactory(bridge.client, 0)
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

	klog.V(4).Infof(
		"kubeEventBridge received k8s event ns=%s name=%s involved=%s/%s reason=%s annotations=%v",
		event.Namespace,
		event.Name,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Name,
		event.Reason,
		event.Annotations,
	)

	if bridge.isExpiredEvent(event, time.Now()) {
		klog.V(4).Infof("kubeEventBridge dropped expired event ns=%s name=%s", event.Namespace, event.Name)
		return
	}

	evt, ok := bridge.toInternalEvent(event)
	if !ok {
		klog.V(4).Infof("kubeEventBridge ignored non-recovery event ns=%s name=%s", event.Namespace, event.Name)
		return
	}

	klog.V(4).Infof(
		"kubeEventBridge forwarding internal event type=%s ns=%s name=%s eventType=%d",
		evt.ResourceType,
		evt.Namespace,
		evt.Name,
		evt.EventType,
	)
	bridge.eventCh <- evt
}

func (bridge *kubeEventBridge) isExpiredEvent(event *corev1.Event, now time.Time) bool {
	eventTimestamp := event.LastTimestamp
	if eventTimestamp.IsZero() {
		eventTimestamp = event.CreationTimestamp
	}

	if eventTimestamp.Add(eventMaxAge).Before(now) {
		klog.V(2).Infof("event %s is too old %s against %s, ignore it", event.Name, eventTimestamp.String(), now.String())
		return true
	}

	return false
}

func (bridge *kubeEventBridge) toInternalEvent(event *corev1.Event) (Event, bool) {
	if isPreflightEvent(event.Annotations) {
		obj := event.InvolvedObject
		if obj.GroupVersionKind() != (schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"}) {
			klog.V(2).Infof(
				"ignore preflight event with unsupported involved object gvk=%s/%s kind=%s",
				obj.APIVersion,
				obj.Kind,
				obj.Kind,
			)
			return Event{}, false
		}

		payload, err := bridge.preflightPayloadForEvent(event)
		if err != nil {
			klog.Errorf("load preflight payload for node %s: %v", obj.Name, err)
			return Event{}, false
		}

		klog.V(4).Infof("parsed preflight node event from agent node=%s ns=%s", obj.Name, obj.Namespace)
		return Event{
			ResourceType: Node,
			Namespace:    event.Annotations[constants.PreflightNamespaceAnnotation],
			Name:         obj.Name,
			EventType:    Error,
			Message:      payload,
			Annotations:  copyAnnotations(event.Annotations),
		}, true
	}

	if event.Annotations[constants.NeedRecoveryAnnotation] != constants.True {
		klog.V(4).Infof("skip event without recovery annotation ns=%s name=%s", event.Namespace, event.Name)
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
		if event.Namespace != kube.CurrentNamespace() {
			klog.V(4).Infof("skip day2 node event outside runtime namespace eventNs=%s runtimeNs=%s node=%s", event.Namespace, kube.CurrentNamespace(), obj.Name)
			return Event{}, false
		}
		return Event{
			ResourceType: Node,
			Namespace:    event.Namespace,
			Name:         obj.Name,
			EventType:    Error,
			Message:      event.Message,
		}, true
	default:
		klog.V(4).Infof("skip event with unsupported involved object kind=%s apiVersion=%s", obj.Kind, obj.APIVersion)
		return Event{}, false
	}
}

func (bridge *kubeEventBridge) preflightPayloadForEvent(event *corev1.Event) (string, error) {
	if payload := event.Annotations[constants.PreflightPayloadAnnotation]; payload != "" {
		return payload, nil
	}

	if looksLikeJSONPayload(event.Message) {
		return event.Message, nil
	}

	return "", fmt.Errorf("preflight payload annotation %s is empty", constants.PreflightPayloadAnnotation)
}

func looksLikeJSONPayload(message string) bool {
	return strings.HasPrefix(strings.TrimSpace(message), "{")
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
