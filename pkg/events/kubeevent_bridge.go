package events

import (
	"fmt"
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

var (
	podObjectGVK  = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	nodeObjectGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"}
)

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
	klog.InfoS("kube event bridge started")
	return nil
}

func (bridge *kubeEventBridge) handleK8sEventAdd(obj any) {
	event, ok := obj.(*corev1.Event)
	if !ok {
		return
	}

	klog.V(4).InfoS("kube event bridge received k8s event", "namespace", event.Namespace, "name", event.Name, "kind", event.InvolvedObject.Kind, "involvedName", event.InvolvedObject.Name, "reason", event.Reason, "annotations", event.Annotations)

	if bridge.isExpiredEvent(event, time.Now()) {
		klog.V(4).InfoS("kube event bridge dropped expired event", "namespace", event.Namespace, "name", event.Name)
		return
	}

	evt, ok := bridge.toInternalEvent(event)
	if !ok {
		klog.V(4).InfoS("kube event bridge ignored non-recovery event", "namespace", event.Namespace, "name", event.Name)
		return
	}

	klog.V(4).InfoS("kube event bridge forwarding internal event", "resourceType", evt.ResourceType, "namespace", evt.Namespace, "name", evt.Name, "eventType", evt.EventType)
	bridge.eventCh <- evt
}

func (bridge *kubeEventBridge) isExpiredEvent(event *corev1.Event, now time.Time) bool {
	eventTimestamp := event.LastTimestamp
	if eventTimestamp.IsZero() {
		eventTimestamp = event.CreationTimestamp
	}

	if eventTimestamp.Add(eventMaxAge).Before(now) {
		klog.V(2).InfoS("event is too old", "name", event.Name, "eventTimestamp", eventTimestamp.String(), "now", now.String())
		return true
	}

	return false
}

func (bridge *kubeEventBridge) toInternalEvent(event *corev1.Event) (Event, bool) {
	if IsPreflightEvent(event.Annotations) {
		return bridge.toInternalPreflightEvent(event)
	}

	if event.Annotations[constants.NeedRecoveryAnnotation] != constants.True {
		klog.V(4).InfoS("skip event without recovery annotation", "namespace", event.Namespace, "name", event.Name)
		return Event{}, false
	}

	return bridge.toInternalRecoveryEvent(event)
}

func (bridge *kubeEventBridge) toInternalPreflightEvent(event *corev1.Event) (Event, bool) {
	obj := event.InvolvedObject
	if !isNodeObjectRef(obj) {
		klog.V(2).InfoS("ignore preflight event with unsupported involved object", "apiVersion", obj.APIVersion, "kind", obj.Kind)
		return Event{}, false
	}

	payload, err := bridge.extractPreflightPayload(event)
	if err != nil {
		klog.ErrorS(err, "load preflight payload failed", "node", obj.Name)
		return Event{}, false
	}

	klog.V(4).InfoS("parsed preflight node event from agent", "node", obj.Name, "namespace", obj.Namespace)
	return Event{
		ResourceType: Node,
		Namespace:    event.Annotations[constants.PreflightNamespaceAnnotation],
		Name:         obj.Name,
		EventType:    Error,
		Message:      payload,
		Annotations:  copyAnnotations(event.Annotations),
	}, true
}

func (bridge *kubeEventBridge) toInternalRecoveryEvent(event *corev1.Event) (Event, bool) {
	obj := event.InvolvedObject
	if isPodObjectRef(obj) {
		return Event{
			ResourceType: Pod,
			Namespace:    obj.Namespace,
			Name:         obj.Name,
			EventType:    Error,
			Message:      event.Message,
		}, true
	}

	if isNodeObjectRef(obj) {
		if event.Namespace != kube.CurrentNamespace() {
			klog.V(4).InfoS("skip day2 node event outside runtime namespace", "eventNamespace", event.Namespace, "runtimeNamespace", kube.CurrentNamespace(), "node", obj.Name)
			return Event{}, false
		}
		return Event{
			ResourceType: Node,
			Namespace:    event.Namespace,
			Name:         obj.Name,
			EventType:    Error,
			Message:      event.Message,
		}, true
	}

	klog.V(4).InfoS("skip event with unsupported involved object", "kind", obj.Kind, "apiVersion", obj.APIVersion)
	return Event{}, false
}

func (bridge *kubeEventBridge) extractPreflightPayload(event *corev1.Event) (string, error) {
	if payload := event.Annotations[constants.PreflightPayloadAnnotation]; payload != "" {
		return payload, nil
	}
	return "", fmt.Errorf("preflight payload annotation %s is empty", constants.PreflightPayloadAnnotation)
}

func isNodeObjectRef(ref corev1.ObjectReference) bool {
	return ref.GroupVersionKind() == nodeObjectGVK
}

func isPodObjectRef(ref corev1.ObjectReference) bool {
	return ref.GroupVersionKind() == podObjectGVK
}

func (bridge *kubeEventBridge) Stop() {
	close(bridge.stopCh)
	close(bridge.eventCh)
}

func (bridge *kubeEventBridge) EventChan() <-chan Event {
	return bridge.eventCh
}
