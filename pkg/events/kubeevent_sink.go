package events

import (
	"context"
	"fmt"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/kube"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
)

type kubeEventSink struct {
	client   kubernetes.Interface
	recorder record.EventRecorder
}

const preflightEventReason = "PreflightReportAvailable"

func NewKubeEventSink(cli kubernetes.Interface) Sink {
	return &kubeEventSink{
		client:   cli,
		recorder: newEventRecorder(cli),
	}
}

func newEventRecorder(cli kubernetes.Interface) record.EventRecorder {
	eb := record.NewBroadcaster()
	eb.StartRecordingToSink(&v1.EventSinkImpl{
		Interface: cli.CoreV1().Events(""),
	})

	return eb.NewRecorder(runtime.NewScheme(), corev1.EventSource{Component: "kcover"})
}

func (sink *kubeEventSink) recordToPod(e Event) error {
	pod, err := sink.client.CoreV1().Pods(e.Namespace).Get(context.Background(), e.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	return sink.recordEvent(pod, e)
}

func (sink *kubeEventSink) recordToNode(e Event) error {
	node, err := sink.client.CoreV1().Nodes().Get(context.Background(), e.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	return sink.recordEvent(node, e)
}

func (sink *kubeEventSink) recordEvent(obj runtime.Object, event Event) error {
	ref, err := reference.GetReference(scheme.Scheme, obj)
	if err != nil {
		return err
	}

	if isInternalPreflightEvent(event) {
		return sink.recordPreflightEvent(ref, event)
	}

	fixEventNamespace(ref, event)
	return sink.recordStdEvent(ref, event)
}

func fixEventNamespace(ref *corev1.ObjectReference, event Event) {
	if ref == nil || ref.Namespace != "" || event.ResourceType != Node {
		return
	}

	// Only day2 node events should be pinned to the agent runtime namespace.
	ns := event.Namespace
	if ns == "" {
		ns = kube.CurrentNamespace()
	}
	ref.Namespace = ns
}

func (sink *kubeEventSink) recordStdEvent(ref *corev1.ObjectReference, event Event) error {
	sink.recorder.AnnotatedEventf(ref, annotationsForEvent(event), corev1.EventTypeWarning, "Error", "%s", event.Message)
	return nil
}

func (sink *kubeEventSink) recordPreflightEvent(ref *corev1.ObjectReference, event Event) error {
	if ref == nil {
		return fmt.Errorf("preflight event reference is nil")
	}

	evt := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "kcover-preflight-",
			Namespace:    kube.CurrentNamespace(),
			Annotations:  annotationsForEvent(event),
		},
		InvolvedObject: *ref,
		Reason:         preflightEventReason,
		Message:        preflightEventMessage(event),
		Type:           corev1.EventTypeNormal,
		Source:         corev1.EventSource{Component: "kcover"},
	}
	_, err := sink.client.CoreV1().Events(kube.CurrentNamespace()).Create(context.Background(), evt, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create preflight event for %s: %w", ref.Name, err)
	}

	return nil
}

func preflightEventMessage(event Event) string {
	workload := event.Annotations[constants.PreflightWorkloadAnnotation]
	if workload == "" {
		return fmt.Sprintf("preflight report available for node %s", event.Name)
	}

	return fmt.Sprintf("preflight report available for workload %s on node %s", workload, event.Name)
}

func annotationsForEvent(event Event) map[string]string {
	annotations := make(map[string]string, len(event.Annotations)+1)
	for key, value := range event.Annotations {
		annotations[key] = value
	}

	if isInternalPreflightEvent(event) {
		annotations[constants.PreflightNamespaceAnnotation] = event.Namespace
		annotations[constants.PreflightPayloadAnnotation] = event.Message
		return annotations
	}

	if _, ok := annotations[constants.NeedRecoveryAnnotation]; !ok {
		annotations[constants.NeedRecoveryAnnotation] = constants.True
	}

	return annotations
}

func isInternalPreflightEvent(event Event) bool {
	return event.Annotations[constants.PreflightWorkloadAnnotation] != ""
}

func isPreflightEvent(annotations map[string]string) bool {
	return annotations[constants.PreflightNamespaceAnnotation] != ""
}

func (sink *kubeEventSink) RecordEvent(e Event) error {
	var err error
	switch e.ResourceType {
	case Pod:
		err = sink.recordToPod(e)
	case Node:
		err = sink.recordToNode(e)
	default:
		return fmt.Errorf("unsupported target type: %s", e.ResourceType)
	}

	return err
}
