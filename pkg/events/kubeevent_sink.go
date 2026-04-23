package events

import (
	"context"
	"fmt"

	"github.com/baizeai/kcover/pkg/constants"

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

var recoveryEventAnnotations = map[string]string{
	constants.NeedRecoveryAnnotation: constants.True,
}

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

	return sink.recordRecoveryEvent(pod, e)
}

func (sink *kubeEventSink) recordToNode(e Event) error {
	node, err := sink.client.CoreV1().Nodes().Get(context.Background(), e.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	return sink.recordRecoveryEvent(node, e)
}

func (sink *kubeEventSink) recordRecoveryEvent(obj runtime.Object, event Event) error {
	ref, err := reference.GetReference(scheme.Scheme, obj)
	if err != nil {
		return err
	}

	sink.recorder.AnnotatedEventf(ref, recoveryEventAnnotations, corev1.EventTypeWarning, "Error", "%s", event.Message)
	return nil
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
