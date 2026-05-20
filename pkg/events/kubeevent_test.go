package events

import (
	"context"
	"testing"

	"github.com/baizeai/kcover/pkg/constants"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRecordEventStoresPreflightPayloadInEventAnnotation(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	sink := NewKubeEventSink(client).(*kubeEventSink)
	payload := `{"workload_size":2,"rank":0,"node_name":"node-a","gpu_check":1,"storage_check":1,"batches":[{"batch_idx":0,"pair":["10.0.0.1","10.0.0.2"],"self_ip":"10.0.0.1","status":"fail"}]}`

	err := sink.RecordEvent(Event{
		ResourceType: Node,
		Namespace:    "default",
		Name:         "node-a",
		EventType:    Error,
		Message:      payload,
		Annotations: map[string]string{
			constants.PreflightWorkloadAnnotation: "job-a",
		},
	})
	if err != nil {
		t.Fatalf("RecordEvent(...) error = %v", err)
	}

	events, err := client.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(events) error = %v", err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("len(events.Items) = %d, want 1", len(events.Items))
	}
	stored := events.Items[0]
	if stored.Type != corev1.EventTypeNormal {
		t.Fatalf("event type = %q, want %q", stored.Type, corev1.EventTypeNormal)
	}
	if stored.Reason != preflightEventReason {
		t.Fatalf("event reason = %q, want %q", stored.Reason, preflightEventReason)
	}
	if stored.Message != "preflight report available for workload(job-a) on node(node-a)" {
		t.Fatalf("event message = %q, want %q", stored.Message, "preflight report available for workload(job-a) on node(node-a)")
	}
	if stored.Annotations[constants.PreflightPayloadAnnotation] != payload {
		t.Fatalf("event preflight payload annotation = %q, want %q", stored.Annotations[constants.PreflightPayloadAnnotation], payload)
	}
	if stored.Annotations[constants.PreflightNamespaceAnnotation] != "default" {
		t.Fatalf("event preflight namespace annotation = %q, want %q", stored.Annotations[constants.PreflightNamespaceAnnotation], "default")
	}
	if stored.InvolvedObject.Namespace != stored.Namespace {
		t.Fatalf("involved object namespace = %q, want %q", stored.InvolvedObject.Namespace, stored.Namespace)
	}
}

func TestToInternalEventHydratesPreflightPayloadFromEventAnnotation(t *testing.T) {
	t.Parallel()

	payload := `{"workload_size":2,"rank":0,"node_name":"node-a","gpu_check":1,"storage_check":1,"batches":[{"batch_idx":0,"pair":["10.0.0.1","10.0.0.2"],"self_ip":"10.0.0.1","status":"fail"}]}`
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	bridge := NewKubeEventBridge(client).(*kubeEventBridge)

	event, ok := bridge.toInternalEvent(&corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "preflight-event",
			Namespace: "default",
			Annotations: map[string]string{
				constants.PreflightNamespaceAnnotation: "train-ns",
				constants.PreflightPayloadAnnotation:   payload,
				constants.PreflightWorkloadAnnotation:  "job-a",
			},
		},
		Message: "preflight report available for workload job-a on node node-a",
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       "node-a",
			FieldPath:  "",
		},
	})
	if !ok {
		t.Fatal("toInternalEvent(...) ok = false, want true")
	}
	if event.Namespace != "train-ns" {
		t.Fatalf("event.Namespace = %q, want %q", event.Namespace, "train-ns")
	}
	if event.Message != payload {
		t.Fatalf("event.Message = %q, want %q", event.Message, payload)
	}
	if event.Annotations[constants.PreflightWorkloadAnnotation] != "job-a" {
		t.Fatalf("job annotation = %q, want %q", event.Annotations[constants.PreflightWorkloadAnnotation], "job-a")
	}
}

func TestReasonForEventUsesDay2ReasonForNodeEvent(t *testing.T) {
	t.Parallel()

	reason := reasonForEvent(Event{
		ResourceType: Node,
		Name:         "node-a",
		Reason:       Day2EventReason,
		EventType:    Error,
		Message:      "day2 check failed",
	})
	if reason != Day2EventReason {
		t.Fatalf("reasonForEvent(...) = %q, want %q", reason, Day2EventReason)
	}
}

func TestShouldWatchEventAllowsPreflightNodeEvent(t *testing.T) {
	t.Parallel()

	bridge := NewKubeEventBridge(fake.NewSimpleClientset()).(*kubeEventBridge)

	if !bridge.shouldWatchEvent(&corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				constants.PreflightWorkloadAnnotation: "job-a",
			},
		},
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: "node-a"},
	}) {
		t.Fatal("shouldWatchEvent(preflight node event) = false, want true")
	}
}

func TestShouldWatchEventAllowsDay2NodeEvent(t *testing.T) {
	t.Parallel()

	bridge := NewKubeEventBridge(fake.NewSimpleClientset()).(*kubeEventBridge)

	if !bridge.shouldWatchEvent(&corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				constants.NeedRecoveryAnnotation: constants.True,
			},
		},
		Reason:         Day2EventReason,
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: "node-a"},
	}) {
		t.Fatal("shouldWatchEvent(day2 node event) = false, want true")
	}
}

func TestShouldWatchEventRejectsNonDay2NodeRecoveryEvent(t *testing.T) {
	t.Parallel()

	bridge := NewKubeEventBridge(fake.NewSimpleClientset()).(*kubeEventBridge)

	if bridge.shouldWatchEvent(&corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				constants.NeedRecoveryAnnotation: constants.True,
			},
		},
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: "node-a"},
	}) {
		t.Fatal("shouldWatchEvent(non-day2 node recovery event) = true, want false")
	}
}

func TestShouldWatchEventKeepsPodRecoveryLogic(t *testing.T) {
	t.Parallel()

	bridge := NewKubeEventBridge(fake.NewSimpleClientset()).(*kubeEventBridge)

	if !bridge.shouldWatchEvent(&corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				constants.NeedRecoveryAnnotation: constants.True,
			},
		},
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "default", Name: "pod-a"},
	}) {
		t.Fatal("shouldWatchEvent(pod recovery event) = false, want true")
	}
}
