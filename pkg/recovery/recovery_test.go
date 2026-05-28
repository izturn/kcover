package recovery

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/preflight"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPreflightEventMarksSlowNodesAtDefaultThreshold(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
	)

	controller := NewController(client, nil, 0, 0)
	controller.onEvent(context.Background(), preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(context.Background(), preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

	assertNodeUnschedulable(t, client, "node-a", true)
	assertNodeUnschedulable(t, client, "node-b", true)
}

func TestPreflightEventUsesDefaultThresholdWithoutOverride(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
	)

	controller := NewController(client, nil, 0, 0)
	controller.onEvent(context.Background(), preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(context.Background(), preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

	assertNodeUnschedulable(t, client, "node-a", true)
	assertNodeUnschedulable(t, client, "node-b", true)
}

func TestSweepExpiredPreflightReportsDropsIncompleteWorkload(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	controller := NewController(client, nil, 0, 0)
	now := time.Unix(100, 0)
	controller.preflight.aggregator = preflight.NewSlowNodeAggregator(10 * time.Second)
	controller.preflight.aggregator.SetNowForTest(func() time.Time { return now })

	controller.onEvent(context.Background(), preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	if len(controller.preflight.aggregator.ExpireTimedOutWorkloads()) != 0 {
		t.Fatal("ExpireTimedOutWorkloads() returned errors before timeout, want none")
	}

	now = now.Add(11 * time.Second)
	errs := controller.preflight.aggregator.ExpireTimedOutWorkloads()
	if len(errs) != 1 {
		t.Fatalf("len(ExpireTimedOutWorkloads()) = %d, want 1", len(errs))
	}
	if errs[0].FirstReportedNode() != "node-a" {
		t.Fatalf("errs[0].FirstReportedNodeName() = %q, want node-a", errs[0].FirstReportedNode())
	}
	if !strings.Contains(errs[0].Error(), "got 1/2 reports") {
		t.Fatalf("ExpireTimedOutWorkloads error = %q, want report count detail", errs[0])
	}

	controller.onEvent(context.Background(), preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))
	assertNodeUnschedulable(t, client, "node-a", false)
}

func TestDuplicatePreflightEventDoesNotRefreshTimeoutWindow(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	controller := NewController(client, nil, 0, 0)
	now := time.Unix(100, 0)
	controller.preflight.aggregator = preflight.NewSlowNodeAggregator(10 * time.Second)
	controller.preflight.aggregator.SetNowForTest(func() time.Time { return now })

	evt := preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a"))
	controller.onEvent(context.Background(), evt)

	now = now.Add(9 * time.Second)
	controller.onEvent(context.Background(), evt)

	now = now.Add(2 * time.Second)
	errs := controller.preflight.aggregator.ExpireTimedOutWorkloads()
	if len(errs) != 1 {
		t.Fatalf("len(ExpireTimedOutWorkloads()) = %d, want 1", len(errs))
	}
	if errs[0].ReceivedReports != 1 {
		t.Fatalf("errs[0].ReceivedReports = %d, want 1", errs[0].ReceivedReports)
	}
}

func TestProcessedPreflightEntriesExpireDuringSweep(t *testing.T) {
	t.Parallel()

	controller := NewController(fake.NewSimpleClientset(), nil, 10*time.Millisecond, 0)

	evt := preflightEvent("train-ns", "node-a", "job-a", reportText("job-a", 2, 0, "node-a"))
	duplicate, err := controller.preflight.markProcessed(evt)
	if err != nil {
		t.Fatalf("markProcessed(...) error = %v", err)
	}
	if duplicate {
		t.Fatal("first markProcessed(...) = true, want false")
	}
	if controller.preflight.processed.Len() != 1 {
		t.Fatalf("processed.Len() = %d, want 1", controller.preflight.processed.Len())
	}

	time.Sleep(20 * time.Millisecond)
	controller.sweepExpiredPreflightReports()
	if controller.preflight.processed.Len() != 0 {
		t.Fatalf("processed.Len() = %d, want 0 after sweep", controller.preflight.processed.Len())
	}
	duplicate, err = controller.preflight.markProcessed(evt)
	if err != nil {
		t.Fatalf("markProcessed(...) after expiry error = %v", err)
	}
	if duplicate {
		t.Fatal("markProcessed(...) after expiry = true, want false")
	}
}

func TestProcessedPreflightEntriesAreDroppedWhenWorkloadCompletes(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
	)
	controller := NewController(client, nil, time.Minute, 0)

	controller.onEvent(context.Background(), preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	if controller.preflight.processed.Len() != 1 {
		t.Fatalf("processed.Len() after first report = %d, want 1", controller.preflight.processed.Len())
	}

	controller.onEvent(context.Background(), preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))
	if controller.preflight.processed.Len() != 0 {
		t.Fatalf("processed.Len() after workload completion = %d, want 0", controller.preflight.processed.Len())
	}
}

func TestDay2NodeEventSkipsJobRecovery(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pod-a",
				Labels: map[string]string{
					constants.KubeflowJobLabel:     "job-a",
					constants.EnabledRecoveryLabel: constants.True,
				},
			},
			Spec: corev1.PodSpec{NodeName: "node-a"},
		},
	)
	controller := NewController(client, nil, 0, 0)

	controller.onEvent(context.Background(), events.Event{
		ResourceType: events.Node,
		Name:         "node-a",
		Reason:       events.Day2EventReason,
		EventType:    events.Error,
	})

	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{}); err != nil {
		t.Fatalf("Get(pod-a) error = %v, want pod to remain because day2 should skip job recovery", err)
	}
	assertNodeUnschedulable(t, client, "node-a", true)
}

func TestStopReturnsWithoutEventStreamClose(t *testing.T) {
	t.Parallel()

	stream := blockingEventStream{ch: make(chan events.Event)}
	controller := NewController(fake.NewSimpleClientset(), stream, 0, time.Hour)
	if err := controller.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		controller.Stop()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop() did not return before event stream closed")
	}
}

type blockingEventStream struct {
	ch <-chan events.Event
}

func (s blockingEventStream) EventChan() <-chan events.Event {
	return s.ch
}

func preflightEvent(namespace, nodeName, _ string, report string) events.Event {
	return events.Event{
		ResourceType: events.Node,
		Namespace:    namespace,
		Name:         nodeName,
		EventType:    events.Error,
		Message:      report,
		Annotations: map[string]string{
			constants.PreflightNamespaceAnnotation: namespace,
			constants.PreflightDedupKeyAnnotation:  preflight.EventDedupKey(namespace, nodeName, "job-a", report),
			constants.PreflightWorkloadAnnotation:  "job-a",
		},
	}
}

func reportText(workloadName string, worldSize, rank int, nodeName string) string {
	selfIP := "10.0.0.1"
	if rank != 0 {
		selfIP = "10.0.0.2"
	}

	return `{"version":1,"workload":"` + workloadName + `","workload_size":` + strconv.Itoa(worldSize) + `,"rank":` + strconv.Itoa(rank) + `,"node_name":"` + nodeName + `","gpu_check":2,"storage_check":2,"batches":[{"batch_idx":0,"pair":["10.0.0.1","10.0.0.2"],"self_ip":"` + selfIP + `","status":"fail"}]}`
}

func assertNodeUnschedulable(t *testing.T, client *fake.Clientset, nodeName string, want bool) {
	t.Helper()

	node, err := client.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(node=%s) error = %v", nodeName, err)
	}
	if node.Spec.Unschedulable != want {
		t.Fatalf("node %s unschedulable = %v, want %v", nodeName, node.Spec.Unschedulable, want)
	}
}
