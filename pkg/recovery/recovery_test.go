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

func TestPreflightEventIgnoresConfiguredSlowNodeThreshold(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: preflight.ConfigMapName, Namespace: "default"},
			Data: map[string]string{
				preflight.ConfigKeyBusBWThreshold: "5",
				"SLOW_NODE_THRESHOLD":             "2",
			},
		},
	)

	controller := NewController(client, nil)
	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

	assertNodeUnschedulable(t, client, "node-a", true)
	assertNodeUnschedulable(t, client, "node-b", true)
}

func TestPreflightEventUsesDefaultSlowNodeThresholdWhenConfigMapMissing(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
	)

	controller := NewController(client, nil)
	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

	assertNodeUnschedulable(t, client, "node-a", true)
	assertNodeUnschedulable(t, client, "node-b", true)
}

func TestSweepExpiredPreflightReportsDropsIncompleteJob(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	controller := NewController(client, nil)
	now := time.Unix(100, 0)
	controller.slowNodeAggregator = preflight.NewSlowNodeAggregator(preflight.Settings{ReportCollectionTimeout: 10 * time.Second})
	controller.slowNodeAggregator.SetNowForTest(func() time.Time { return now })

	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	if len(controller.slowNodeAggregator.ExpireStale()) != 0 {
		t.Fatal("ExpireStale() returned errors before timeout, want none")
	}

	now = now.Add(11 * time.Second)
	errs := controller.slowNodeAggregator.ExpireStale()
	if len(errs) != 1 {
		t.Fatalf("len(ExpireStale()) = %d, want 1", len(errs))
	}
	if errs[0].AnchorNodeName() != "node-a" {
		t.Fatalf("errs[0].AnchorNodeName() = %q, want node-a", errs[0].AnchorNodeName())
	}
	if !strings.Contains(errs[0].Error(), "got 1/2 reports") {
		t.Fatalf("ExpireStale error = %q, want report count detail", errs[0])
	}

	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))
	assertNodeUnschedulable(t, client, "node-a", false)
}

func TestReportPreflightAggregationTimeoutRecordsWarningEvent(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	controller := NewController(client, nil)
	recorded := &recordingSink{}
	controller.eventSink = recorded
	controller.reportPreflightAggregationTimeout(preflight.ReportCollectionTimeoutError{
		Namespace:       "default",
		JobName:         "job-a",
		ReportedNodes:   []string{"node-a", "node-b"},
		ReceivedReports: 2,
		ExpectedReports: 16,
		Timeout:         30 * time.Minute,
	})

	if len(recorded.events) != 1 {
		t.Fatalf("len(recorded.events) = %d, want 1", len(recorded.events))
	}
	if recorded.events[0].Name != "node-a" {
		t.Fatalf("recorded.events[0].Name = %q, want node-a", recorded.events[0].Name)
	}
	message := recorded.events[0].Message
	if !strings.Contains(message, "job-a") || !strings.Contains(message, "2/16") {
		t.Fatalf("warning event message = %q, want job/report summary", message)
	}
}

type recordingSink struct {
	events []events.Event
}

func (s *recordingSink) RecordEvent(event events.Event) error {
	s.events = append(s.events, event)
	return nil
}

func preflightEvent(namespace, nodeName, jobName, report string) events.Event {
	return events.Event{
		ResourceType: events.Node,
		Namespace:    namespace,
		Name:         nodeName,
		EventType:    events.Error,
		Message:      report,
		Annotations: map[string]string{
			constants.PreflightReportAnnotation: constants.True,
			constants.KubeflowJobLabel:          jobName,
		},
	}
}

func reportText(jobName string, worldSize, rank int, nodeName string) string {
	return `{"version":1,"workload":"` + jobName + `","world_size":` + strconv.Itoa(worldSize) + `,"rank":` + strconv.Itoa(rank) + `,"node_name":"` + nodeName + `","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"batch_idx":0,"pair":["node-a","node-b"],"status":"fail"}]}`
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
