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
	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

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
	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

	assertNodeUnschedulable(t, client, "node-a", true)
	assertNodeUnschedulable(t, client, "node-b", true)
}

func TestSweepExpiredPreflightReportsDropsIncompleteWorkload(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	controller := NewController(client, nil, 0, 0)
	now := time.Unix(100, 0)
	controller.slowNodeAgg = preflight.NewSlowNodeAggregator(10 * time.Second)
	controller.slowNodeAgg.SetNowForTest(func() time.Time { return now })

	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	if len(controller.slowNodeAgg.ExpireTimedOutWorkloads()) != 0 {
		t.Fatal("ExpireTimedOutWorkloads() returned errors before timeout, want none")
	}

	now = now.Add(11 * time.Second)
	errs := controller.slowNodeAgg.ExpireTimedOutWorkloads()
	if len(errs) != 1 {
		t.Fatalf("len(ExpireTimedOutWorkloads()) = %d, want 1", len(errs))
	}
	if errs[0].FirstReportedNode() != "node-a" {
		t.Fatalf("errs[0].FirstReportedNodeName() = %q, want node-a", errs[0].FirstReportedNode())
	}
	if !strings.Contains(errs[0].Error(), "got 1/2 reports") {
		t.Fatalf("ExpireTimedOutWorkloads error = %q, want report count detail", errs[0])
	}

	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))
	assertNodeUnschedulable(t, client, "node-a", false)
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
