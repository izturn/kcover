package recovery

import (
	"context"
	"testing"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/preflight"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPreflightEventRespectsSlowNodeScoreFromConfigMap(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: preflight.ConfigMapName, Namespace: "default"},
			Data: map[string]string{
				preflight.ConfigKeyBusBWThreshold: "5",
				preflight.ConfigKeySlowNodeScore:  "2",
			},
		},
	)

	controller := NewController(client, nil)
	controller.onEvent(preflightEvent("default", "node-a", "job-a", reportText("job-a", 2, 0, "node-a")))
	controller.onEvent(preflightEvent("default", "node-b", "job-a", reportText("job-a", 2, 1, "node-b")))

	assertNodeUnschedulable(t, client, "node-a", false)
	assertNodeUnschedulable(t, client, "node-b", false)
}

func TestPreflightEventUsesDefaultSlowNodeScoreWhenConfigMapMissing(t *testing.T) {
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
	return `{"version":1,"workload":"` + jobName + `","world_size":` + itoa(worldSize) + `,"rank":` + itoa(rank) + `,"node_name":"` + nodeName + `","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"batch_idx":0,"pair":["node-a","node-b"],"status":"fail"}]}`
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

func itoa(value int) string {
	if value == 0 {
		return "0"
	}

	buf := [20]byte{}
	index := len(buf)
	for value > 0 {
		index--
		buf[index] = byte('0' + value%10)
		value /= 10
	}

	return string(buf[index:])
}
