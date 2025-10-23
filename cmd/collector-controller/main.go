package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/baizeai/kcover/pkg/diagnosis/agent"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var pVendor = flag.Int("vendor", 1, "the gpu vendor: 1-metax 2-nvidia default: 1")

func main() {
	flag.Parse()

	cfg := kube.GetK8sConfigConfigWithFile("", "")
	client := kubernetes.NewForConfigOrDie(cfg)
	evtWriter := events.NewKubeEventsRecorder(client, false)

	diag := agent.MustNewDiagnosis(mustHostName(), agent.Vendor(*pVendor), evtWriter)

	klog.Info("the node info collector is started")
	diag.Start()

	cc := make(chan os.Signal, 1)
	signal.Notify(cc, os.Interrupt, syscall.SIGTERM)
	<-cc

	diag.Stop()
	klog.Info("the node info collector is stopped")
}

func mustHostName() string {
	if hn := os.Getenv("FAST_RECOVERY_NODE_NAME"); hn != "" {
		return hn
	}
	hn, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hn
}
