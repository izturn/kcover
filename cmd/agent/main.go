package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/baizeai/kcover/pkg/diagnosis/node"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var (
	pVendor        = flag.Int("vendor", 1, "the gpu vendor: 1-metax 2-nvidia (default: 1)")
	pInterval      = flag.Int("interval", 5, "diagnostic interval in seconds (default: 5s)")
	pDay2CheckHour = flag.Int("day2-check-hour", 10, "daily day2 check hour in local time, 0-23 (default: 10)")
)

type config struct {
	vendor        node.Vendor
	interval      int
	day2CheckHour int
}

func main() {
	if err := run(); err != nil {
		klog.ErrorS(err, "agent exited with error")
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config{
		vendor:        node.Vendor(*pVendor),
		interval:      *pInterval,
		day2CheckHour: *pDay2CheckHour,
	}
	if cfg.interval <= 0 {
		cfg.interval = 5
	}
	if cfg.day2CheckHour < 0 || cfg.day2CheckHour > 23 {
		cfg.day2CheckHour = 10
	}

	hostName, err := hostName()
	if err != nil {
		return err
	}

	k8sConfig := kube.GetK8sConfigConfigWithFile("", "")
	if k8sConfig == nil {
		return fmt.Errorf("kubernetes config is nil")
	}

	client, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}

	sink := events.NewKubeEventSink(client)

	diag, err := node.NewDiagnostic(hostName, cfg.vendor, cfg.interval, cfg.day2CheckHour, sink)
	if err != nil {
		return fmt.Errorf("create node diagnostic: %w", err)
	}
	defer diag.Stop()

	if err := diag.Start(); err != nil {
		return fmt.Errorf("start node diagnostic: %w", err)
	}

	klog.Info("the node agent is started")
	<-ctx.Done()

	klog.Info("the node agent is stopped")
	return nil
}

func hostName() (string, error) {
	if hn := kube.NodeNameFromEnv(); hn != "" {
		return hn, nil
	}
	hn, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("get hostname: %w", err)
	}
	return hn, nil
}