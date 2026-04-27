package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/baizeai/kcover/cmd/agent/config"
	"github.com/baizeai/kcover/pkg/diagnosis/node"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var pConfigPath = flag.String("config", config.DefaultPath, "path to the agent config file")

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

	cfg, err := config.Load(*pConfigPath)
	if err != nil {
		return fmt.Errorf("load agent config: %w", err)
	}

	hostName, err := hostName()
	if err != nil {
		return fmt.Errorf("resolve node name: %w", err)
	}

	k8sConfig := kube.GetK8sConfigConfigWithFile("", "")
	if k8sConfig == nil {
		return fmt.Errorf("load kubernetes config: config is nil")
	}

	client, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}

	sink := events.NewKubeEventSink(client)

	diag, err := node.NewDiagnostic(hostName, node.Vendor(cfg.Vendor), cfg.Interval, cfg.MetaX, sink)
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
