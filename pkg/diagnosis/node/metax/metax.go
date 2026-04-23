package metax

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"time"

	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"

	"k8s.io/klog/v2"
)

var _ diagnosis.Diagnostic = (*diag)(nil)

type diag struct {
	nodeName  string
	eventCh   chan events.Event
	stopCh    chan struct{}
	interval  int
	gpuNum    int
	checkHour int
}

func NewDiagnosis(nodeName string, interval int, checkHour int) *diag {
	klog.Info("for vendor: metax")
	return &diag{
		nodeName:  nodeName,
		stopCh:    make(chan struct{}),
		eventCh:   make(chan events.Event),
		interval:  interval,
		checkHour: checkHour,
	}
}

func (d *diag) check() {
	output, err := runDay2()
	if err != nil {
		klog.Error(err)
		return
	}
	count := countAvailableGPUs(output, linePrefix, keyword)
	if count >= d.gpuNum {
		return
	}
	d.eventCh <- events.Event{
		ResourceType: events.Node,
		Name:         d.nodeName,
		EventType:    events.Error,
		Message:      fmt.Sprintf("insufficient available GPUs: expected %d, but found %d", d.gpuNum, count),
	}
}

func nextCheckTime(now time.Time, hour int) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}

	return next
}

func (d *diag) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(d.interval))
		defer ticker.Stop()

		timer := time.NewTimer(time.Until(nextCheckTime(time.Now(), d.checkHour)))
		defer timer.Stop()
		for {
			select {
			case <-ticker.C:
				d.check()

			case <-timer.C:
				d.check()
				timer.Reset(time.Until(nextCheckTime(time.Now(), d.checkHour)))

			case <-d.stopCh:
				return
			}
		}
	}()
	return nil
}

func (d *diag) Stop() {
	close(d.stopCh)
	close(d.eventCh)
}

func (d *diag) EventChan() <-chan events.Event {
	return d.eventCh
}

func (d *diag) String() string {
	return "MetaX"
}

var (
	keyword    = []byte("Available")
	linePrefix = []byte("GPU#")
)

func countAvailableGPUs(text []byte, prefix, keyword []byte) int {
	count := 0
	scanner := bufio.NewScanner(bytes.NewReader(text))

	for scanner.Scan() {
		line := scanner.Bytes()
		fields := bytes.Fields(line)

		if len(fields) >= 4 && bytes.HasPrefix(fields[0], prefix) {
			if bytes.Equal(fields[3], keyword) {
				count++
			}
		}
	}
	_ = scanner.Err()

	return count
}

/*
http://daocloud.feishu.cn/wiki/LjBlwK5riikYjEkfHujcr1lOnBh
GPU可用性:        mx-smi -L
GPU温度:          mx-smi --show-temperature\|grep hotspot
GPU ECC坏页检查:  mx-smi --count-ecc \| grep 'Double Bit ECC
IB网卡可用性:     ibdev2netdev\|grepUp\|WC -l
*/
func runDay2() ([]byte, error) {
	cmd := exec.Command("mx-smi", "-L")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run mx-smi -L: %v, stderr: %s", err, stderr.String())
	}
	return out.Bytes(), nil
}
