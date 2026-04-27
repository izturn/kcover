package metax

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	kcoverconfig "github.com/baizeai/kcover/cmd/agent/config"
)

const defaultCheckHour = 10

func TestNextCheckTimeSameDay(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 9, 30, 0, 0, time.FixedZone("UTC+8", 8*3600))
	next := nextCheckTime(now, defaultCheckHour)

	want := time.Date(2026, 4, 22, 10, 0, 0, 0, now.Location())
	if !next.Equal(want) {
		t.Fatalf("nextCheckTime() = %v, want %v", next, want)
	}
}

func TestNextCheckTimeNextDay(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	next := nextCheckTime(now, defaultCheckHour)

	want := time.Date(2026, 4, 23, 10, 0, 0, 0, now.Location())
	if !next.Equal(want) {
		t.Fatalf("nextCheckTime() = %v, want %v", next, want)
	}
}

func TestNextCheckTimeAfterScheduledHour(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 10, 5, 0, 0, time.FixedZone("UTC+8", 8*3600))
	next := nextCheckTime(now, defaultCheckHour)

	want := time.Date(2026, 4, 23, 10, 0, 0, 0, now.Location())
	if !next.Equal(want) {
		t.Fatalf("nextCheckTime() = %v, want %v", next, want)
	}

}

func TestNewDiagnosisKeepsIntervalAndHour(t *testing.T) {
	t.Parallel()

	diag := NewDiagnosis(kcoverconfig.MetaX{
		NodeName:           "node-a",
		HCAIDs:             []string{"mlx5_0", "mlx5_1"},
		GPUNum:             8,
		Temperature:        85,
		ECCMaxCount:        64,
		NTPMaxOffsetMillis: 10,
		Day2CheckHour:      defaultCheckHour,
	}, 5)
	if diag.config.NodeName != "node-a" {
		t.Fatalf("diag.config.NodeName = %q, want %q", diag.config.NodeName, "node-a")
	}
	if !reflect.DeepEqual(diag.config.HCAIDs, []string{"mlx5_0", "mlx5_1"}) {
		t.Fatalf("diag.config.HCAIDs = %v, want %v", diag.config.HCAIDs, []string{"mlx5_0", "mlx5_1"})
	}
	if diag.interval != 5 {
		t.Fatalf("diag.interval = %d, want 5", diag.interval)
	}
	if diag.config.GPUNum != 8 {
		t.Fatalf("diag.config.GPUNum = %d, want 8", diag.config.GPUNum)
	}
	if diag.config.Temperature != 85 {
		t.Fatalf("diag.config.Temperature = %d, want 85", diag.config.Temperature)
	}
	if diag.config.ECCMaxCount != 64 {
		t.Fatalf("diag.config.ECCMaxCount = %d, want 64", diag.config.ECCMaxCount)
	}
	if diag.config.NTPMaxOffsetMillis != 10 {
		t.Fatalf("diag.config.NTPMaxOffsetMillis = %v, want 10", diag.config.NTPMaxOffsetMillis)
	}
	if diag.config.Day2CheckHour != defaultCheckHour {
		t.Fatalf("diag.config.Day2CheckHour = %d, want %d", diag.config.Day2CheckHour, defaultCheckHour)
	}
}

func TestParseHotspotTemperatures(t *testing.T) {
	t.Parallel()

	temperatures, err := parseHotspotTemperatures([]byte(`
mx-smi  version: 2.2.9

=================== MetaX System Management Interface Log ===================
Timestamp                                         : Thu Apr 23 07:34:59 2026

Attached GPUs                                     : 2
GPU#0  MXC500  0000:08:00.0

	Chip Temperature
	    hotspot(sensor0)                          :  39.00 C
	Board Temperature
	    air-inlet                                 :  28.75 C

GPU#1  MXC500  0000:09:00.0

	Chip Temperature
	    hotspot(sensor4)                          :  39.25 C
	Board Temperature
	    air-inlet                                 :  28.12 C

End of Log
`), []byte("hotspot("))
	if err != nil {
		t.Fatalf("parseHotspotTemperatures returned error: %v", err)
	}

	want := map[string]float64{
		"GPU#0": 39.00,
		"GPU#1": 39.25,
	}
	if !reflect.DeepEqual(temperatures, want) {
		t.Fatalf("temperatures = %v, want %v", temperatures, want)
	}
}

func TestParseHotspotTemperaturesRejectsInvalidHotspotLine(t *testing.T) {
	t.Parallel()

	_, err := parseHotspotTemperatures([]byte(`
GPU#0  MXC500  0000:08:00.0
	hotspot(sensor0)                          39.00 C
`), []byte("hotspot("))
	if err == nil {
		t.Fatal("parseHotspotTemperatures error = nil, want non-nil")
	}
}

func TestParseGPUIntegerMetricExtractsDoubleBitECCPerGPU(t *testing.T) {
	t.Parallel()

	values, err := parseGPUIntegerMetric([]byte(`
mx-smi  version: 2.2.9

=================== MetaX System Management Interface Log ===================
Timestamp                                         : Thu Apr 23 08:02:53 2026

Attached GPUs                                     : 3
GPU#0  MXC500  0000:08:00.0
    ECC Errors
        SRAM Correctable                          : 0
    Retired Pages
        Double Bit ECC                            : 0

GPU#1  MXC500  0000:09:00.0
    ECC Errors
        SRAM Correctable                          : 0
    Retired Pages
        Double Bit ECC                            : 4

GPU#2  MXC500  0000:0e:00.0
	ECC Errors
		SRAM Correctable                          : 0
	Retired Pages
		Double Bit ECC                            : 1
End of Log
`), []byte("Double Bit ECC"))
	if err != nil {
		t.Fatalf("parseGPUIntegerMetric returned error: %v", err)
	}

	want := map[string]int{
		"GPU#0": 0,
		"GPU#1": 4,
		"GPU#2": 1,
	}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestGPUValuesExceedingLimitReturnsEmptyWhenAllValuesAreWithinLimit(t *testing.T) {
	t.Parallel()

	offenders := doubleBitECCExceedingLimit(map[string]int{
		"GPU#0": 0,
		"GPU#1": 64,
	}, 64)

	if len(offenders) != 0 {
		t.Fatalf("gpuValuesExceedingLimit(...) = %v, want empty", offenders)
	}
}

func TestGPUValuesExceedingLimitReturnsSortedOffenders(t *testing.T) {
	t.Parallel()

	offenders := doubleBitECCExceedingLimit(map[string]int{
		"GPU#2": 70,
		"GPU#0": 0,
		"GPU#1": 65,
	}, 64)

	want := []string{"GPU#1=65", "GPU#2=70"}
	if !reflect.DeepEqual(offenders, want) {
		t.Fatalf("gpuValuesExceedingLimit(...) = %v, want %v", offenders, want)
	}
}

func TestGPUStatusCountCheckReturnsNilWhenEnoughGPUsAreAvailable(t *testing.T) {
	t.Parallel()

	err := gpuStatusCountCheck(2, map[string]string{
		"GPU#0": "Available",
		"GPU#1": "Available",
	}, "Available")

	if err != nil {
		t.Fatalf("gpuStatusCountCheck(...) = %v, want nil", err)
	}
}

func TestGPUStatusCountCheckIncludesDetailedOffenders(t *testing.T) {
	t.Parallel()

	err := gpuStatusCountCheck(3, map[string]string{
		"GPU#0": "Available",
		"GPU#2": "Unavailable",
		"GPU#1": "Disabled",
	}, "Available")
	if err == nil {
		t.Fatal("gpuStatusCountCheck(...) = nil, want non-nil")
	}

	want := "insufficient available GPUs: expected 3, found 1: GPU#1=Disabled, GPU#2=Unavailable"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

func TestParseHCAStates(t *testing.T) {
	t.Parallel()

	states, err := parseHCAStates([]byte(`
hca_id: mlx5_0
	transport:                      InfiniBand (0)
	phys_port_cnt:                  1
		port:   1
			state:                  PORT_ACTIVE (4)

hca_id: mlx5_1
	transport:                      InfiniBand (0)
	phys_port_cnt:                  1
		port:   1
			state:                  PORT_DOWN (1)
`))
	if err != nil {
		t.Fatalf("parseHCAStates returned error: %v", err)
	}

	want := map[string]string{
		"mlx5_0": "PORT_ACTIVE (4)",
		"mlx5_1": "PORT_DOWN (1)",
	}
	if !reflect.DeepEqual(states, want) {
		t.Fatalf("states = %v, want %v", states, want)
	}
}

func TestHCAIDsWithoutStateReturnsMissingAndInactiveSorted(t *testing.T) {
	t.Parallel()

	offenders := hcaIDsWithoutState([]string{"mlx5_2", "mlx5_0", "mlx5_1"}, map[string]string{
		"mlx5_0": "PORT_ACTIVE (4)",
		"mlx5_1": "PORT_DOWN (1)",
	}, "PORT_ACTIVE")

	want := []string{"mlx5_1=PORT_DOWN (1)", "mlx5_2=missing"}
	if !reflect.DeepEqual(offenders, want) {
		t.Fatalf("hcaIDsWithoutState(...) = %v, want %v", offenders, want)
	}
}

func TestHotspotTemperaturesExceedingLimitReturnsSortedOffenders(t *testing.T) {
	t.Parallel()

	offenders := hotspotTemperaturesExceedingLimit(map[string]float64{
		"GPU#1": 85.00,
		"GPU#0": 84.99,
		"GPU#2": 86.25,
	}, 85)

	want := []string{"GPU#1=85.00C", "GPU#2=86.25C"}
	if !reflect.DeepEqual(offenders, want) {
		t.Fatalf("hotspotTemperaturesExceedingLimit(...) = %v, want %v", offenders, want)
	}
}

func TestParseChronySystemTimeMillis(t *testing.T) {
	t.Parallel()

	offsetMillis, err := parseChronySystemTimeMillis([]byte(`
Reference ID    : 771CB7B8 (119.28.183.184)
Stratum         : 3
Ref time (UTC)  : Fri Apr 24 06:12:16 2026
System time     : 0.005333243 seconds fast of NTP time
Last offset     : -0.000130641 seconds
Leap status     : Normal
`))
	if err != nil {
		t.Fatalf("parseChronySystemTimeMillis returned error: %v", err)
	}

	if math.Abs(offsetMillis-5.333243) > 1e-9 {
		t.Fatalf("parseChronySystemTimeMillis(...) = %v, want 5.333243", offsetMillis)
	}
}

func TestParseChronySystemTimeMillisUsesAbsoluteValue(t *testing.T) {
	t.Parallel()

	offsetMillis, err := parseChronySystemTimeMillis([]byte(`
System time     : -0.009000000 seconds slow of NTP time
`))
	if err != nil {
		t.Fatalf("parseChronySystemTimeMillis returned error: %v", err)
	}

	if offsetMillis != 9 {
		t.Fatalf("parseChronySystemTimeMillis(...) = %v, want 9", offsetMillis)
	}
}

func TestParseChronySystemTimeMillisRejectsMissingSystemTimeLine(t *testing.T) {
	t.Parallel()

	_, err := parseChronySystemTimeMillis([]byte(`Leap status     : Normal
`))
	if err == nil {
		t.Fatal("parseChronySystemTimeMillis error = nil, want non-nil")
	}
}

func TestNTPSyncCheckErrorIncludesLimitAndActual(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("system time offset exceeds limit %.2f ms: actual=%.2f ms", 10.0, 12.5)
	if err.Error() != "system time offset exceeds limit 10.00 ms: actual=12.50 ms" {
		t.Fatalf("error string = %q, want %q", err.Error(), "system time offset exceeds limit 10.00 ms: actual=12.50 ms")
	}
}
