package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileAppliesDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte("vendor: 2\ninterval: 9\nmetaX:\n  gpuNum: 16\n  temperature: 70\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Vendor != 2 {
		t.Fatalf("cfg.Vendor = %d, want 2", cfg.Vendor)
	}
	if cfg.Interval != 9 {
		t.Fatalf("cfg.Interval = %d, want 9", cfg.Interval)
	}
	if cfg.MetaX.GPUNum != 16 {
		t.Fatalf("cfg.MetaX.GPUNum = %d, want 16", cfg.MetaX.GPUNum)
	}
	if cfg.MetaX.Temperature != 70 {
		t.Fatalf("cfg.MetaX.Temperature = %d, want 70", cfg.MetaX.Temperature)
	}
	if cfg.MetaX.HCAIDs != nil {
		t.Fatalf("cfg.MetaX.HCAIDs = %v, want nil", cfg.MetaX.HCAIDs)
	}
	if cfg.MetaX.Day2CheckHour != DefaultMetaXDay2CheckHour {
		t.Fatalf("cfg.MetaX.Day2CheckHour = %d, want %d", cfg.MetaX.Day2CheckHour, DefaultMetaXDay2CheckHour)
	}
	if cfg.MetaX.ECCMaxCount != DefaultMetaXECCMaxCount {
		t.Fatalf("cfg.MetaX.ECCMaxCount = %d, want %d", cfg.MetaX.ECCMaxCount, DefaultMetaXECCMaxCount)
	}
}

func TestLoadParsesMetaXHCAIDs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte("metaX:\n  hcaIDs:\n    - mlx5_0\n    - mlx5_4\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	want := []string{"mlx5_0", "mlx5_4"}
	if len(cfg.MetaX.HCAIDs) != len(want) {
		t.Fatalf("cfg.MetaX.HCAIDs = %v, want %v", cfg.MetaX.HCAIDs, want)
	}
	for index, hcaID := range want {
		if cfg.MetaX.HCAIDs[index] != hcaID {
			t.Fatalf("cfg.MetaX.HCAIDs = %v, want %v", cfg.MetaX.HCAIDs, want)
		}
	}
}

func TestLoadReturnsDefaultsWhenPathIsEmpty(t *testing.T) {
	t.Parallel()

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Vendor != DefaultVendor {
		t.Fatalf("cfg.Vendor = %d, want %d", cfg.Vendor, DefaultVendor)
	}
	if cfg.Interval != DefaultInterval {
		t.Fatalf("cfg.Interval = %d, want %d", cfg.Interval, DefaultInterval)
	}
	if cfg.MetaX.GPUNum != DefaultMetaXGPUNum {
		t.Fatalf("cfg.MetaX.GPUNum = %d, want %d", cfg.MetaX.GPUNum, DefaultMetaXGPUNum)
	}
}

func TestApplyDefaultsRepairsInvalidValues(t *testing.T) {
	t.Parallel()

	cfg := Agent{
		MetaX: MetaX{
			GPUNum:             -1,
			Temperature:        0,
			ECCMaxCount:        -1,
			NTPMaxOffsetMillis: 0,
			Day2CheckHour:      24,
		},
	}

	cfg.ApplyDefaults()

	if cfg.Vendor != DefaultVendor {
		t.Fatalf("cfg.Vendor = %d, want %d", cfg.Vendor, DefaultVendor)
	}
	if cfg.Interval != DefaultInterval {
		t.Fatalf("cfg.Interval = %d, want %d", cfg.Interval, DefaultInterval)
	}
	if cfg.MetaX.GPUNum != DefaultMetaXGPUNum {
		t.Fatalf("cfg.MetaX.GPUNum = %d, want %d", cfg.MetaX.GPUNum, DefaultMetaXGPUNum)
	}
	if cfg.MetaX.Temperature != DefaultMetaXTemperature {
		t.Fatalf("cfg.MetaX.Temperature = %d, want %d", cfg.MetaX.Temperature, DefaultMetaXTemperature)
	}
	if cfg.MetaX.ECCMaxCount != DefaultMetaXECCMaxCount {
		t.Fatalf("cfg.MetaX.ECCMaxCount = %d, want %d", cfg.MetaX.ECCMaxCount, DefaultMetaXECCMaxCount)
	}
	if cfg.MetaX.NTPMaxOffsetMillis != DefaultMetaXNTPMaxOffsetMS {
		t.Fatalf("cfg.MetaX.NTPMaxOffsetMillis = %v, want %v", cfg.MetaX.NTPMaxOffsetMillis, DefaultMetaXNTPMaxOffsetMS)
	}
	if cfg.MetaX.Day2CheckHour != DefaultMetaXDay2CheckHour {
		t.Fatalf("cfg.MetaX.Day2CheckHour = %d, want %d", cfg.MetaX.Day2CheckHour, DefaultMetaXDay2CheckHour)
	}
}
