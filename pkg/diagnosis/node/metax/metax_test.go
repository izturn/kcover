package metax

import (
	"testing"
	"time"
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

	diag := NewDiagnosis("node-a", 5, defaultCheckHour)
	if diag.interval != 5 {
		t.Fatalf("diag.interval = %d, want 5", diag.interval)
	}
	if diag.checkHour != defaultCheckHour {
		t.Fatalf("diag.checkHour = %d, want %d", diag.checkHour, defaultCheckHour)
	}
}
