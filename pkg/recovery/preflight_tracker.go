package recovery

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/preflight"

	"github.com/jellydator/ttlcache/v3"
)

type preflightEventKey string

type preflightTracker struct {
	processed    *ttlcache.Cache[preflightEventKey, time.Time]
	processedTTL time.Duration
	aggregator   *preflight.SlowNodeAggregator
}

type preflightReportResult struct {
	workloadName string
	slowNodes    []string
	skipped      bool
	waiting      bool
	duplicate    bool
}

func newPreflightTracker(reportCollectionTimeout time.Duration) *preflightTracker {
	if reportCollectionTimeout <= 0 {
		reportCollectionTimeout = preflight.DefaultReportCollectionTimeout
	}

	return &preflightTracker{
		processed:    ttlcache.New[preflightEventKey, time.Time](),
		processedTTL: reportCollectionTimeout,
		aggregator:   preflight.NewSlowNodeAggregator(reportCollectionTimeout),
	}
}

func preflightEventNamespace(e events.Event) string {
	if namespace := e.Annotations[constants.PreflightNamespaceAnnotation]; namespace != "" {
		return namespace
	}

	return e.Namespace
}

func preflightEventKeyFromEvent(e events.Event) preflightEventKey {
	key := e.Annotations[constants.PreflightDedupKeyAnnotation]

	return preflightEventKey(key)
}

// markProcessed only deduplicates inside the current manager process lifetime.
// If the manager restarts or leadership moves, previously seen events can
// still be observed and processed again unless the dedup state is externalized.
func (s *preflightTracker) markProcessed(e events.Event) (bool, error) {
	key := preflightEventKeyFromEvent(e)
	if key == "" {
		return false, fmt.Errorf("missing annotation %s", constants.PreflightDedupKeyAnnotation)
	}
	if s.processed.Get(key, ttlcache.WithDisableTouchOnHit[preflightEventKey, time.Time]()) != nil {
		return true, nil
	}
	s.processed.Set(key, time.Now().Add(s.processedTTL), s.processedTTL)
	return false, nil
}

func (s *preflightTracker) cleanupProcessed() {
	s.processed.DeleteExpired()
}

func (s *preflightTracker) cleanupProcessedForWorkload(namespace, workloadName string) {
	if s == nil || namespace == "" || workloadName == "" {
		return
	}

	prefix := preflightEventKey(namespace + "/" + workloadName + "/")
	for key := range s.processed.Items() {
		if strings.HasPrefix(string(key), string(prefix)) {
			s.processed.Delete(key)
		}
	}
}

func (s *preflightTracker) handleReport(e events.Event) (preflightReportResult, error) {
	if s == nil || s.aggregator == nil {
		return preflightReportResult{skipped: true}, nil
	}

	namespace := preflightEventNamespace(e)
	workloadName := e.Annotations[constants.PreflightWorkloadAnnotation]
	if workloadName == "" {
		return preflightReportResult{skipped: true}, nil
	}

	duplicate, err := s.markProcessed(e)
	if err != nil {
		return preflightReportResult{}, err
	}
	if duplicate {
		return preflightReportResult{workloadName: workloadName, skipped: true, duplicate: true}, nil
	}

	ready, slowNodes, err := s.aggregator.AddReport(namespace, workloadName, e.Message)
	if err != nil {
		var timeoutErr preflight.WorkloadTimeoutError
		if errors.As(err, &timeoutErr) {
			s.cleanupProcessedForWorkload(timeoutErr.Namespace, timeoutErr.WorkloadName)
		}
		return preflightReportResult{workloadName: workloadName}, err
	}
	if !ready {
		return preflightReportResult{workloadName: workloadName, waiting: true}, nil
	}

	s.cleanupProcessedForWorkload(namespace, workloadName)

	return preflightReportResult{workloadName: workloadName, slowNodes: slowNodes}, nil
}

func (s *preflightTracker) sweepExpired() []preflight.WorkloadTimeoutError {
	if s == nil {
		return nil
	}

	s.cleanupProcessed()
	if s.aggregator == nil {
		return nil
	}

	errs := s.aggregator.ExpireTimedOutWorkloads()
	for _, err := range errs {
		s.cleanupProcessedForWorkload(err.Namespace, err.WorkloadName)
	}

	return errs
}
