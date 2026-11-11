package usageaggregate

import (
	"fmt"
	"strconv"
	"strings"
)

const cacheAccountingGenerationPrefix = "cache-accounting-v2-"

// IsCurrentStructureRevision accepts only generations issued for the current
// hourly format: the base, an optional 128-bit rebuild nonce, and zero or more
// historical accounting corrections. Repeated corrections were appended to the
// previous generation. Never normalize an existing generation: ledger claims
// must keep exactly the same revision as the aggregate state.
func IsCurrentStructureRevision(revision string) bool {
	parts := strings.Split(revision, ":")
	base := strings.Split(StructureRevision, ":")
	if len(parts) < len(base) || strings.Join(parts[:len(base)], ":") != StructureRevision {
		return false
	}
	parts = parts[len(base):]
	if len(parts) > 0 && strings.HasPrefix(parts[0], "rebuild-") {
		nonce := strings.TrimPrefix(parts[0], "rebuild-")
		if len(nonce) != 32 {
			return false
		}
		for _, value := range nonce {
			if (value < '0' || value > '9') && (value < 'a' || value > 'f') && (value < 'A' || value > 'F') {
				return false
			}
		}
		parts = parts[1:]
	}
	for _, part := range parts {
		marker, ok := strings.CutPrefix(part, cacheAccountingGenerationPrefix)
		if !ok {
			return false
		}
		startedAt, target, ok := strings.Cut(marker, "-")
		if !ok || !positiveGenerationInteger(startedAt) || !positiveGenerationInteger(target) {
			return false
		}
	}
	return true
}

func positiveGenerationInteger(value string) bool {
	parsed, err := strconv.ParseInt(value, 10, 64)
	return err == nil && parsed > 0 && strconv.FormatInt(parsed, 10) == value
}

// CacheAccountingStructureRevision constructs the same generation used by
// historical corrections. Retrying the same correction is idempotent; invalid
// state is rejected rather than relabeled as a compatible generation.
func CacheAccountingStructureRevision(previous string, startedAtMS, targetEventID int64) (string, error) {
	if !IsCurrentStructureRevision(previous) || startedAtMS <= 0 || targetEventID <= 0 {
		return "", fmt.Errorf("invalid hourly cache-accounting generation: revision=%q started=%d target=%d", previous, startedAtMS, targetEventID)
	}
	marker := fmt.Sprintf(":%s%d-%d", cacheAccountingGenerationPrefix, startedAtMS, targetEventID)
	if strings.HasSuffix(previous, marker) {
		return previous, nil
	}
	return previous + marker, nil
}
