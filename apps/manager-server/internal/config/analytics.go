package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultAnalyticsTimeout          = 20 * time.Second
	DefaultAnalyticsMaxConcurrent    = 2
	DefaultAnalyticsQueueSize        = 4
	DefaultAnalyticsAdmissionTimeout = 500 * time.Millisecond
)

func loadAnalyticsConfig(file fileConfig) (Config, error) {
	var cfg Config
	flags := []struct {
		key      string
		fallback bool
		target   *bool
	}{
		{"USAGE_MYSQL_ANALYTICS_ENABLED", file.MySQLAnalyticsEnabled, &cfg.MySQLAnalyticsEnabled},
		{"USAGE_MYSQL_BULK_ENABLED", file.MySQLBulkEnabled, &cfg.MySQLBulkEnabled},
		{"USAGE_MYSQL_HOURLY_ENABLED", file.MySQLHourlyEnabled, &cfg.MySQLHourlyEnabled},
		{"USAGE_ANALYTICS_PROGRESSIVE_ENABLED", file.AnalyticsProgressiveEnabled == nil || *file.AnalyticsProgressiveEnabled, &cfg.AnalyticsProgressiveEnabled},
	}
	for _, flag := range flags {
		*flag.target = flag.fallback
		if value := strings.TrimSpace(os.Getenv(flag.key)); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return Config{}, fmt.Errorf("%s must be a boolean", flag.key)
			}
			*flag.target = parsed
		}
	}
	timeout, err := boundedAnalyticsInt("USAGE_ANALYTICS_TIMEOUT_MS", file.AnalyticsTimeoutMS, int(DefaultAnalyticsTimeout/time.Millisecond), 1, 120000)
	if err != nil {
		return Config{}, err
	}
	concurrent, err := boundedAnalyticsInt("USAGE_ANALYTICS_MAX_CONCURRENT", file.AnalyticsMaxConcurrent, DefaultAnalyticsMaxConcurrent, 1, 8)
	if err != nil {
		return Config{}, err
	}
	queue, err := boundedAnalyticsInt("USAGE_ANALYTICS_QUEUE_SIZE", file.AnalyticsQueueSize, DefaultAnalyticsQueueSize, 0, 64)
	if err != nil {
		return Config{}, err
	}
	admission, err := boundedAnalyticsInt("USAGE_ANALYTICS_ADMISSION_TIMEOUT_MS", file.AnalyticsAdmissionTimeoutMS, int(DefaultAnalyticsAdmissionTimeout/time.Millisecond), 1, 30000)
	if err != nil {
		return Config{}, err
	}
	if admission > timeout {
		return Config{}, fmt.Errorf("analytics admission timeout must not exceed analytics timeout")
	}
	cfg.AnalyticsTimeout = time.Duration(timeout) * time.Millisecond
	cfg.AnalyticsMaxConcurrent = concurrent
	cfg.AnalyticsQueueSize = queue
	cfg.AnalyticsAdmissionTimeout = time.Duration(admission) * time.Millisecond
	return cfg, nil
}

func boundedAnalyticsInt(key string, file *int, fallback, min, max int) (int, error) {
	value := fallback
	if file != nil {
		value = *file
	}
	if input := strings.TrimSpace(os.Getenv(key)); input != "" {
		parsed, err := strconv.Atoi(input)
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		value = parsed
	}
	if value < min || value > max {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}
	return value, nil
}
