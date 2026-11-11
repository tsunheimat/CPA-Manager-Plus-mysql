package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var analyticsEnvKeys = []string{
	"USAGE_MYSQL_ANALYTICS_ENABLED", "USAGE_MYSQL_BULK_ENABLED", "USAGE_MYSQL_HOURLY_ENABLED", "USAGE_ANALYTICS_PROGRESSIVE_ENABLED",
	"USAGE_ANALYTICS_TIMEOUT_MS", "USAGE_ANALYTICS_MAX_CONCURRENT", "USAGE_ANALYTICS_QUEUE_SIZE", "USAGE_ANALYTICS_ADMISSION_TIMEOUT_MS",
}

func clearAnalyticsEnv(t *testing.T) {
	t.Helper()
	for _, key := range analyticsEnvKeys {
		t.Setenv(key, "")
	}
}

func TestAnalyticsConfigurationDefaults(t *testing.T) {
	clearAnalyticsEnv(t)
	cfg, err := loadAnalyticsConfig(fileConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQLAnalyticsEnabled || cfg.MySQLBulkEnabled || cfg.MySQLHourlyEnabled || !cfg.AnalyticsProgressiveEnabled {
		t.Fatalf("incorrect feature defaults: %+v", cfg)
	}
	if cfg.AnalyticsTimeout != 20*time.Second || cfg.AnalyticsMaxConcurrent != 2 || cfg.AnalyticsQueueSize != 4 || cfg.AnalyticsAdmissionTimeout != 500*time.Millisecond {
		t.Fatalf("incorrect execution defaults: %+v", cfg)
	}
}

func TestAnalyticsConfigurationFileAndEnv(t *testing.T) {
	clearAnalyticsEnv(t)
	var file fileConfig
	if err := json.Unmarshal([]byte(`{"mysqlAnalyticsEnabled":true,"mysqlBulkEnabled":true,"mysqlHourlyEnabled":true,"analyticsProgressiveEnabled":false,"analyticsTimeoutMs":1000,"analyticsMaxConcurrent":3,"analyticsQueueSize":0,"analyticsAdmissionTimeoutMs":200}`), &file); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadAnalyticsConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.MySQLAnalyticsEnabled || !cfg.MySQLBulkEnabled || !cfg.MySQLHourlyEnabled || cfg.AnalyticsProgressiveEnabled || cfg.AnalyticsQueueSize != 0 || cfg.AnalyticsTimeout != time.Second {
		t.Fatalf("file configuration ignored: %+v", cfg)
	}
	t.Setenv("USAGE_MYSQL_ANALYTICS_ENABLED", "false")
	t.Setenv("USAGE_ANALYTICS_PROGRESSIVE_ENABLED", "true")
	t.Setenv("USAGE_ANALYTICS_MAX_CONCURRENT", "1")
	t.Setenv("USAGE_ANALYTICS_QUEUE_SIZE", "2")
	t.Setenv("USAGE_ANALYTICS_TIMEOUT_MS", "2000")
	t.Setenv("USAGE_ANALYTICS_ADMISSION_TIMEOUT_MS", "300")
	cfg, err = loadAnalyticsConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQLAnalyticsEnabled || !cfg.AnalyticsProgressiveEnabled || cfg.AnalyticsMaxConcurrent != 1 || cfg.AnalyticsQueueSize != 2 || cfg.AnalyticsTimeout != 2*time.Second || cfg.AnalyticsAdmissionTimeout != 300*time.Millisecond {
		t.Fatalf("env precedence ignored: %+v", cfg)
	}
}

func TestAnalyticsConfigurationRejectsInvalidValues(t *testing.T) {
	for _, key := range analyticsEnvKeys[:4] {
		t.Run(key, func(t *testing.T) {
			clearAnalyticsEnv(t)
			t.Setenv(key, "not-a-bool")
			if _, err := loadAnalyticsConfig(fileConfig{}); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	for _, tc := range []struct{ key, field, value string }{
		{"USAGE_ANALYTICS_TIMEOUT_MS", "analyticsTimeoutMs", "0"},
		{"USAGE_ANALYTICS_TIMEOUT_MS", "analyticsTimeoutMs", "120001"},
		{"USAGE_ANALYTICS_MAX_CONCURRENT", "analyticsMaxConcurrent", "0"},
		{"USAGE_ANALYTICS_MAX_CONCURRENT", "analyticsMaxConcurrent", "9"},
		{"USAGE_ANALYTICS_QUEUE_SIZE", "analyticsQueueSize", "-1"},
		{"USAGE_ANALYTICS_QUEUE_SIZE", "analyticsQueueSize", "65"},
		{"USAGE_ANALYTICS_ADMISSION_TIMEOUT_MS", "analyticsAdmissionTimeoutMs", "0"},
		{"USAGE_ANALYTICS_ADMISSION_TIMEOUT_MS", "analyticsAdmissionTimeoutMs", "30001"},
	} {
		t.Run(tc.field+tc.value, func(t *testing.T) {
			clearAnalyticsEnv(t)
			var file fileConfig
			if err := json.Unmarshal([]byte(`{"`+tc.field+`":`+tc.value+`}`), &file); err != nil {
				t.Fatal(err)
			}
			if _, err := loadAnalyticsConfig(file); err == nil {
				t.Fatal("accepted invalid JSON range")
			}
			t.Setenv(tc.key, tc.value)
			if _, err := loadAnalyticsConfig(fileConfig{}); err == nil {
				t.Fatal("accepted invalid env range")
			}
			t.Setenv(tc.key, "1.5")
			if _, err := loadAnalyticsConfig(fileConfig{}); err == nil {
				t.Fatal("accepted noninteger env")
			}
		})
	}
	clearAnalyticsEnv(t)
	t.Setenv("USAGE_ANALYTICS_TIMEOUT_MS", "100")
	if _, err := loadAnalyticsConfig(fileConfig{}); err == nil {
		t.Fatal("admission budget exceeded total budget")
	}
}
