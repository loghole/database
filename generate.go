package database

// nolint:lll // generate.
//go:generate mockgen --build_flags=--mod=mod -destination mocks/metrics.go -package mocks github.com/loghole/database/hooks MetricCollector
