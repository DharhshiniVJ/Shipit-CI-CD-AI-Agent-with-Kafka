package main

import (
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Counter: total builds processed, labeled by outcome.
	metricBuildsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shipit_builds_total",
			Help: "Total number of builds completed, labeled by status (success/failed)",
		},
		[]string{"status"},
	)

	// Histogram: how long each build took to simulate.
	metricBuildDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "shipit_build_duration_seconds",
			Help:    "Time taken to complete a build (simulated)",
			Buckets: []float64{1, 2, 3, 5, 8, 13, 21}, // build times in seconds
		},
	)
)

func init() {
	prometheus.MustRegister(
		metricBuildsTotal,
		metricBuildDuration,
	)
}

// observeBuild records build outcome and duration into Prometheus metrics.
// Call this immediately after a build completes.
func observeBuild(status string, duration time.Duration) {
	metricBuildsTotal.WithLabelValues(status).Inc()
	metricBuildDuration.Observe(duration.Seconds())
}

func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Println("📊 Metrics server listening on :9100")
	if err := http.ListenAndServe(":9100", mux); err != nil {
		log.Fatalf("❌ Metrics server failed: %v", err)
	}
}
