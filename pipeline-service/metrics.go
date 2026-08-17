package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --------------------------------------------------------------------------------
//  METRICS DEFINITIONS
// --------------------------------------------------------------------------------
// All metrics follow the naming convention: shipit_<service>_<what>_<unit>
// Labels let us slice the same counter by different dimensions (e.g. status="success").

var (
	// Counter: total pipeline runs triggered. Incremented every time POST /trigger succeeds.
	metricPipelinesTriggered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shipit_pipelines_triggered_total",
			Help: "Total number of CI/CD pipeline runs triggered via POST /trigger",
		},
		[]string{"repo"},
	)

	// Counter: total build results received (sliced by status).
	metricBuildsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shipit_builds_received_total",
			Help: "Total build results received from build-service (labeled by status)",
		},
		[]string{"status"},
	)

	// Gauge: pipelines currently sitting in the approval queue.
	metricAwaitingApproval = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shipit_awaiting_approval_count",
			Help: "Number of successful builds currently waiting for QA approval",
		},
	)

	// Histogram: HTTP request duration for all handlers.
	metricHTTPDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "shipit_http_request_duration_seconds",
			Help:    "HTTP request latency for pipeline-service endpoints",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"handler", "method", "status"},
	)
)

func init() {
	prometheus.MustRegister(
		metricPipelinesTriggered,
		metricBuildsReceived,
		metricAwaitingApproval,
		metricHTTPDuration,
	)
}

// startMetricsServer starts a lightweight HTTP server on :9100 that only
// serves the /metrics endpoint. This is separate from the main :8080 server
// so that Prometheus can scrape it independently.
func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Println("📊 Metrics server listening on :9100")
	if err := http.ListenAndServe(":9100", mux); err != nil {
		log.Fatalf("❌ Metrics server failed: %v", err)
	}
}
