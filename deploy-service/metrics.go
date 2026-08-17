package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Counter: deployments completed, labeled by status (deployed/skipped).
	metricDeploymentsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shipit_deployments_total",
			Help: "Total deployments processed, labeled by status (deployed/skipped)",
		},
		[]string{"status"},
	)

	// Gauge: builds currently waiting in the approval queue.
	// Incremented when a success build arrives; decremented when approval is granted.
	metricAwaitingApproval = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shipit_awaiting_approval_count",
			Help: "Number of successful builds currently waiting for QA approval",
		},
	)
)

func init() {
	prometheus.MustRegister(
		metricDeploymentsTotal,
		metricAwaitingApproval,
	)
}

func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Println("📊 Metrics server listening on :9100")
	if err := http.ListenAndServe(":9100", mux); err != nil {
		log.Fatalf("❌ Metrics server failed: %v", err)
	}
}
