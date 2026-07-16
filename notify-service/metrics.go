package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Counter: notifications sent, labeled by type and outcome.
	metricNotificationsSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shipit_notifications_sent_total",
			Help: "Total notifications sent, labeled by event type (build_failed/deployed/skipped)",
		},
		[]string{"event_type"},
	)
)

func init() {
	prometheus.MustRegister(metricNotificationsSent)
}

func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Println("📊 Metrics server listening on :9100")
	if err := http.ListenAndServe(":9100", mux); err != nil {
		log.Fatalf("❌ Metrics server failed: %v", err)
	}
}
