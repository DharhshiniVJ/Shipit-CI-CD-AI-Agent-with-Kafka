package main

import (
	"os"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// --------------------------------------------------------------------------------
//  DATA MODELS
// --------------------------------------------------------------------------------

// BuildResult is the message from "build.completed".
// We consume this to catch FAILED builds and notify immediately.
type BuildResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// DeploymentResult is the message from "deploy.completed".
// We consume this to notify about successful or skipped deployments.
type DeploymentResult struct {
	PipelineID string    `json:"pipeline_id"`
	DeployID   string    `json:"deploy_id"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
	DeployedAt time.Time `json:"deployed_at"`
}

// --------------------------------------------------------------------------------
//  KAFKA CONFIGURATION
// --------------------------------------------------------------------------------

var kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092")

const (
	topicBuildCompleted  = "build.completed"  // We CONSUME this (for failed builds)
	topicDeployCompleted = "deploy.completed" // We CONSUME this (for deployment results)
)

// --------------------------------------------------------------------------------
//  MAIN
// --------------------------------------------------------------------------------

func main() {
	fmt.Println("ðŸ”” Starting notify-service...")

	// notify-service is the END of the Kafka chain.
	// It only CONSUMES â€” it never produces to another topic.
	// It listens on TWO topics simultaneously using two goroutines.
	//
	// Why two topics--------------------------------------------------------------------------------
	//   - "build.completed" â†’ tells us about FAILED builds (deploy-service skips these,
	//     but we still want to notify the developer their build failed)
	//   - "deploy.completed" â†’ tells us about deployment outcomes (success or skipped)
	//
	// Go Idiom: sync.WaitGroup is used to wait for multiple goroutines to finish.
	// wg.Add(2) means "I'm launching 2 goroutines, wait for both".
	// wg.Wait() blocks main() until both goroutines call wg.Done().
	// Without this, main() would exit immediately and kill both goroutines.
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: listen for build failures
	go func() {
		defer wg.Done() // signals WaitGroup when this goroutine exits
		consumeBuildResults()
	}()

	// Goroutine 2: listen for deployment results
	go func() {
		defer wg.Done()
		consumeDeployResults()
	}()

	log.Println("👂 Listening on topics: build.completed + deploy.completed")

	// Start Prometheus metrics server in background
	go startMetricsServer()

	// Block until both goroutines finish (they never will — they loop forever)
	wg.Wait()
}

// --------------------------------------------------------------------------------
//  CONSUMERS
// --------------------------------------------------------------------------------

// consumeBuildResults listens for build results.
// We ONLY care about failed builds here â€” successful builds will be reported
// via "deploy.completed" after deploy-service processes them.
func consumeBuildResults() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicBuildCompleted,
		GroupID: "notify-service-build-group",
		StartOffset: kafka.FirstOffset, // unique GroupID for this consumer
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("âŒ Error reading from build.completed: %v\n", err)
			continue
		}

		var result BuildResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			log.Printf("âŒ Failed to parse build result: %v\n", err)
			continue
		}

		// Only send a notification for failed builds.
		// Successful builds will be notified via deploy.completed.
		if result.Status == "failed" {
			metricNotificationsSent.WithLabelValues("build_failed").Inc()
			sendNotification(
				"build_failed",
				result.ID,
				"âŒ Build Failed",
				fmt.Sprintf("Pipeline %s failed during the build step. The AI agent will analyze the logs and open a fix PR.", result.ID),
			)
		}
	}
}

// consumeDeployResults listens for deployment outcomes from deploy-service.
// This fires for BOTH successful deployments and skipped deployments.
func consumeDeployResults() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicDeployCompleted,
		GroupID: "notify-service-deploy-group",
		StartOffset: kafka.FirstOffset, // separate GroupID from above
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("âŒ Error reading from deploy.completed: %v\n", err)
			continue
		}

		var result DeploymentResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			log.Printf("âŒ Failed to parse deploy result: %v\n", err)
			continue
		}

		switch result.Status {
		case "deployed":
			sendNotification(
				"deploy_success",
				result.PipelineID,
				"ðŸš€ Deployment Successful",
				fmt.Sprintf("Pipeline %s was successfully deployed to staging at %s",
					result.PipelineID,
					result.DeployedAt.Local().Format("15:04:05"),
				),
			)
		case "skipped":
			// This happens when a build failed and deploy-service skipped it.
			// We don't need to notify again â€” consumeBuildResults already handled it.
		}
	}
}

// --------------------------------------------------------------------------------
//  NOTIFICATION SENDER
// --------------------------------------------------------------------------------

// sendNotification is where you'd integrate with real notification systems:
//   - Slack webhook
//   - Email (SMTP)
//   - PagerDuty
//   - GitHub PR comment
//
// For now, we print a formatted log to stdout.
// The structure is already in place to swap in a real integration later.
func sendNotification(eventType, pipelineID, title, message string) {
	timestamp := time.Now().Local().Format("15:04:05")

	fmt.Println()
	fmt.Println("  â”Œ--------------------------------------------------------------------------------")
	fmt.Printf("  â”‚ ðŸ”” NOTIFICATION [%s]\n", timestamp)
	fmt.Printf("  â”‚ Type:       %s\n", eventType)
	fmt.Printf("  â”‚ Pipeline:   %s\n", pipelineID)
	fmt.Printf("  â”‚ Title:      %s\n", title)
	fmt.Printf("  â”‚ Message:    %s\n", message)
	fmt.Println("  â””--------------------------------------------------------------------------------")
	fmt.Println()

	// ðŸ”Œ TODO: Replace the above with a real integration, e.g.:
	// sendSlackMessage(webhookURL, title, message)
	// sendEmail(to, subject, body)
	log.Printf("ðŸ“¨ Notification sent | type: %s | pipeline: %s\n", eventType, pipelineID)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}




