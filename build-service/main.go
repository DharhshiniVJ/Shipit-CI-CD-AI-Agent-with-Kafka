package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// --------------------------------------------------------------------------------
//  DATA MODELS
// --------------------------------------------------------------------------------

// BuildEvent is the Kafka message we RECEIVE from pipeline-service.
// It tells us which pipeline to build and for which repo/commit.
type BuildEvent struct {
	ID     string `json:"id"`
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// BuildResult is the Kafka message we PUBLISH back to pipeline-service.
// It carries the final outcome of the build.
// BuildLog is populated only on failure â€” the AI worker uses it to diagnose the issue.
type BuildResult struct {
	ID       string `json:"id"`
	Status   string `json:"status"`   // "success" or "failed"
	BuildLog string `json:"build_log"` // raw build output â€” empty on success
	Repo     string `json:"repo"`
	Commit   string `json:"commit"`
}

// --------------------------------------------------------------------------------
//  KAFKA CONFIGURATION
// --------------------------------------------------------------------------------

var kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092")

const (
	topicTriggered      = "pipeline.triggered" // We CONSUME from this topic
	topicBuildCompleted = "build.completed"     // We PRODUCE to this topic
)

// --------------------------------------------------------------------------------
//  MAIN
// --------------------------------------------------------------------------------

func main() {
	fmt.Println("ðŸ”¨ Starting build-service...")

	// -------------------------------------------------------------------------------- Kafka Consumer (Reader) --------------------------------------------------------------------------------
	// This reads incoming build jobs from the "pipeline.triggered" topic.
	// GroupID "build-service-group" means Kafka tracks our read position â€”
	// if this service restarts, it won't re-process already-handled messages.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicTriggered,
		GroupID: "build-service-group",
		StartOffset: kafka.FirstOffset,
		// MinBytes and MaxBytes control how much data to fetch per request.
		// These are sensible defaults for low-latency development.
		MinBytes: 1,
		MaxBytes: 10e6, // 10 MB
	})
	defer reader.Close()

	// -------------------------------------------------------------------------------- Kafka Producer (Writer) --------------------------------------------------------------------------------
	// This publishes build results back to pipeline-service via "build.completed".
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicBuildCompleted,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	log.Printf("ðŸ‘‚ Listening on Kafka topic '%s'...\n", topicTriggered)

	// -------------------------------------------------------------------------------- Main Event Loop --------------------------------------------------------------------------------
	// This runs forever. Each iteration processes one build job.
	//
	// Go Idiom: `for {}` without a condition is an infinite loop.
	// This is the standard pattern for long-running consumer services.
	for {
		// ReadMessage blocks (waits) until a new message arrives.
		// When pipeline-service triggers a build, it lands here.
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("âŒ Error reading from Kafka: %v\n", err)
			continue
		}

		// Decode the JSON message into a BuildEvent struct
		var event BuildEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("âŒ Failed to parse build event: %v\n", err)
			continue
		}

		log.Printf("ðŸ“¥ Received build job | pipeline: %s | repo: %s | commit: %s\n",
			event.ID, event.Repo, event.Commit)

		// Run the (simulated) build. Now returns both status and a log.
		finalStatus, buildLog := runBuild(event)

		// Publish the result back to Kafka so pipeline-service can update its store.
		// BuildLog is included so the ai-worker can analyze it on failure.
		result := BuildResult{
			ID:       event.ID,
			Status:   finalStatus,
			BuildLog: buildLog,
			Repo:     event.Repo,
			Commit:   event.Commit,
		}
		resultBytes, err := json.Marshal(result)
		if err != nil {
			log.Printf("âŒ Failed to marshal build result: %v\n", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(result.ID),
			Value: resultBytes,
		})
		if err != nil {
			log.Printf("âŒ Failed to publish result to Kafka: %v\n", err)
		} else {
			icon := "âœ…"
			if finalStatus == "failed" {
				icon = "âŒ"
			}
			log.Printf("%s Published build result for pipeline %s: %s\n", icon, event.ID, finalStatus)
		}
	}
}


// --------------------------------------------------------------------------------
//  BUILD SIMULATION
// --------------------------------------------------------------------------------

// runBuild simulates the actual build process for a given pipeline event.
// Returns the final status AND a build log (populated only on failure).
func runBuild(event BuildEvent) (string, string) {
	log.Printf("ðŸ”§ Building pipeline %s (repo: %s, commit: %s)...\n",
		event.ID, event.Repo, event.Commit)

	// Simulate build work taking 10 seconds
	time.Sleep(10 * time.Second)

	// Randomly decide success or failure using crypto/rand
	b := make([]byte, 1)
	rand.Read(b)

	if b[0]%2 == 0 {
		return "failed", generateFakeFailureLog(event)
	}
	return "success", ""
}

// generateFakeFailureLog produces a realistic-looking Go build error log.
// In a real system this would be captured stdout/stderr from `go build`.
// The AI agents use this log to understand what went wrong and generate a fix.
func generateFakeFailureLog(event BuildEvent) string {
	return fmt.Sprintf(`[shipit-build] Starting build for %s @ %s
[shipit-build] Cloning repository...
[shipit-build] Running: go build ./...

# %s
./handler.go:31:16: undefined: handleReqest
./handler.go:31:16: (did you mean handleRequest--------------------------------------------------------------------------------)
./config.go:14:2: imported and not used: "os"
note: module requires Go >= 1.20

FAIL\t%s [build failed]
exit status 2

[shipit-build] Build failed after 8.3s`, event.Repo, event.Commit, event.Repo, event.Repo)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}




