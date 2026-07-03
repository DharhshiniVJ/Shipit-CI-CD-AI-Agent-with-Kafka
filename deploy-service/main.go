package main

import (
	"os"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/lib/pq"
)

// --------------------------------------------------------------------------------
//  DATA MODELS
// --------------------------------------------------------------------------------

// BuildResult is the message we CONSUME from "build.completed".
// Published by build-service when it finishes.
type BuildResult struct {
	ID     string `json:"id"`
	Status string `json:"status"` // "success" or "failed"
}

// DeploymentResult is the message we PRODUCE to "deploy.completed".
// notify-service will consume this to send the final notification.
type DeploymentResult struct {
	PipelineID string    `json:"pipeline_id"`
	DeployID   string    `json:"deploy_id"`
	Status     string    `json:"status"`     // "deployed" or "skipped"
	Reason     string    `json:"reason"`     // human-readable explanation
	DeployedAt time.Time `json:"deployed_at"`
}

// --------------------------------------------------------------------------------
//  KAFKA CONFIGURATION
// --------------------------------------------------------------------------------

var kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092")

const (
	topicBuildCompleted  = "build.completed"   // We CONSUME from this
	topicDeployCompleted = "deploy.completed"  // We PRODUCE to this
)

// --------------------------------------------------------------------------------
//  DATABASE
// --------------------------------------------------------------------------------

var db *sql.DB

// initDB connects to PostgreSQL and creates the deployments table.
// Note: In production, each microservice would have its OWN database instance.
// Here we use the same PostgreSQL server but a separate table to keep things simple.
func initDB() {
	// We reuse the same pipeline_db for simplicity in this learning project.
	// In production: deploy-service would have deploy_db on its own Postgres instance.
	dbHost := getEnv("DB_HOST", "localhost")
	connStr := fmt.Sprintf("postgres://shipit:shipit@%s:5432/pipeline_db?sslmode=disable", dbHost)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("âŒ Failed to open database: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("âŒ Cannot connect to PostgreSQL: %v\n   Is docker-compose up--------------------------------------------------------------------------------", err)
	}
	log.Println("âœ… Connected to PostgreSQL")

	// Create the deployments table if it doesn't exist.
	// Each row represents one deployment attempt for a pipeline run.
	createTable := `
	CREATE TABLE IF NOT EXISTS deployments (
		id          TEXT PRIMARY KEY,
		pipeline_id TEXT NOT NULL,
		status      TEXT NOT NULL,
		reason      TEXT NOT NULL,
		deployed_at TIMESTAMPTZ NOT NULL
	)`

	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("âŒ Failed to create deployments table: %v", err)
	}
	log.Println("âœ… Table `deployments` ready")
}

// --------------------------------------------------------------------------------
//  MAIN
// --------------------------------------------------------------------------------

func main() {
	fmt.Println("ðŸš€ Starting deploy-service...")

	initDB()
	defer db.Close()

	// Kafka consumer â€” reads build results
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicBuildCompleted,
		// Each service that consumes "build.completed" needs its OWN unique GroupID.
		// Kafka uses GroupID to track which messages each consumer has already read.
		// If deploy-service and pipeline-service shared a GroupID, they'd split
		// messages between them â€” each message would only go to ONE of them!
		// With different GroupIDs, BOTH services get EVERY message independently.
		GroupID: "deploy-service-group",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	// Kafka producer â€” publishes deployment results
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicDeployCompleted,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	log.Printf("ðŸ‘‚ Listening on Kafka topic '%s'...\n", topicBuildCompleted)

	// Main event loop
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("âŒ Error reading from Kafka: %v\n", err)
			continue
		}

		var result BuildResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			log.Printf("âŒ Failed to parse build result: %v\n", err)
			continue
		}

		log.Printf("ðŸ“¥ Received build result | pipeline: %s | status: %s\n",
			result.ID, result.Status)

		// -------------------------------------------------------------------------------- Conditional Logic â€” only deploy successful builds --------------------------------------------------------------------------------
		// This is a key pattern in event-driven systems: consumers FILTER events.
		// deploy-service doesn't blindly process every event â€” it checks the
		// status and decides whether to act.
		if result.Status != "success" {
			log.Printf("â­ï¸  Skipping deployment for pipeline %s (build %s)\n",
				result.ID, result.Status)

			// Still publish a "skipped" event so notify-service knows what happened
			publishDeployResult(writer, DeploymentResult{
				PipelineID: result.ID,
				DeployID:   generateUUID(),
				Status:     "skipped",
				Reason:     fmt.Sprintf("Build %s â€” deployment skipped", result.Status),
				DeployedAt: time.Now(),
			})
			continue
		}

		// Build succeeded â€” run the deployment
		deployResult := runDeployment(result.ID)

		// Save deployment record to PostgreSQL
		_, err = db.Exec(
			`INSERT INTO deployments (id, pipeline_id, status, reason, deployed_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			deployResult.DeployID,
			deployResult.PipelineID,
			deployResult.Status,
			deployResult.Reason,
			deployResult.DeployedAt,
		)
		if err != nil {
			log.Printf("âŒ Failed to save deployment record: %v\n", err)
		}

		// Publish the deployment result to Kafka for notify-service
		publishDeployResult(writer, deployResult)
	}
}

// runDeployment simulates deploying the application.
// In a real system this would:
//   - Pull the Docker image for the commit
//   - Apply Kubernetes manifests (kubectl apply)
//   - Wait for the rollout to complete
//   - Run smoke tests against the new deployment
func runDeployment(pipelineID string) DeploymentResult {
	log.Printf("ðŸš¢ Deploying pipeline %s to staging...\n", pipelineID)
	time.Sleep(5 * time.Second) // simulate deployment work

	return DeploymentResult{
		PipelineID: pipelineID,
		DeployID:   generateUUID(),
		Status:     "deployed",
		Reason:     "Deployment to staging successful",
		DeployedAt: time.Now(),
	}
}

// publishDeployResult marshals a DeploymentResult and writes it to Kafka.
func publishDeployResult(writer *kafka.Writer, result DeploymentResult) {
	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("âŒ Failed to marshal deploy result: %v\n", err)
		return
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(result.PipelineID),
		Value: resultBytes,
	})
	if err != nil {
		log.Printf("âŒ Failed to publish deploy result to Kafka: %v\n", err)
	} else {
		icon := "âœ…"
		if result.Status != "deployed" {
			icon = "â­ï¸"
		}
		log.Printf("%s Published deploy result for pipeline %s: %s\n",
			icon, result.PipelineID, result.Status)
	}
}

// generateUUID creates a random UUID-like string.
func generateUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal("Failed to generate UUID:", err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}




