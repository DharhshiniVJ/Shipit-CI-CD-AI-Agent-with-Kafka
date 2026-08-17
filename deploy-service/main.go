package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"
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

// ApprovalEvent is the message we CONSUME from "approval.granted".
type ApprovalEvent struct {
	PipelineID string `json:"pipeline_id"`
	ApprovedBy string `json:"approved_by"`
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
	topicApprovalGranted = "approval.granted"  // We CONSUME from this
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
		log.Fatalf("â Œ Failed to create deployments table: %v", err)
	}
	log.Println("âœ… Table `deployments` ready")
}

// --------------------------------------------------------------------------------
//  MAIN
// --------------------------------------------------------------------------------

func main() {
	fmt.Println("🚀 Starting deploy-service...")

	initDB()
	defer db.Close()

	// Kafka producer — publishes deployment results
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicDeployCompleted,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	go consumeBuildResults(writer)
	go consumeApprovals(writer)
	go startMetricsServer()

	// Block forever
	select {}
}

func consumeBuildResults(writer *kafka.Writer) {
	// Kafka consumer — reads build results
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicBuildCompleted,
		GroupID: "deploy-service-group",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	log.Printf("👂 Listening on Kafka topic '%s'...\n", topicBuildCompleted)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Error reading from Kafka: %v\n", err)
			continue
		}

		var result BuildResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			log.Printf("❌ Failed to parse build result: %v\n", err)
			continue
		}

		log.Printf("📥 Received build result | pipeline: %s | status: %s\n",
			result.ID, result.Status)

		if result.Status != "success" {
			log.Printf("⏭️  Skipping deployment for pipeline %s (build %s)\n",
				result.ID, result.Status)

			// Still publish a "skipped" event so notify-service knows what happened
			publishDeployResult(writer, DeploymentResult{
				PipelineID: result.ID,
				DeployID:   generateUUID(),
				Status:     "skipped",
				Reason:     fmt.Sprintf("Build %s — deployment skipped", result.Status),
				DeployedAt: time.Now(),
			})
			continue
		}

		// Build succeeded — wait for QA approval!
		log.Printf("⏸️  Build %s succeeded. Waiting for QA approval before deployment.\n", result.ID)
		metricAwaitingApproval.Inc()
	}
}

func consumeApprovals(writer *kafka.Writer) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicApprovalGranted,
		GroupID: "deploy-service-approval-group",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	log.Printf("👂 Listening on Kafka topic '%s'...\n", topicApprovalGranted)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Error reading from Kafka: %v\n", err)
			continue
		}

		var event ApprovalEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("❌ Failed to parse approval event: %v\n", err)
			continue
		}

		log.Printf("✅ Received QA approval for pipeline: %s (by %s)\n", event.PipelineID, event.ApprovedBy)

		// Build succeeded and approved — run the deployment
		deployResult := runDeployment(event.PipelineID)

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
			log.Printf("❌ Failed to save deployment record: %v\n", err)
		}

		// Publish the deployment result to Kafka for notify-service
		publishDeployResult(writer, deployResult)

		// Record metrics: deployment done, one fewer item in approval queue
		metricDeploymentsTotal.WithLabelValues(deployResult.Status).Inc()
		metricAwaitingApproval.Dec()
	}
}

// runDeployment pulls the image from the local daemon, creates k8s manifests, and deploys using kubectl.
func runDeployment(pipelineID string) DeploymentResult {
	log.Printf("🚢 Deploying pipeline %s to Kubernetes...\n", pipelineID)
	
	// Query repo and commit from database
	var repo, commit string
	err := db.QueryRow("SELECT repo, commit FROM pipeline_runs WHERE id = $1", pipelineID).Scan(&repo, &commit)
	if err != nil {
		log.Printf("❌ Failed to query pipeline run for deployment: %v\n", err)
		return DeploymentResult{
			PipelineID: pipelineID,
			DeployID:   generateUUID(),
			Status:     "failed",
			Reason:     "Database error finding pipeline",
			DeployedAt: time.Now(),
		}
	}

	appName := strings.ReplaceAll(strings.ToLower(repo), "/", "-")
	imageTag := fmt.Sprintf("shipit-%s:%s", appName, commit)

	// Create temporary directory for k8s manifests
	tmpDir, err := os.MkdirTemp("", "deploy-*")
	if err != nil {
		log.Printf("❌ Failed to create tmp dir for deployment: %v\n", err)
		return DeploymentResult{
			PipelineID: pipelineID,
			DeployID:   generateUUID(),
			Status:     "failed",
			Reason:     "Filesystem error",
			DeployedAt: time.Now(),
		}
	}
	defer os.RemoveAll(tmpDir)

	manifestYAML := fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: web
        image: %s
        imagePullPolicy: Never
        ports:
        - containerPort: 8081
---
apiVersion: v1
kind: Service
metadata:
  name: %s-svc
spec:
  type: NodePort
  selector:
    app: %s
  ports:
  - port: 8081
    targetPort: 8081
    nodePort: 30081
`, appName, appName, appName, imageTag, appName, appName)

	manifestPath := filepath.Join(tmpDir, "deployment.yaml")
	os.WriteFile(manifestPath, []byte(manifestYAML), 0644)

	// Execute kubectl apply
	// We pass the server explicitly because inside the container 127.0.0.1 is the container itself,
	// but the kubeconfig mounted from Windows uses 127.0.0.1. kubernetes.docker.internal points to the host's K8s API.
	cmd := exec.Command("kubectl", "apply", "-f", manifestPath, "--server=https://kubernetes.docker.internal:6443", "--insecure-skip-tls-verify=true")
	cmd.Dir = tmpDir
	output, err := cmd.CombinedOutput()
	
	if err != nil {
		log.Printf("❌ kubectl apply failed: %v\nOutput: %s\n", err, string(output))
		return DeploymentResult{
			PipelineID: pipelineID,
			DeployID:   generateUUID(),
			Status:     "failed",
			Reason:     fmt.Sprintf("kubectl apply failed: %v", err),
			DeployedAt: time.Now(),
		}
	}

	log.Printf("✅ Deployment applied to Kubernetes for %s\n%s", repo, string(output))

	return DeploymentResult{
		PipelineID: pipelineID,
		DeployID:   generateUUID(),
		Status:     "deployed",
		Reason:     "Deployed to Kubernetes cluster",
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




