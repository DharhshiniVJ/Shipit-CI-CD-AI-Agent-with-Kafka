package main

import (
	"context"
	"crypto/rand"
	"database/sql" // Go's built-in database abstraction layer
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	// lib/pq is the PostgreSQL driver. We import it with a blank identifier (_)
	// because we never call it directly â€” it registers itself with database/sql
	// under the hood via an init() function. This is a common Go pattern.
	_ "github.com/lib/pq"
)

// --------------------------------------------------------------------------------
//  DATA MODELS
// --------------------------------------------------------------------------------

// PipelineRun represents a single execution of our CI/CD pipeline.
// These fields map 1-to-1 with columns in our `pipeline_runs` table.
type PipelineRun struct {
	ID        string    `json:"id"`
	Repo      string    `json:"repo"`
	Commit    string    `json:"commit"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// TriggerRequest is the expected JSON body for POST /trigger.
type TriggerRequest struct {
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// BuildEvent is the Kafka message we publish to "pipeline.triggered".
type BuildEvent struct {
	ID     string `json:"id"`
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// BuildResult is the Kafka message we receive from "build.completed".
type BuildResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// Approval represents an approval record in the database.
type Approval struct {
	PipelineID string    `json:"pipeline_id"`
	Status     string    `json:"status"`
	ApprovedBy string    `json:"approved_by"`
	ApprovedAt time.Time `json:"approved_at"`
}

// ApprovalEvent is the Kafka message we publish to "approval.granted".
type ApprovalEvent struct {
	PipelineID string `json:"pipeline_id"`
	ApprovedBy string `json:"approved_by"`
}

// --------------------------------------------------------------------------------
//  DATABASE
// --------------------------------------------------------------------------------

// db is our global database connection pool.
//
// Go Idiom: *sql.DB is NOT a single connection â€” it's a managed pool of
// connections. Go automatically opens/closes/reuses connections as needed.
// It is safe to use from multiple goroutines concurrently.
var db *sql.DB

// initDB connects to PostgreSQL and creates the pipeline_runs table
// if it doesn't already exist. This is called once at startup.
func initDB() {
	// The connection string tells Go's sql package how to reach Postgres.
	// Format: postgres://user:password@host:port/dbname--------------------------------------------------------------------------------options
	dbHost := "localhost"
	if envHost := os.Getenv("DB_HOST"); envHost != "" {
		dbHost = envHost
	}
	connStr := fmt.Sprintf("postgres://shipit:shipit@%s:5432/pipeline_db?sslmode=disable", dbHost)

	var err error
	// sql.Open does NOT actually connect yet â€” it just validates the config.
	// The real connection happens on the first query (lazy connection).
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("âŒ Failed to open database: %v", err)
	}

	// db.Ping() actually tests the connection. This is where we find out
	// if Postgres is reachable and our credentials are correct.
	if err = db.Ping(); err != nil {
		log.Fatalf("âŒ Cannot connect to PostgreSQL: %v\n\nIs postgres running-------------------------------------------------------------------------------- Try: docker-compose up -d", err)
	}

	log.Println("âœ… Connected to PostgreSQL")

	// Auto-create the table on startup if it doesn't exist yet.
	// "IF NOT EXISTS" makes this safe to run every time â€” it's idempotent.
	// In production you'd use a proper migration tool (golang-migrate, goose, etc.)
	createTable := `
	CREATE TABLE IF NOT EXISTS pipeline_runs (
		id         TEXT PRIMARY KEY,
		repo       TEXT NOT NULL,
		commit     TEXT NOT NULL,
		status     TEXT NOT NULL DEFAULT 'pending',
		created_at TIMESTAMPTZ NOT NULL
	)`

	// Exec runs a SQL statement that doesn't return rows (CREATE, INSERT, UPDATE, DELETE).
	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("❌ Failed to create table: %v", err)
	}

	createApprovalsTable := `
	CREATE TABLE IF NOT EXISTS approvals (
		pipeline_id TEXT PRIMARY KEY,
		status      TEXT NOT NULL DEFAULT 'pending',
		approved_by TEXT,
		approved_at TIMESTAMPTZ
	)`

	if _, err := db.Exec(createApprovalsTable); err != nil {
		log.Fatalf("❌ Failed to create approvals table: %v", err)
	}

	log.Println("✅ Tables `pipeline_runs` and `approvals` ready")
}

// --------------------------------------------------------------------------------
//  KAFKA CONFIGURATION
// --------------------------------------------------------------------------------

var kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092")

const (
	topicTriggered       = "pipeline.triggered"
	topicBuildCompleted  = "build.completed"
	topicDeployCompleted = "deploy.completed"
	topicApprovalGranted = "approval.granted"
)

var kafkaWriter *kafka.Writer
var kafkaApprovalWriter *kafka.Writer

// --------------------------------------------------------------------------------
//  MAIN
// --------------------------------------------------------------------------------

func main() {
	// Step 1: Connect to the database and ensure the schema exists.
	initDB()
	defer db.Close()

	// Step 2: Set up the Kafka producer.
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicTriggered,
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	kafkaApprovalWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicApprovalGranted,
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaApprovalWriter.Close()

	// Step 3: Start the Kafka consumer in the background.
	go consumeBuildResults()

	// Step 4: Register HTTP routes and start the server.
	http.HandleFunc("/trigger", triggerHandler)
	http.HandleFunc("/status/", statusHandler)
	http.HandleFunc("/approve/", approveHandler)
	http.HandleFunc("/approvals", listApprovalsHandler)

	fmt.Println("🚀 Starting pipeline-service on http://localhost:8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// --------------------------------------------------------------------------------
//  HTTP HANDLERS
// --------------------------------------------------------------------------------

// triggerHandler handles POST /trigger.
// Creates a pipeline run in PostgreSQL and publishes a Kafka event.
func triggerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	run := PipelineRun{
		ID:        generateUUID(),
		Repo:      req.Repo,
		Commit:    req.Commit,
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	// -------------------------------------------------------------------------------- INSERT into PostgreSQL --------------------------------------------------------------------------------
	// $1, $2, $3... are positional placeholders — PostgreSQL's way of safely
	// injecting values. This prevents SQL injection attacks.
	// Never use fmt.Sprintf to build SQL strings!
	insertSQL := `
		INSERT INTO pipeline_runs (id, repo, commit, status, created_at)
		VALUES ($1, $2, $3, $4, $5)`

	_, err := db.Exec(insertSQL, run.ID, run.Repo, run.Commit, run.Status, run.CreatedAt)
	if err != nil {
		log.Printf("❌ Failed to insert pipeline run: %v\n", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	// --------------------------------------------------------------------------------

	log.Printf("▶️   Pipeline %s triggered | repo: %s | commit: %s\n", run.ID, run.Repo, run.Commit)

	// -------------------------------------------------------------------------------- Publish to Kafka --------------------------------------------------------------------------------
	event := BuildEvent{ID: run.ID, Repo: run.Repo, Commit: run.Commit}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("❌ Failed to marshal Kafka event: %v\n", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(run.ID),
		Value: eventBytes,
	})
	if err != nil {
		log.Printf("❌ Failed to publish to Kafka: %v\n", err)
	} else {
		log.Printf("📧 Published event to Kafka topic '%s'\n", topicTriggered)
	}
	// --------------------------------------------------------------------------------

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(run)
}

// statusHandler handles GET /status/{id}.
// Fetches the pipeline run directly from PostgreSQL.
func statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/status/")
	if id == "" {
		http.Error(w, "Missing pipeline ID", http.StatusBadRequest)
		return
	}

	// -------------------------------------------------------------------------------- SELECT from PostgreSQL --------------------------------------------------------------------------------
	// QueryRow returns exactly one row. We scan its columns into our struct.
	selectSQL := `SELECT id, repo, commit, status, created_at FROM pipeline_runs WHERE id = $1`

	var run PipelineRun
	// row.Scan() reads each column value from the result into the variables we provide.
	// The order of &run.ID, &run.Repo... must match the SELECT column order above.
	err := db.QueryRow(selectSQL, id).Scan(
		&run.ID,
		&run.Repo,
		&run.Commit,
		&run.Status,
		&run.CreatedAt,
	)
	// --------------------------------------------------------------------------------

	if err == sql.ErrNoRows {
		// sql.ErrNoRows is the standard "not found" sentinel in database/sql.
		http.Error(w, "Pipeline not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("❌ Database query error: %v\n", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

// approveHandler handles POST /approve/{id}.
// Marks a pipeline as approved and publishes an approval event.
func approveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/approve/")
	if id == "" {
		http.Error(w, "Missing pipeline ID", http.StatusBadRequest)
		return
	}

	updateSQL := `
		UPDATE approvals 
		SET status = 'approved', approved_by = 'qa-team', approved_at = $1 
		WHERE pipeline_id = $2 AND status = 'pending'
		RETURNING pipeline_id`
	
	var returnedID string
	err := db.QueryRow(updateSQL, time.Now(), id).Scan(&returnedID)
	if err == sql.ErrNoRows {
		http.Error(w, "Pipeline not found or not pending approval", http.StatusNotFound)
		return
	} else if err != nil {
		log.Printf("❌ Failed to update approval: %v\n", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Publish to Kafka
	event := ApprovalEvent{PipelineID: id, ApprovedBy: "qa-team"}
	eventBytes, _ := json.Marshal(event)
	
	err = kafkaApprovalWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(id),
		Value: eventBytes,
	})
	if err != nil {
		log.Printf("❌ Failed to publish approval to Kafka: %v\n", err)
	} else {
		log.Printf("📫 Published approval for pipeline %s to Kafka\n", id)
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message": "Pipeline %s approved"}`, id)
}

// listApprovalsHandler handles GET /approvals
func listApprovalsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	selectSQL := `SELECT pipeline_id, status, COALESCE(approved_by, ''), COALESCE(approved_at, '1970-01-01T00:00:00Z'::timestamptz) FROM approvals WHERE status = 'pending'`
	rows, err := db.Query(selectSQL)
	if err != nil {
		log.Printf("❌ Failed to query approvals: %v\n", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	approvals := []Approval{}
	for rows.Next() {
		var a Approval
		if err := rows.Scan(&a.PipelineID, &a.Status, &a.ApprovedBy, &a.ApprovedAt); err != nil {
			log.Printf("❌ Failed to scan approval: %v\n", err)
			continue
		}
		approvals = append(approvals, a)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(approvals)
}

// --------------------------------------------------------------------------------
//  KAFKA CONSUMER
// --------------------------------------------------------------------------------

// consumeBuildResults listens on "build.completed" and updates the
// pipeline's status in PostgreSQL when build-service finishes a build.
func consumeBuildResults() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicBuildCompleted,
		GroupID: "pipeline-service-group",
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

		// -------------------------------------------------------------------------------- UPDATE in PostgreSQL --------------------------------------------------------------------------------
		// This is durable — even if the service restarts after this line,
		// the status is safely stored on disk in Postgres.
		updateSQL := `UPDATE pipeline_runs SET status = $1 WHERE id = $2`
		res, err := db.Exec(updateSQL, result.Status, result.ID)
		if err != nil {
			log.Printf("❌ Failed to update pipeline status: %v\n", err)
			continue
		}

		// RowsAffected tells us how many rows were changed.
		// 0 means the ID wasn't found — something is wrong.
		rows, _ := res.RowsAffected()
		if rows == 0 {
			log.Printf("⚠️   No pipeline found with ID %s to update\n", result.ID)
			continue
		}
		// --------------------------------------------------------------------------------

		icon := "✅"
		if result.Status == "failed" {
			icon = "❌"
		}
		log.Printf("%s Pipeline %s updated to status: %s\n", icon, result.ID, result.Status)

		// If success, insert into approvals table so it can be approved
		if result.Status == "success" {
			insertApprovalSQL := `INSERT INTO approvals (pipeline_id, status) VALUES ($1, 'pending') ON CONFLICT DO NOTHING`
			_, err = db.Exec(insertApprovalSQL, result.ID)
			if err != nil {
				log.Printf("❌ Failed to insert into approvals: %v\n", err)
			} else {
				log.Printf("⏸️  Pipeline %s is awaiting deployment approval\n", result.ID)
			}
		}
	}
}

// --------------------------------------------------------------------------------
//  HELPERS
// --------------------------------------------------------------------------------

// generateUUID creates a random UUID-like string using crypto/rand.
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




