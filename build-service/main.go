package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// --------------------------------------------------------------------------------
//  DATA MODELS
// --------------------------------------------------------------------------------

// BuildEvent is the Kafka message we RECEIVE from pipeline-service.
type BuildEvent struct {
	ID     string `json:"id"`
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// BuildResult is the Kafka message we PUBLISH back to pipeline-service.
// BuildLog is populated on failure — the AI worker uses it to diagnose the issue.
type BuildResult struct {
	ID       string `json:"id"`
	Status   string `json:"status"`   // "success" or "failed"
	BuildLog string `json:"build_log"` // real compiler output — empty on success
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
	fmt.Println("🔨 Starting build-service...")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topicTriggered,
		GroupID:     "build-service-group",
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6, // 10 MB
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicBuildCompleted,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	log.Printf("👂 Listening on Kafka topic '%s'...\n", topicTriggered)

	// Start Prometheus metrics server in background
	go startMetricsServer()

	// Main event loop — runs forever.
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Error reading from Kafka: %v\n", err)
			continue
		}

		var event BuildEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("❌ Failed to parse build event: %v\n", err)
			continue
		}

		log.Printf("📥 Received build job | pipeline: %s | repo: %s | commit: %s\n",
			event.ID, event.Repo, event.Commit)

		start := time.Now()
		finalStatus, buildLog := runBuild(event)
		duration := time.Since(start)

		result := BuildResult{
			ID:       event.ID,
			Status:   finalStatus,
			BuildLog: buildLog,
			Repo:     event.Repo,
			Commit:   event.Commit,
		}
		resultBytes, err := json.Marshal(result)
		if err != nil {
			log.Printf("❌ Failed to marshal build result: %v\n", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(result.ID),
			Value: resultBytes,
		})
		if err != nil {
			log.Printf("❌ Failed to publish result to Kafka: %v\n", err)
		} else {
			icon := "✅"
			if finalStatus == "failed" {
				icon = "❌"
			}
			log.Printf("%s Build finished for pipeline %s: %s (took %s)\n",
				icon, event.ID, finalStatus, duration.Round(time.Second))
		}

		// Record Prometheus metrics
		observeBuild(finalStatus, duration)
	}
}

// --------------------------------------------------------------------------------
//  REAL BUILD RUNNER
// --------------------------------------------------------------------------------

// runBuild clones the repo, checks out the exact commit, detects the language,
// and runs the real build command. Returns ("success","") or ("failed","<log>").
func runBuild(event BuildEvent) (string, string) {
	log.Printf("🔧 Starting real build | pipeline: %s | repo: %s | commit: %s\n",
		event.ID, event.Repo, event.Commit)

	// Step 1: Create a temporary working directory.
	// os.MkdirTemp creates a unique directory and cleans it up when we call
	// os.RemoveAll — this ensures no disk leaks between builds.
	tmpDir, err := os.MkdirTemp("", "shipit-build-*")
	if err != nil {
		msg := fmt.Sprintf("[shipit] Failed to create temp directory: %v", err)
		log.Println(msg)
		return "failed", msg
	}
	defer os.RemoveAll(tmpDir) // Always clean up, even if build fails

	// Step 2: Clone the repository.
	// We embed the token in the URL so git doesn't prompt for credentials.
	// Format: https://<token>@github.com/<owner>/<repo>.git
	token := os.Getenv("GITHUB_TOKEN")
	var cloneURL string
	if token != "" {
		cloneURL = fmt.Sprintf("https://%s@github.com/%s.git", token, event.Repo)
	} else {
		// Fall back to unauthenticated clone for public repos
		cloneURL = fmt.Sprintf("https://github.com/%s.git", event.Repo)
	}

	log.Printf("📦 Cloning %s...\n", event.Repo)
	cloneOut, err := runCmd(tmpDir, "git", "clone", "--quiet", cloneURL, ".")
	if err != nil {
		buildLog := fmt.Sprintf("[shipit] git clone failed\n\n%s\n%s", cloneOut, err)
		log.Printf("❌ Clone failed: %v\n", err)
		return "failed", buildLog
	}

	// Step 3: Check out the exact commit SHA.
	// This is critical — we must build the exact code that was pushed,
	// not just "whatever is on main right now".
	log.Printf("🔀 Checking out commit %s...\n", event.Commit)
	checkoutOut, err := runCmd(tmpDir, "git", "checkout", event.Commit)
	if err != nil {
		buildLog := fmt.Sprintf("[shipit] git checkout %s failed\n\n%s\n%s",
			event.Commit, checkoutOut, err)
		log.Printf("❌ Checkout failed: %v\n", err)
		return "failed", buildLog
	}

	// Step 4: Auto-detect the build language by looking for marker files.
	lang, buildCmd := detectLanguage(tmpDir)
	log.Printf("🔍 Detected language: %s | command: %s\n", lang, strings.Join(buildCmd, " "))

	// Step 5: Run the real build command and capture its output.
	log.Printf("🏗️  Running build: %s\n", strings.Join(buildCmd, " "))
	buildOutput, err := runCmd(tmpDir, buildCmd[0], buildCmd[1:]...)

	// Always show the build output in our own logs, even on success
	if buildOutput != "" {
		for _, line := range strings.Split(strings.TrimSpace(buildOutput), "\n") {
			log.Printf("  [build] %s\n", line)
		}
	}

	if err != nil {
		// Build failed — include the real compiler output in the result
		// so the AI worker has something concrete to analyze
		buildLog := fmt.Sprintf(
			"[shipit] Build failed for %s @ %s\n[shipit] Language: %s\n[shipit] Command: %s\n\n%s",
			event.Repo, event.Commit, lang, strings.Join(buildCmd, " "), buildOutput,
		)
		return "failed", buildLog
	}

	// Step 6: Build the Docker image for Kubernetes deployment
	log.Println("🐳 Building Docker image...")
	
	// If there's no Dockerfile, generate a simple one for Go apps
	dockerfilePath := filepath.Join(tmpDir, "Dockerfile")
	if _, err := os.Stat(dockerfilePath); os.IsNotExist(err) {
		log.Println("⚠️  No Dockerfile found. Generating a default Go Dockerfile.")
		defaultDockerfile := `FROM golang:1.23-alpine
WORKDIR /app
COPY . .
RUN go build -o main .
CMD ["./main"]
`
		os.WriteFile(dockerfilePath, []byte(defaultDockerfile), 0644)
	}

	imageTag := fmt.Sprintf("shipit-%s:%s", strings.ReplaceAll(strings.ToLower(event.Repo), "/", "-"), event.Commit)
	
	dockerOut, err := runCmd(tmpDir, "docker", "build", "-t", imageTag, ".")
	if err != nil {
		buildLog := fmt.Sprintf("[shipit] Docker build failed for %s @ %s\n\n%s\n%s", event.Repo, event.Commit, dockerOut, err)
		log.Printf("❌ Docker build failed: %v\nOutput: %s\n", err, dockerOut)
		return "failed", buildLog
	}

	log.Printf("✅ Docker image built: %s\n", imageTag)

	return "success", ""
}

// AIBuildRequest is sent to the ai-worker
type AIBuildRequest struct {
	FileTree     []string          `json:"file_tree"`
	FileContents map[string]string `json:"file_contents"`
}

// AIBuildResponse is received from the ai-worker
type AIBuildResponse struct {
	Language     string `json:"language"`
	BuildCommand string `json:"build_command"`
	TestCommand  string `json:"test_command"`
	RuntimeImage string `json:"runtime_image"`
	Confidence   string `json:"confidence"`
	Reasoning    string `json:"reasoning"`
	Error        string `json:"error"`
}

// detectLanguage asks the AI build planner for the exact build command.
// If the AI fails or returns low confidence, it falls back to basic file detection.
func detectLanguage(dir string) (string, []string) {
	log.Println("🧠 Asking AI build planner for configuration...")

	// 1. Gather file tree (top level only)
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("⚠️ Failed to read directory, using fallback: %v", err)
		return fallbackDetectLanguage(dir)
	}

	var tree []string
	contents := make(map[string]string)
	keyFiles := []string{
		"go.mod", "package.json", "requirements.txt", "Makefile",
		"Dockerfile", ".shipit.yml", "Cargo.toml", "pom.xml", "build.gradle",
	}

	for _, e := range entries {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			tree = append(tree, e.Name())
			
			// If it's a key file, read its contents
			for _, kf := range keyFiles {
				if e.Name() == kf {
					b, err := os.ReadFile(filepath.Join(dir, e.Name()))
					if err == nil {
						contents[e.Name()] = string(b)
					}
					break
				}
			}
		}
	}

	// 2. Call the AI Build Planner API
	reqBody, _ := json.Marshal(AIBuildRequest{
		FileTree:     tree,
		FileContents: contents,
	})

	// The ai-worker container is accessible at http://ai-worker:8000
	resp, err := http.Post("http://ai-worker:8000/analyze-build", "application/json", bytes.NewBuffer(reqBody))
	if err == nil {
		defer resp.Body.Close()
		
		if resp.StatusCode == 200 {
			var aiResp AIBuildResponse
			if err := json.NewDecoder(resp.Body).Decode(&aiResp); err == nil && aiResp.Error == "" {
				log.Printf("🤖 AI says: %s (confidence: %s) - %s", aiResp.Language, aiResp.Confidence, aiResp.Reasoning)
				if aiResp.BuildCommand != "" {
					// Split command string into args ("go build ./..." -> ["go", "build", "./..."])
					// Note: a real shell parser would be better here, but this is fine for basic commands
					return aiResp.Language, strings.Fields(aiResp.BuildCommand)
				}
			}
		}
	}
	
	log.Println("⚠️ AI planner failed or returned invalid response, falling back to basic detection.")
	return fallbackDetectLanguage(dir)
}

// fallbackDetectLanguage checks for well-known project marker files
func fallbackDetectLanguage(dir string) (string, []string) {
	// Check in priority order: Go → Node → Python → Make
	checks := []struct {
		marker  string
		lang    string
		command []string
	}{
		{"go.mod", "Go", []string{"go", "build", "./..."}},
		{"package.json", "Node.js", []string{"npm", "ci"}},
		{"requirements.txt", "Python", []string{"pip", "install", "-r", "requirements.txt"}},
		{"Makefile", "Make", []string{"make", "build"}},
	}

	for _, c := range checks {
		if _, err := os.Stat(filepath.Join(dir, c.marker)); err == nil {
			return c.lang, c.command
		}
	}

	// Nothing recognized — fail with a helpful message
	return "unknown", []string{
		"sh", "-c",
		"echo 'No supported build file found (go.mod, package.json, requirements.txt, Makefile)' && exit 1",
	}
}

// runCmd runs an external command in the given working directory,
// captures stdout+stderr combined, and returns them along with any error.
// This is a thin wrapper around os/exec to keep the build logic readable.
func runCmd(dir string, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir

	// Combine stdout and stderr into one buffer.
	// Real compilers mix both streams in their output (e.g. `go build`
	// prints errors to stderr but progress to stdout).
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	return out.String(), err
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
