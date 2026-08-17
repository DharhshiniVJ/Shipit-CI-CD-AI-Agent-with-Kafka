package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
)

// --------------------------------------------------------------------------------
//  GITHUB WEBHOOK HANDLER
// --------------------------------------------------------------------------------
//
// GitHub sends a POST request to /webhook every time you push to a repo that
// has ShipIt registered as a webhook. The payload contains the repo name,
// the commit SHA, and who pushed.
//
// Flow:
//   1. Validate the HMAC signature (so only GitHub can trigger builds)
//   2. Parse the push event payload
//   3. Call the same internal trigger logic as POST /trigger
//
// To set up in GitHub:
//   Repo → Settings → Webhooks → Add webhook
//   Payload URL: https://<ngrok-url>/webhook
//   Content type: application/json
//   Secret: same value as WEBHOOK_SECRET env var
//   Events: Just the push event

// GitHubPushEvent is the JSON payload GitHub sends on a push.
// We only extract the fields we need — GitHub sends many more.
type GitHubPushEvent struct {
	// Ref is the branch that was pushed to, e.g. "refs/heads/main"
	Ref string `json:"ref"`

	// After is the SHA of the HEAD commit after the push — this is what we build.
	After string `json:"after"`

	// Repository contains repo metadata
	Repository struct {
		FullName string `json:"full_name"` // e.g. "DharhshiniVJ/demo-app"
	} `json:"repository"`

	// Pusher is who triggered the push
	Pusher struct {
		Name string `json:"name"`
	} `json:"pusher"`

	// HeadCommit has the commit message (useful for logging)
	HeadCommit struct {
		Message string `json:"message"`
	} `json:"head_commit"`
}

// GitHubIssueCommentEvent is the JSON payload GitHub sends on an issue comment.
type GitHubIssueCommentEvent struct {
	Action  string `json:"action"` // e.g. "created"
	Issue   struct {
		Number int `json:"number"`
		Body   string `json:"body"`
	} `json:"issue"`
	Comment struct {
		Body string `json:"body"`
		User struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"comment"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}


// webhookHandler receives push events from GitHub and automatically
// triggers a pipeline for the pushed commit.
func webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Step 1: Read the raw body.
	// We need the raw bytes to validate the HMAC signature BEFORE parsing JSON.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Step 2: Validate the GitHub webhook signature.
	// GitHub signs every payload with HMAC-SHA256 using your webhook secret.
	// This prevents anyone on the internet from faking a push event.
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret != "" {
		sig := r.Header.Get("X-Hub-Signature-256")
		if !validSignature(body, sig, secret) {
			log.Println("⚠️  Webhook received with invalid signature — ignoring")
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Step 3: Handle events based on type
	eventType := r.Header.Get("X-GitHub-Event")
	
	if eventType == "issue_comment" {
		handleIssueCommentEvent(w, body)
		return
	}
	
	if eventType != "push" {
		// Acknowledge other events silently
		w.WriteHeader(http.StatusOK)
		log.Printf("ℹ️  Ignored GitHub event type: %s\n", eventType)
		return
	}

	// Step 4: Parse the push event
	var event GitHubPushEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// Step 5: Ignore pushes to non-branch refs (tags, etc.)
	if !strings.HasPrefix(event.Ref, "refs/heads/") {
		w.WriteHeader(http.StatusOK)
		log.Printf("ℹ️  Ignored push to non-branch ref: %s\n", event.Ref)
		return
	}

	// Ignore branch deletions (After = all zeros)
	if event.After == "" || strings.Trim(event.After, "0") == "" {
		w.WriteHeader(http.StatusOK)
		log.Printf("ℹ️  Ignored branch deletion event\n")
		return
	}

	branch := strings.TrimPrefix(event.Ref, "refs/heads/")
	log.Printf("📨 Webhook received | repo: %s | branch: %s | commit: %.8s | pusher: %s\n",
		event.Repository.FullName, branch, event.After, event.Pusher.Name)
	log.Printf("   Message: %s\n", firstLine(event.HeadCommit.Message))

	// Step 6: Create and trigger a pipeline using the shared helper
	run, err := createAndTriggerPipeline(event.Repository.FullName, event.After)
	if err != nil {
		log.Printf("❌ Failed to trigger pipeline from webhook: %v\n", err)
		http.Error(w, "failed to trigger pipeline", http.StatusInternalServerError)
		return
	}

	log.Printf("✅ Pipeline %s triggered automatically from GitHub push\n", run.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(run)
}

// handleIssueCommentEvent processes /approve comments to trigger deployments
func handleIssueCommentEvent(w http.ResponseWriter, body []byte) {
	var event GitHubIssueCommentEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if event.Action != "created" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Check if the comment contains /approve
	if !strings.Contains(strings.ToLower(event.Comment.Body), "/approve") {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Extract the pipeline ID from the issue body: <!-- pipeline: <id> -->
	re := regexp.MustCompile(`<!-- pipeline: ([a-f0-9-]+) -->`)
	match := re.FindStringSubmatch(event.Issue.Body)
	if len(match) < 2 {
		log.Println("⚠️  /approve comment received, but no pipeline ID found in issue body")
		w.WriteHeader(http.StatusOK)
		return
	}
	pipelineID := match[1]
	
	log.Printf("👍 Webhook received /approve for pipeline %s by %s", pipelineID, event.Comment.User.Login)

	// Approve the pipeline in the DB and trigger deploy-service
	err := approvePipeline(pipelineID, event.Comment.User.Login)
	if err != nil {
		log.Printf("❌ Failed to approve pipeline via webhook: %v", err)
		http.Error(w, "failed to approve", http.StatusInternalServerError)
		return
	}
	
	log.Printf("✅ Pipeline %s approved via GitHub Webhook!", pipelineID)

	// Auto-close the GitHub issue
	closeGitHubIssue(event.Repository.FullName, event.Issue.Number)

	w.WriteHeader(http.StatusOK)
}

func closeGitHubIssue(repo string, issueNumber int) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/issues/%d", repo, issueNumber)
	bodyBytes, _ := json.Marshal(map[string]string{"state": "closed"})

	req, _ := http.NewRequest("PATCH", url, bytes.NewBuffer(bodyBytes))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("❌ Failed to close GitHub issue: %v", err)
	} else if resp != nil {
		resp.Body.Close()
	}
}

// validSignature verifies the HMAC-SHA256 signature GitHub attaches to webhooks.
// GitHub sends: X-Hub-Signature-256: sha256=<hex digest>
func validSignature(body []byte, sig, secret string) bool {
	if !strings.HasPrefix(sig, "sha256=") {
		return false
	}
	expected := sig[len("sha256="):]

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	actual := hex.EncodeToString(mac.Sum(nil))

	// Use hmac.Equal for constant-time comparison (prevents timing attacks)
	return hmac.Equal([]byte(expected), []byte(actual))
}

// firstLine returns just the first line of a commit message.
func firstLine(s string) string {
	if i := strings.Index(s, "\n"); i >= 0 {
		return s[:i]
	}
	return s
}
