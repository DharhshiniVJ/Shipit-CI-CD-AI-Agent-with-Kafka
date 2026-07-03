package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

// triggerCmd builds and returns the "shipit trigger" subcommand.
//
// Go Idiom: Returning *cobra.Command from a constructor function (rather than
// using a global variable) is the cleanest pattern for cobra commands.
// It makes each command self-contained and easy to test.
func triggerCmd() *cobra.Command {
	// These variables are bound to CLI flags below.
	// They're declared here so the Run function can close over them.
	var repo string
	var commit string

	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "Trigger a new pipeline run",
		Long:  `Sends a trigger request to pipeline-service, which creates a new PipelineRun and kicks off the build via Kafka.`,
		Example: `  shipit trigger --repo github.com/myorg/myapp --commit a1b2c3d4`,

		// Run is called when the user runs "shipit trigger ...".
		// cmd is the cobra command itself, args are any positional arguments.
		Run: func(cmd *cobra.Command, args []string) {
			// Validate required flags
			if repo == "" || commit == "" {
				fmt.Println("❌ Both --repo and --commit are required.")
				fmt.Println("   Example: shipit trigger --repo github.com/myorg/myapp --commit a1b2c3d4")
				os.Exit(1)
			}

			fmt.Printf("🚀 Triggering pipeline for %s @ %s...\n", repo, commit)

			// Build the JSON request body
			// map[string]string{"key": "value"} is a quick way to create a simple JSON object.
			payload, err := json.Marshal(map[string]string{
				"repo":   repo,
				"commit": commit,
			})
			if err != nil {
				fmt.Printf("❌ Failed to build request: %v\n", err)
				os.Exit(1)
			}

			// POST to pipeline-service
			// bytes.NewBuffer wraps our []byte payload so http.Post can stream it.
			resp, err := http.Post(
				baseURL+"/trigger",
				"application/json",
				bytes.NewBuffer(payload),
			)
			if err != nil {
				fmt.Printf("❌ Could not reach pipeline-service at %s\n", baseURL)
				fmt.Println("   Is pipeline-service running? Try: go run main.go (in pipeline-service/)")
				os.Exit(1)
			}
			defer resp.Body.Close()

			// Decode the response JSON into our local PipelineRun struct
			var run PipelineRun
			if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
				fmt.Printf("❌ Failed to parse response: %v\n", err)
				os.Exit(1)
			}

			// Pretty-print the result
			fmt.Println()
			fmt.Println("   Pipeline created successfully!")
			fmt.Println("   ┌─────────────────────────────────────────────────────")
			fmt.Printf("   │ ID:     %s\n", run.ID)
			fmt.Printf("   │ Repo:   %s\n", run.Repo)
			fmt.Printf("   │ Commit: %s\n", run.Commit)
			fmt.Printf("   │ Status: %s %s\n", statusEmoji(run.Status), run.Status)
			fmt.Println("   └─────────────────────────────────────────────────────")
			fmt.Println()
			fmt.Printf("   Watch it live:  shipit logs %s\n", run.ID)
			fmt.Printf("   Check status:   shipit status %s\n", run.ID)
		},
	}

	// Bind flags to our local variables.
	// StringVar(pointer, flagName, defaultValue, description)
	cmd.Flags().StringVar(&repo, "repo", "", "Repository URL, e.g. github.com/myorg/myapp (required)")
	cmd.Flags().StringVar(&commit, "commit", "", "Commit SHA to build (required)")

	return cmd
}
