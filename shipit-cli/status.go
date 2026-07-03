package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// statusCmd builds and returns the "shipit status" subcommand.
func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <pipeline-id>",
		Short: "Get the current status of a pipeline run",
		Long:  `Fetches the latest state of a pipeline run from pipeline-service by ID.`,
		Example: `  shipit status bc05e982-5748-7fdd-bd2d-140624742fda`,

		// cobra.ExactArgs(1) tells cobra to enforce exactly 1 positional argument.
		// If the user forgets to provide the ID, cobra prints a helpful error automatically.
		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			// args[0] is the first positional argument (the pipeline ID)
			id := args[0]

			run, err := fetchStatus(id)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// Format CreatedAt into a human-readable string
			created := run.CreatedAt.Local().Format(time.RFC822) // e.g. "02 Jul 26 14:43 IST"

			fmt.Println()
			fmt.Printf("   %s Pipeline %s\n", statusEmoji(run.Status), run.ID)
			fmt.Println("   ┌─────────────────────────────────────────────────────")
			fmt.Printf("   │ Repo:    %s\n", run.Repo)
			fmt.Printf("   │ Commit:  %s\n", run.Commit)
			fmt.Printf("   │ Status:  %s %s\n", statusEmoji(run.Status), run.Status)
			fmt.Printf("   │ Created: %s\n", created)
			fmt.Println("   └─────────────────────────────────────────────────────")
			fmt.Println()
		},
	}
}

// fetchStatus is a shared helper used by both status.go and logs.go.
// It makes a GET request to /status/{id} and returns the decoded PipelineRun.
//
// Go Idiom: Returning (value, error) is the standard Go pattern for functions
// that can fail. The caller decides what to do with the error.
func fetchStatus(id string) (*PipelineRun, error) {
	resp, err := http.Get(baseURL + "/status/" + id)
	if err != nil {
		return nil, fmt.Errorf("❌ Could not reach pipeline-service at %s\n   Is it running?", baseURL)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("❌ Pipeline '%s' not found", id)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("❌ Unexpected response from server: HTTP %d", resp.StatusCode)
	}

	var run PipelineRun
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		return nil, fmt.Errorf("❌ Failed to parse response: %v", err)
	}

	return &run, nil
}
