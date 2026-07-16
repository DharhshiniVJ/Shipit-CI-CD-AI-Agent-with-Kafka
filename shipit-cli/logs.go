package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// logsCmd builds and returns the "shipit logs" subcommand.
// Unlike `status` (one-shot), `logs` polls the API every 2 seconds
// and prints live updates until the pipeline reaches a terminal state.
func logsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "logs <pipeline-id>",
		Short: "Watch a pipeline run live until it completes",
		Long:  `Polls pipeline-service every 2 seconds and prints status changes in real time. Exits automatically when the build succeeds or fails.`,
		Example: `  shipit logs bc05e982-5748-7fdd-bd2d-140624742fda`,
		Args:    cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			id := args[0]

			fmt.Printf("👀 Watching pipeline %s...\n", id)
			fmt.Println("   (polls every 2 seconds — Ctrl+C to exit early)")


			lastStatus := "" // track the previous status so we only print changes

			// Poll in a loop until the pipeline reaches a terminal state.
			for {
				run, err := fetchStatus(id)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				// Only print a line when the status actually changes.
				// This avoids spamming the same "pending" line every 2 seconds.
				if run.Status != lastStatus {
					timestamp := time.Now().Local().Format("15:04:05") // e.g. "14:43:27"
					fmt.Printf("   [%s] %s %s\n", timestamp, statusEmoji(run.Status), run.Status)
					lastStatus = run.Status
				}

				// "success" and "failed" are terminal states — the build is done.
				// Break out of the loop and print the final summary.
				if run.Status == "success" || run.Status == "failed" {
					fmt.Println()
					if run.Status == "success" {
						fmt.Println("   ✅ Build completed successfully!")
					} else {
						fmt.Println("   ❌ Build failed.")
						fmt.Println("   💡 The AI agent system will analyze the failure and open a fix PR. (coming soon)")
					}
					fmt.Println()
					break
				}

				// Go Idiom: time.Sleep pauses the current goroutine for the specified duration.
				// This is how you add delays without blocking other goroutines.
				time.Sleep(2 * time.Second)
			}
		},
	}
}
