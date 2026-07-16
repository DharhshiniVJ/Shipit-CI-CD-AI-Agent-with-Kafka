package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// PipelineRun mirrors the struct from pipeline-service.
// The CLI uses this to decode JSON responses from the API.
// Notice: the CLI doesn't import pipeline-service's code — it talks to it
// over HTTP and decodes the JSON response into this local copy of the struct.
// This is the microservices pattern: services are independent, they share data
// via APIs, not by sharing code.
type PipelineRun struct {
	ID        string    `json:"id"`
	Repo      string    `json:"repo"`
	Commit    string    `json:"commit"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// baseURL is where pipeline-service is running.
// In production this would be read from an env var or config file.
const baseURL = "http://localhost:8080"

func main() {
	// rootCmd is the top-level "shipit" command.
	// Every subcommand (trigger, status, logs) is registered as a child of this.
	rootCmd := &cobra.Command{
		Use:   "shipit",
		Short: "ShipIt — AI-Powered Developer Workflow CLI",
		Long: `
███████╗██╗  ██╗██╗██████╗ ██╗████████╗
██╔════╝██║  ██║██║██╔══██╗██║╚══██╔══╝
███████╗███████║██║██████╔╝██║   ██║   
╚════██║██╔══██║██║██╔═══╝ ██║   ██║   
███████║██║  ██║██║██║     ██║   ██║   
╚══════╝╚═╝  ╚═╝╚═╝╚═╝     ╚═╝   ╚═╝   

The CLI for the ShipIt CI/CD platform.
Trigger pipelines, check status, and watch builds in real time.`,
	}

	// Register each subcommand.
	// Each command is defined in its own file for clean separation.
	rootCmd.AddCommand(triggerCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(logsCmd())
	rootCmd.AddCommand(approveCmd())
	rootCmd.AddCommand(approvalsCmd())

	// Execute parses os.Args and runs the appropriate command.
	// If it fails (e.g., unknown flag), cobra prints the error and usage automatically.
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// statusEmoji returns a visual icon for each pipeline status.
// Shared across status.go and logs.go.
func statusEmoji(status string) string {
	switch status {
	case "pending":
		return "⏳"
	case "running":
		return "🔄"
	case "success":
		return "✅"
	case "failed":
		return "❌"
	default:
		return "❓"
	}
}
