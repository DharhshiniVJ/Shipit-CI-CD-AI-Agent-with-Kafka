package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// Approval mirrors the struct from pipeline-service
type Approval struct {
	PipelineID string    `json:"pipeline_id"`
	Status     string    `json:"status"`
	ApprovedBy string    `json:"approved_by"`
	ApprovedAt time.Time `json:"approved_at"`
}

func approvalsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "approvals",
		Short: "List pipelines awaiting QA approval",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := http.Get(fmt.Sprintf("%s/approvals", baseURL))
			if err != nil {
				fmt.Printf("❌ Error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("❌ API returned status %d\n", resp.StatusCode)
				os.Exit(1)
			}

			var approvals []Approval
			if err := json.NewDecoder(resp.Body).Decode(&approvals); err != nil {
				fmt.Printf("❌ Failed to parse response: %v\n", err)
				os.Exit(1)
			}

			if len(approvals) == 0 {
				fmt.Println("🎉 No pipelines awaiting approval!")
				return
			}

			fmt.Printf("📋 %d pipelines awaiting QA approval:\n\n", len(approvals))
			
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "PIPELINE ID\tSTATUS\tWAITING SINCE")
			for _, a := range approvals {
				fmt.Fprintf(w, "%s\t%s\t%s\n", a.PipelineID, a.Status, "now")
			}
			w.Flush()
			
			fmt.Println("\nRun `shipit approve <pipeline-id>` to deploy.")
		},
	}
}
