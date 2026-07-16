package main

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

func approveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "approve <pipeline-id>",
		Short: "Approve a successful pipeline for deployment",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			pipelineID := args[0]
			fmt.Printf("⏸️  Approving pipeline %s for deployment...\n", pipelineID)

			resp, err := http.Post(fmt.Sprintf("%s/approve/%s", baseURL, pipelineID), "application/json", nil)
			if err != nil {
				fmt.Printf("❌ Error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				fmt.Printf("❌ Failed to approve: %s\n", string(body))
				os.Exit(1)
			}

			fmt.Println("✅ Pipeline approved! Deployment should begin shortly.")
		},
	}
}
