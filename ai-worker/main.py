"""
main.py — AI Worker Service entry point

This is the Kafka consumer that listens for failed builds and kicks off
the LangGraph multi-agent pipeline to diagnose and fix the failure.

Flow:
  1. Consume messages from "build.completed" topic
  2. Filter for status == "failed"
  3. Extract the build log from the message
  4. Run the LangGraph graph (Inspector → Analyst → Fixer → Critic)
  5. Log the PR URL if a fix was opened

This service is written in Python (not Go) because LangGraph, LangChain,
and Pydantic are Python-native. The Go services communicate with this
service only through Kafka — they never call each other directly.
"""

import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load environment variables from .env file
# This must happen BEFORE importing graph.py, since agents read env vars at import time
load_dotenv()

from graph import agent_graph  # noqa: E402 — imported after load_dotenv intentionally

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "build.completed"
GROUP_ID = "ai-worker-group"


def main():
    print("🤖 Starting ai-worker service...")
    print(f"   Model:  Cerebras llama-3.3-70b")
    print(f"   Kafka:  {KAFKA_BROKER}")
    print(f"   Topic:  {TOPIC}")
    print(f"   Repo:   {os.getenv('GITHUB_REPO', '(not set)')}")
    print()

    # Validate required environment variables early
    if not os.getenv("CEREBRAS_API_KEY"):
        print("❌ CEREBRAS_API_KEY not set. Copy .env.example to .env and fill it in.")
        return
    if not os.getenv("GITHUB_TOKEN"):
        print("⚠️  GITHUB_TOKEN not set. AI will analyze but cannot open PRs.")

    # Create the Kafka consumer
    # value_deserializer automatically parses JSON bytes into Python dicts
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",  # only process new messages (not old ones)
    )

    print(f"👂 Listening on Kafka topic '{TOPIC}'...")
    print("   (waiting for failed builds...)\n")

    # Main consumer loop — runs forever
    for message in consumer:
        event = message.value

        pipeline_id = event.get("id", "unknown")
        status = event.get("status", "")
        build_log = event.get("build_log", "")

        # Only process failed builds
        if status != "failed":
            continue

        print(f"\n{'='*60}")
        print(f"🚨 Build failure detected!")
        print(f"   Pipeline: {pipeline_id}")
        print(f"   Repo:     {event.get('repo', 'unknown')}")
        print(f"   Status:   {status}")
        print(f"{'='*60}")

        if not build_log:
            print("⚠️  No build log in event. Cannot analyze.")
            continue

        # Build the initial state for the LangGraph graph
        initial_state = {
            "pipeline_id": pipeline_id,
            "repo": event.get("repo", ""),
            "commit": event.get("commit", ""),
            "build_log": build_log,
            "classification": None,
            "root_cause": None,
            "proposed_fix": None,
            "critique": None,
            "iteration": 0,
            "max_iterations": 3,
            "pr_url": None,
            "final_status": "running",
        }

        try:
            # Run the graph — this blocks until all agents complete
            # (Inspector → Analyst → Fixer → Critic, with possible loops)
            print("\n🏃 Running LangGraph agent pipeline...\n")
            final_state = agent_graph.invoke(initial_state)

            # Report the outcome
            print(f"\n{'='*60}")
            print("📊 Agent Pipeline Complete")
            print(f"{'='*60}")

            if final_state.get("pr_url"):
                print(f"✅ PR opened successfully!")
                print(f"   🔗 {final_state['pr_url']}")
            else:
                print("ℹ️  No PR was opened (either fix was rejected or GitHub not configured)")

            if final_state.get("proposed_fix"):
                fix = final_state["proposed_fix"]
                print(f"\n📝 Proposed fix summary:")
                print(f"   File:    {fix.file_path}")
                print(f"   PR:      {fix.pr_title}")
                print(f"   Changes: {fix.changes_explanation[:100]}...")

            if final_state.get("critique"):
                critique = final_state["critique"]
                print(f"\n🎯 Final Critic score: {critique.confidence_score:.0%}")

        except Exception as e:
            print(f"\n❌ Agent pipeline failed with error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
