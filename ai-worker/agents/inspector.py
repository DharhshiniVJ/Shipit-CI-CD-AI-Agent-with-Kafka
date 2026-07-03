"""
inspector.py — The Inspector Agent

Role: First agent in the chain. Reads the raw build log, classifies the
failure type, and fetches ALL failing files from GitHub for context.

Analogy: A detective arriving at the crime scene. Their job is to collect
ALL evidence and write a structured report — not to solve the case yet.
"""

import os
import re
from langchain_openai import ChatOpenAI
from models import FailureClassification
from tools.github_tools import fetch_file


def get_llm():
    """
    Create a Cerebras-backed LLM using LangChain's OpenAI-compatible client.
    """
    return ChatOpenAI(
        api_key=os.getenv("CEREBRAS_API_KEY"),
        base_url="https://api.cerebras.ai/v1",
        model="gpt-oss-120b",
        temperature=0.1,
    )


def _extract_go_files_from_log(log: str) -> list[str]:
    """
    Parse a Go compiler build log and extract all unique .go file names
    that appear in error lines.

    Go error format: ./handler.go:31:16: undefined: handleReqest
    """
    pattern = re.compile(r'(?:\./)?([\w/]+\.go):\d+')
    files = []
    seen = set()
    for match in pattern.finditer(log):
        fname = match.group(1)
        if fname not in seen:
            files.append(fname)
            seen.add(fname)
    return files


def run_inspector(state: dict) -> dict:
    """
    Inspector node — reads the build log and classifies the failure.

    Improvements over v1:
    - Extracts ALL failing files from the build log (not just the first one)
    - Fetches each file's content from GitHub and includes them all in the prompt
    - Gives the LLM full context to produce a complete FailureClassification
    """
    print("\n🔍 [Inspector] Analyzing build log...")

    llm = get_llm()
    structured_llm = llm.with_structured_output(FailureClassification)

    repo = os.getenv("GITHUB_REPO", "")
    commit = state.get("commit", "main")
    log = state["build_log"]

    # Fetch ALL files mentioned in error lines from GitHub
    failing_files = _extract_go_files_from_log(log)
    github_context_parts = []

    if failing_files:
        print(f"   📂 Files referenced in build log: {', '.join(failing_files)}")
        for file_path in failing_files:
            try:
                content = fetch_file(repo, file_path, commit)
                github_context_parts.append(
                    f"FILE: {file_path} (fetched from GitHub @ {commit})\n"
                    f"```go\n{content[:4000]}\n```"
                )
                print(f"   ✅ Fetched {file_path} ({len(content)} chars)")
            except Exception as e:
                github_context_parts.append(
                    f"FILE: {file_path} — could not fetch: {e}"
                )
                print(f"   ⚠️  Could not fetch {file_path}: {e}")
    else:
        print("   ⚠️  No .go files found in build log — proceeding without file context")

    github_context = "\n\n".join(github_context_parts) if github_context_parts else "(No file context available)"

    prompt = f"""You are a CI/CD build failure inspector for a Go project.

Your job is to analyze the build log and the source files below, then produce
a structured failure report. Do NOT attempt to fix anything — just classify and report.

REPO: {state.get("repo", "unknown")}
COMMIT: {commit}

BUILD LOG:
{log}

SOURCE FILES FROM GITHUB:
{github_context}

Instructions:
1. Identify the failure_type (compilation_error, test_failure, config_error, etc.)
2. Write a clear error_summary
3. Set failed_file to the PRIMARY file causing the failure
4. Set all_failed_files to EVERY file that needs changes to fix the build
5. Extract every error line verbatim into error_lines
6. Describe where the fix should be applied in suggested_fix_area

Be thorough — a missed file at this stage will cause the fix to fail."""

    classification: FailureClassification = structured_llm.invoke(prompt)

    # Ensure all_failed_files always has at least the primary failed_file
    if classification.failed_file not in classification.all_failed_files:
        classification.all_failed_files.insert(0, classification.failed_file)

    print(f"   ✅ Classified as: {classification.failure_type}")
    print(f"   📄 Failed files: {', '.join(classification.all_failed_files)}")
    print(f"   💬 Summary: {classification.error_summary}")

    return {**state, "classification": classification, "critique_history": []}
