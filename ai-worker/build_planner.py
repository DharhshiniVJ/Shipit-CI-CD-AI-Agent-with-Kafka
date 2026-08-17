"""
build_planner.py — AI-powered build configuration detector

Instead of guessing build commands from file names alone, this agent
reads the actual content of key project files and asks the LLM to
determine exactly how to build the project.

Examples of what this handles that file-based detection can't:
  - package.json with "build": "next build" vs "vite build" vs "tsc"
  - Monorepos with multiple packages (which one to build?)
  - Go projects where the main package is in a subdirectory
  - Projects that need environment setup before building
  - Makefile with multiple targets (which one is "build"?)
"""

import json
import os

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage


def get_llm():
    """Cerebras-backed LLM — same setup as the other agents."""
    return ChatOpenAI(
        api_key=os.getenv("CEREBRAS_API_KEY"),
        base_url="https://api.cerebras.ai/v1",
        model="gpt-oss-120b",
        temperature=0,  # deterministic for build config
    )


# Files we read and send to the LLM for analysis.
# We keep this list small to avoid hitting token limits.
KEY_FILES = [
    "go.mod",
    "go.sum",          # existence tells us it's a real Go module
    "package.json",
    "package-lock.json",
    "yarn.lock",
    "requirements.txt",
    "pyproject.toml",
    "setup.py",
    "Makefile",
    "Dockerfile",
    "README.md",
    ".shipit.yml",     # explicit ShipIt config takes highest priority
    "Cargo.toml",      # Rust
    "pom.xml",         # Java Maven
    "build.gradle",    # Java Gradle
]


def analyze_repo(file_tree: list[str], file_contents: dict[str, str]) -> dict:
    """
    Uses the LLM to determine how to build a repository.

    Args:
        file_tree: List of all file paths in the repo root (top-level only)
        file_contents: Dict of {filename: content} for key files that exist

    Returns:
        {
          "language": "Go",
          "build_command": "go build ./...",
          "test_command": "go test ./...",
          "runtime_image": "golang:1.23-alpine",
          "confidence": "high",
          "reasoning": "Found go.mod with module declaration"
        }
    """
    llm = get_llm()

    # Format context for the LLM
    tree_str = "\n".join(f"  {f}" for f in sorted(file_tree))

    files_str = ""
    for filename, content in file_contents.items():
        # Truncate large files to avoid token overload
        truncated = content[:2000] + "\n...(truncated)" if len(content) > 2000 else content
        files_str += f"\n--- {filename} ---\n{truncated}\n"

    prompt = f"""You are a CI/CD build configuration expert.

Analyze this repository and determine exactly how to build it.

## File tree (root level):
{tree_str}

## Key file contents:
{files_str}

## Your task:
Return a JSON object with EXACTLY these fields:
{{
  "language": "<primary language, e.g. Go, Node.js, Python, Rust>",
  "build_command": "<exact shell command to build, e.g. go build ./... or npm run build>",
  "test_command": "<exact shell command to test, or null if no tests>",
  "runtime_image": "<Docker image to use, e.g. golang:1.23-alpine or node:20-alpine>",
  "confidence": "<high|medium|low>",
  "reasoning": "<one sentence explaining your choice>"
}}

Rules:
- If .shipit.yml exists, use its build command
- For Go: use "go build ./..." unless main is in a subdirectory
- For Node.js: read package.json scripts to find the correct build command
- For Python: if there's no build step, use "python -m py_compile **/*.py"  
- runtime_image must be a real, publicly available Docker image
- Return ONLY valid JSON, no markdown, no explanation outside the JSON
"""

    response = llm.invoke([HumanMessage(content=prompt)])

    # Parse the JSON response
    raw = response.content.strip()

    # Strip markdown code fences if the LLM added them
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    return json.loads(raw.strip())
