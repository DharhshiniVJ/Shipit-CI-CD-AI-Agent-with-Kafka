"""
models.py — Pydantic schemas for structured agent outputs.

Why Pydantic?
  LLMs by default return free-form text. That's great for conversations
  but terrible for automation — you can't reliably parse a paragraph to
  find the file path and fixed code.

  By calling llm.with_structured_output(SomeModel), LangChain forces the
  LLM to respond ONLY with valid JSON that matches the schema. If the LLM
  tries to return something invalid, it retries automatically.
"""

from pydantic import BaseModel, Field
from typing import Optional


class FailureClassification(BaseModel):
    """
    Output from the Inspector agent.
    Classifies what TYPE of failure occurred and pulls key information from the log.
    """
    failure_type: str = Field(
        description="Type of failure: compilation_error, test_failure, config_error, dependency_error, or runtime_error"
    )
    error_summary: str = Field(
        description="One clear sentence summarising what went wrong"
    )
    failed_file: str = Field(
        description="The PRIMARY source file that caused the failure (e.g. handler.go)"
    )
    all_failed_files: list[str] = Field(
        description="ALL source files that need changes to fix the build. May be multiple files.",
        default_factory=list
    )
    error_lines: list[str] = Field(
        description="The exact error lines extracted from the build log"
    )
    suggested_fix_area: str = Field(
        description="Brief description of where in the code the fix should be applied"
    )


class RootCause(BaseModel):
    """
    Output from the Analyst agent.
    Diagnoses WHY the failure happened and how to approach fixing it.
    """
    diagnosis: str = Field(
        description="Detailed technical explanation of the root cause, covering ALL errors"
    )
    affected_component: str = Field(
        description="Which functions, structs, or modules are affected (all of them)"
    )
    fix_strategy: str = Field(
        description=(
            "Step-by-step strategy the Fixer should follow. "
            "Must address EVERY error in the build log. "
            "Each step should reference the exact file and line."
        )
    )
    files_to_fix: list[str] = Field(
        description="Ordered list of ALL files the Fixer must modify to resolve the build",
        default_factory=list
    )
    confidence: float = Field(
        description="Analyst's confidence in this diagnosis, between 0.0 and 1.0",
        ge=0.0,
        le=1.0
    )


class FileFix(BaseModel):
    """A fix for a single file."""
    file_path: str = Field(description="Relative path to the file (e.g. handler.go)")
    fixed_content: str = Field(description="The COMPLETE fixed file content — the whole file, not a diff")
    changes_explanation: str = Field(description="Exact lines changed and why")


class ProposedFix(BaseModel):
    """
    Output from the Fixer agent.
    Contains the actual code fixes across all necessary files and everything
    needed to open a GitHub PR.
    """
    file_path: str = Field(
        description="Primary file that was fixed (for backwards compat)"
    )
    fixed_content: str = Field(
        description="Fixed content of the PRIMARY file"
    )
    all_fixes: list[FileFix] = Field(
        description="Fixes for ALL files that need changing. Must include every file with errors.",
        default_factory=list
    )
    changes_explanation: str = Field(
        description="Clear explanation of ALL changes across ALL files and why each was made"
    )
    pr_title: str = Field(
        description="Concise PR title, e.g. 'fix: resolve compilation errors in handler.go and config.go'"
    )
    pr_description: str = Field(
        description="Full PR description in markdown explaining the bug and fix for each file"
    )


class Critique(BaseModel):
    """
    Output from the Critic agent.
    Validates the Fixer's proposed fix and decides if it's safe to open a PR.
    """
    is_confident: bool = Field(
        description="True if ALL build errors are resolved and the fix is safe to submit as a PR"
    )
    confidence_score: float = Field(
        description="Confidence score between 0.0 (wrong) and 1.0 (perfect)",
        ge=0.0,
        le=1.0
    )
    feedback: str = Field(
        description="Detailed review. If rejecting: be specific about exactly what is still wrong."
    )
    issues: list[str] = Field(
        description=(
            "List of SPECIFIC remaining problems. Each issue must name the file and the exact error. "
            "Empty list only if ALL errors are resolved."
        )
    )
    files_still_broken: list[str] = Field(
        description="Files that still have errors after the proposed fix. Empty if all are fixed.",
        default_factory=list
    )


class AgentState(dict):
    """
    The shared state that flows through the LangGraph nodes.

    Every node reads from this dict and returns a dict of updates.
    LangGraph merges the updates back into the state automatically.

    Think of it like a relay baton — each agent adds their findings
    and passes it to the next agent.
    """
    # Input (set at graph start)
    pipeline_id: str
    repo: str
    commit: str
    build_log: str

    # Agent outputs (filled in as graph runs)
    classification: Optional[FailureClassification]
    root_cause: Optional[RootCause]
    proposed_fix: Optional[ProposedFix]
    critique: Optional[Critique]

    # History — accumulated across retries so each loop has full context
    critique_history: list  # list of Critique objects from all previous iterations

    # Control flow
    iteration: int
    max_iterations: int

    # Final output
    pr_url: Optional[str]
    final_status: str   # "pr_opened", "max_iterations_reached", "error"
