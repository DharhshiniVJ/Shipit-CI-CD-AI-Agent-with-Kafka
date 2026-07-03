"""
analyst.py — The Analyst Agent

Role: Takes the Inspector's structured report and diagnoses the ROOT CAUSE
across ALL failing files. Also receives accumulated Critic feedback from
every previous retry so it can learn from its mistakes.

Analogy: A forensic scientist who reads all the detective's evidence AND
all the previous lab reports that came back wrong, then figures out the
complete picture of why the crime happened.
"""

import os
from langchain_openai import ChatOpenAI
from models import FailureClassification, RootCause, Critique


def get_llm():
    return ChatOpenAI(
        api_key=os.getenv("CEREBRAS_API_KEY"),
        base_url="https://api.cerebras.ai/v1",
        model="gpt-oss-120b",
        temperature=0.1,
    )


def run_analyst(state: dict) -> dict:
    """
    Analyst node — diagnoses root cause and plans the complete fix strategy.

    Improvements over v1:
    - Includes the FULL history of all previous Critic rejections (not just the last)
    - Prompt explicitly asks for a per-file fix strategy
    - Tracks which files are still broken according to the Critic
    """
    iteration = state.get("iteration", 0)
    print(f"\n🧠 [Analyst] Diagnosing root cause (attempt {iteration + 1})...")

    llm = get_llm()
    structured_llm = llm.with_structured_output(RootCause)

    classification: FailureClassification = state["classification"]

    # Build accumulated critic history section — includes ALL past rejections
    critique_history: list[Critique] = state.get("critique_history", [])
    history_section = ""
    if critique_history:
        history_parts = []
        for i, past_critique in enumerate(critique_history, 1):
            issues_text = "\n".join(f"    - {issue}" for issue in past_critique.issues)
            still_broken = (
                ", ".join(past_critique.files_still_broken)
                if past_critique.files_still_broken
                else "unknown"
            )
            history_parts.append(
                f"--- Attempt {i} ---\n"
                f"Critic verdict: REJECTED (confidence: {past_critique.confidence_score:.0%})\n"
                f"Feedback: {past_critique.feedback}\n"
                f"Specific issues:\n{issues_text}\n"
                f"Files still broken: {still_broken}"
            )
        history_section = f"""
⚠️  PREVIOUS FIX ATTEMPTS WERE REJECTED BY THE CRITIC
This is attempt {iteration + 1}. Study the history below carefully and do NOT repeat the same mistakes.

{chr(10).join(history_parts)}

Your new diagnosis MUST address every issue listed above.
The Fixer MUST produce fixes for every file listed as "still broken".
"""

    prompt = f"""You are a senior Go engineer diagnosing a CI/CD build failure.

FAILURE CLASSIFICATION (from Inspector):
  Type:          {classification.failure_type}
  Summary:       {classification.error_summary}
  All failed files: {', '.join(classification.all_failed_files)}
  Suggested fix: {classification.suggested_fix_area}

EXACT BUILD ERRORS (verbatim from compiler):
{chr(10).join(f"  {line}" for line in classification.error_lines)}

{history_section}

Your task:
1. Diagnose the ROOT CAUSE of EVERY error in the build log above
2. Set affected_component to list ALL affected functions/files
3. Write a fix_strategy that is a numbered list with one step per error:
   - Each step names the file, the line, and the exact change required
   - Steps must be ordered (dependencies first)
4. Set files_to_fix to ALL files the Fixer must modify

CRITICAL: Your strategy must result in a build that compiles with ZERO errors.
Do NOT focus on only one file if there are errors in multiple files."""

    root_cause: RootCause = structured_llm.invoke(prompt)

    print(f"   ✅ Diagnosis: {root_cause.diagnosis[:100]}...")
    print(f"   🎯 Affected: {root_cause.affected_component}")
    print(f"   📁 Files to fix: {', '.join(root_cause.files_to_fix) or classification.failed_file}")
    print(f"   📊 Confidence: {root_cause.confidence:.0%}")

    return {
        **state,
        "root_cause": root_cause,
        "iteration": iteration + 1,
    }
