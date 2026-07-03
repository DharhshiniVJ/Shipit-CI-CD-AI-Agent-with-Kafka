"""
critic.py — The Critic Agent

Role: Reviews the Fixer's proposed fixes across ALL files and decides
whether the entire fix set is good enough. Returns structured feedback
that accumulates across retries so the Analyst/Fixer get smarter each loop.

Analogy: A senior code reviewer who has read every previous review and
is getting increasingly specific about exactly what is STILL wrong.
"""

import os
from langchain_openai import ChatOpenAI
from models import ProposedFix, RootCause, Critique, FileFix

# Minimum confidence to accept the fix and open the PR.
CONFIDENCE_THRESHOLD = 0.80


def get_llm():
    return ChatOpenAI(
        api_key=os.getenv("CEREBRAS_API_KEY"),
        base_url="https://api.cerebras.ai/v1",
        model="gpt-oss-120b",
        temperature=0.1,
    )


def run_critic(state: dict) -> dict:
    """
    Critic node — validates the ENTIRE proposed fix set.

    Improvements over v1:
    - Reviews ALL files in proposed_fix.all_fixes, not just one
    - Passes full history of prior Critic rejections into the prompt
    - Sets files_still_broken so the Analyst knows exactly what to focus on
    - Stricter threshold (0.80) with explicit acceptance criteria
    """
    iteration = state.get("iteration", 1)
    critique_history: list[Critique] = state.get("critique_history", [])
    print(f"\n🎯 [Critic] Reviewing proposed fix (iteration {iteration})...")

    llm = get_llm()
    structured_llm = llm.with_structured_output(Critique)

    proposed_fix: ProposedFix = state["proposed_fix"]
    root_cause: RootCause = state["root_cause"]
    classification = state["classification"]

    # Build the full list of file fixes to review
    fixes_to_review: list[FileFix] = proposed_fix.all_fixes
    if not fixes_to_review:
        from models import FileFix as FF
        fixes_to_review = [FF(
            file_path=proposed_fix.file_path,
            fixed_content=proposed_fix.fixed_content,
            changes_explanation=proposed_fix.changes_explanation,
        )]

    file_review_sections = ""
    for fix in fixes_to_review:
        file_review_sections += (
            f"\n### File: `{fix.file_path}`\n"
            f"**Changes made:** {fix.changes_explanation}\n"
            f"```go\n{fix.fixed_content[:3000]}\n```\n"
        )

    # History of prior rejections so the critic can see if the same mistake repeats
    prior_rejections_section = ""
    if critique_history:
        parts = []
        for i, past in enumerate(critique_history, 1):
            parts.append(
                f"Rejection #{i} (confidence: {past.confidence_score:.0%}): "
                f"{past.feedback} | Issues: {'; '.join(past.issues)}"
            )
        prior_rejections_section = (
            "\n\nPRIOR REJECTION HISTORY (for reference — do not re-raise resolved issues):\n"
            + "\n".join(parts)
        )

    prompt = f"""You are a strict senior Go engineer doing a final code review before a PR is merged to production.

A junior AI agent generated the following fix for a CI/CD build failure. Your job:
1. Verify EVERY build error in the original log is resolved by the fix
2. Check each fixed file compiles correctly (no new imports, no new syntax errors)
3. Ensure fixes are minimal — no unnecessary refactoring
4. Confirm no new bugs are introduced

ORIGINAL BUILD ERRORS (ALL must be resolved):
{chr(10).join(f"  {line}" for line in classification.error_lines)}

ROOT CAUSE:
{root_cause.diagnosis}

PROPOSED FIX (across {len(fixes_to_review)} file(s)):
{file_review_sections}

OVERALL EXPLANATION:
{proposed_fix.changes_explanation}
{prior_rejections_section}

ACCEPTANCE CRITERIA (ALL must be true to accept):
✅ Every error line in the original build log is addressed by a fix
✅ No unused imports remain in any file
✅ No undefined function calls remain in any file
✅ No new compilation errors introduced
✅ Fixed content is complete (not a partial diff)

If ANY criterion fails, set is_confident=False and list EXACTLY which files still have errors.
Rate confidence: 0.0 = completely wrong, 1.0 = all errors resolved, production-safe.
Acceptance threshold: {CONFIDENCE_THRESHOLD}

Be ruthlessly specific in your issues list — name the file and the exact remaining error."""

    critique: Critique = structured_llm.invoke(prompt)

    # Enforce threshold
    critique.is_confident = critique.confidence_score >= CONFIDENCE_THRESHOLD

    if critique.is_confident:
        print(f"   ✅ Fix ACCEPTED (confidence: {critique.confidence_score:.0%})")
    else:
        print(f"   ❌ Fix REJECTED (confidence: {critique.confidence_score:.0%})")
        print(f"   💬 {critique.feedback[:120]}")
        for issue in critique.issues:
            print(f"      • {issue}")
        if critique.files_still_broken:
            print(f"   🔴 Still broken: {', '.join(critique.files_still_broken)}")

    # Accumulate history for the next iteration
    new_history = critique_history + [critique]

    return {
        **state,
        "critique": critique,
        "critique_history": new_history,
    }
