"""
fixer.py — The Fixer Agent

Role: Takes the Analyst's diagnosis and generates the actual code fixes
across ALL required files. Then uses GitHub tools to create a branch,
push every fixed file, and open a single PR covering all changes.

Analogy: The surgeon who takes the doctor's complete diagnosis and performs
ALL the necessary operations — not just one of them.
"""

import os
from langchain_openai import ChatOpenAI
from models import RootCause, ProposedFix, FileFix, Critique
from tools.github_tools import (
    fetch_file,
    get_default_branch_sha,
    create_branch,
    push_file,
    open_pull_request,
)


def get_llm():
    return ChatOpenAI(
        api_key=os.getenv("CEREBRAS_API_KEY"),
        base_url="https://api.cerebras.ai/v1",
        model="gpt-oss-120b",
        temperature=0.2,
    )


def run_fixer(state: dict) -> dict:
    """
    Fixer node — generates fixes for all broken files and opens a GitHub PR.

    Improvements over v1:
    - Fetches ALL files listed in root_cause.files_to_fix (not just one)
    - Passes Critic feedback into the prompt so the LLM knows what was wrong before
    - Generates a multi-file fix (all_fixes list)
    - Pushes all fixed files to the same branch before opening the PR
    - If branch already exists, pushes updated files to it
    """
    print("\n🔧 [Fixer] Generating fix...")

    llm = get_llm()
    structured_llm = llm.with_structured_output(ProposedFix)

    root_cause: RootCause = state["root_cause"]
    classification = state["classification"]
    critique_history: list[Critique] = state.get("critique_history", [])
    repo = os.getenv("GITHUB_REPO", "")
    commit = state.get("commit", "main")

    # Determine which files to fix
    files_to_fix = root_cause.files_to_fix or classification.all_failed_files or [classification.failed_file]

    # Fetch current content of ALL files that need fixing
    file_contexts = []
    for file_path in files_to_fix:
        try:
            content = fetch_file(repo, file_path, commit)
            file_contexts.append(
                f"FILE: {file_path}\n```go\n{content[:4000]}\n```"
            )
            print(f"   📄 Fetched {file_path} from GitHub ({len(content)} chars)")
        except Exception as e:
            file_contexts.append(
                f"FILE: {file_path}\n(Could not fetch: {e} — generate from error info)"
            )
            print(f"   ⚠️  Could not fetch {file_path}: {e}")

    # Build critic feedback section if we're on a retry
    critic_section = ""
    if critique_history:
        last = critique_history[-1]
        issues_text = "\n".join(f"  - {issue}" for issue in last.issues)
        still_broken = ", ".join(last.files_still_broken) if last.files_still_broken else "not specified"
        critic_section = f"""
⚠️  PREVIOUS FIX WAS REJECTED — DO NOT REPEAT THE SAME MISTAKES
Critic feedback: {last.feedback}
Specific issues that MUST be addressed this time:
{issues_text}
Files still broken after last attempt: {still_broken}

You MUST fix every issue listed above. If config.go still has unused imports, fix config.go too.
If handler.go still has a typo, fix handler.go. Fix ALL files listed under "still broken".
"""

    newline = "\n"
    fix_steps = newline.join(
        f"    {i+1}. {step}"
        for i, step in enumerate(root_cause.fix_strategy.split("\n"))
        if step.strip()
    )
    error_lines_text = newline.join(f"  {line}" for line in classification.error_lines)
    file_contexts_text = (newline * 2).join(file_contexts)
    files_to_fix_str = ", ".join(files_to_fix)

    prompt = f"""You are an expert Go developer fixing CI/CD build failures.

ROOT CAUSE (from Analyst):
  Diagnosis:         {root_cause.diagnosis}
  Affected:          {root_cause.affected_component}
  Files to fix:      {files_to_fix_str}
  Fix strategy:
{fix_steps}

ORIGINAL BUILD ERRORS (must ALL be resolved):
{error_lines_text}

{critic_section}

CURRENT FILE CONTENTS FROM GITHUB:
{file_contexts_text}

Your task:
1. Fix EVERY compilation error listed above
2. Fill in `all_fixes` with one FileFix entry per file that needs changing
3. Each FileFix must contain the COMPLETE file content (not a diff)
4. Make MINIMAL changes — only fix what the compiler complained about
5. Do NOT rename functions, add unnecessary imports, or refactor
6. The primary `file_path` and `fixed_content` fields must match the MOST IMPORTANT fix
7. Write a clear `pr_title` that names all fixed files

RULES:
- If a file has an unused import, REMOVE the import (do not add usage of it)
- If a function call is misspelled, fix the call site — do NOT define a new function
- Return the COMPLETE file for each fix — not just the changed lines"""

    proposed_fix: ProposedFix = structured_llm.invoke(prompt)

    print(f"   ✅ Fix generated covering {len(proposed_fix.all_fixes)} file(s)")
    for fix in proposed_fix.all_fixes:
        print(f"      📝 {fix.file_path}: {fix.changes_explanation[:80]}")
    print(f"   🏷️  PR title: {proposed_fix.pr_title}")

    # Open a real GitHub PR (push all fixed files, then open one PR)
    pr_url = None
    if repo and os.getenv("GITHUB_TOKEN"):
        pr_url = _open_github_pr(
            repo=repo,
            pipeline_id=state["pipeline_id"],
            proposed_fix=proposed_fix,
            iteration=state.get("iteration", 1),
        )
        if pr_url:
            print(f"   🎉 PR opened: {pr_url}")
    else:
        print("   ⚠️  GITHUB_TOKEN or GITHUB_REPO not set — skipping PR creation")

    return {
        **state,
        "proposed_fix": proposed_fix,
        "pr_url": pr_url,
    }


def _open_github_pr(repo: str, pipeline_id: str, proposed_fix: ProposedFix, iteration: int) -> str | None:
    """
    Creates a branch, pushes ALL fixed files, and opens a single PR on GitHub.
    Returns the PR URL, or None if anything fails.
    """
    try:
        short_id = pipeline_id[:8]
        branch_name = f"shipit/fix-{short_id}"

        # Get the SHA of the main branch to branch from
        base_sha = get_default_branch_sha(repo, "main")

        # Create (or reuse) the fix branch
        create_branch(repo, branch_name, base_sha)
        print(f"   🌿 Branch: {branch_name}")

        # Decide which files to push: all_fixes if present, else just the primary fix
        fixes_to_push: list[FileFix] = proposed_fix.all_fixes
        if not fixes_to_push:
            fixes_to_push = [FileFix(
                file_path=proposed_fix.file_path,
                fixed_content=proposed_fix.fixed_content,
                changes_explanation=proposed_fix.changes_explanation,
            )]

        for fix in fixes_to_push:
            push_file(
                repo=repo,
                file_path=fix.file_path,
                content=fix.fixed_content,
                branch=branch_name,
                commit_message=(
                    f"fix({fix.file_path}): {fix.changes_explanation[:72]}\n\n"
                    f"Auto-generated by ShipIt AI agent for pipeline {pipeline_id}"
                ),
            )
            print(f"   📤 Pushed {fix.file_path} → {branch_name}")

        # Open the PR
        pr_url = open_pull_request(
            repo=repo,
            title=proposed_fix.pr_title,
            body=_build_pr_body(proposed_fix, pipeline_id, iteration),
            head_branch=branch_name,
        )
        return pr_url

    except Exception as e:
        print(f"   ❌ GitHub PR creation failed: {e}")
        return None


def _build_pr_body(fix: ProposedFix, pipeline_id: str, iteration: int) -> str:
    """Generates a rich PR description in markdown."""
    file_sections = ""
    for f in fix.all_fixes:
        file_sections += f"\n#### `{f.file_path}`\n{f.changes_explanation}\n"

    return f"""## 🤖 Auto-generated fix by ShipIt AI

> This PR was automatically created by the ShipIt AI agent after detecting a build failure.
> It passed {iteration} critique iteration(s) before being approved.

### 📋 Pipeline
`{pipeline_id}`

### 🔍 What was wrong
{fix.changes_explanation}

### 🛠️ Files changed
{file_sections or f"- `{fix.file_path}`: {fix.changes_explanation}"}

### 📝 Full description
{fix.pr_description}

---
*Created by ShipIt AI — Inspector → Analyst → Fixer → Critic pipeline*
*Please review carefully before merging.*
"""
