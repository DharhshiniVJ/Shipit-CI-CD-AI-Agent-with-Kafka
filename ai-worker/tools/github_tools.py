"""
github_tools.py — GitHub API helpers used by the Fixer agent.

We call the GitHub REST API directly using the `requests` library.
This is the equivalent of MCP tools, but written as plain Python functions.

The Fixer agent calls these functions to:
  1. Read the file that needs fixing (fetch_file)
  2. Create a new branch (create_branch)
  3. Push the fixed file (push_file)
  4. Open the Pull Request (open_pull_request)
"""

import os
import base64
import requests

# GitHub API base URL
GITHUB_API = "https://api.github.com"


def _headers() -> dict:
    """Build standard GitHub API headers from the environment token."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN environment variable is not set!")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fetch_file(repo: str, file_path: str, ref: str = "main") -> str:
    """
    Fetch the raw content of a file from a GitHub repository.

    Args:
        repo: "owner/repo-name"  e.g. "myorg/myapp"
        file_path: path inside the repo  e.g. "handler.go"
        ref: branch or commit SHA to read from (defaults to "main")

    Returns:
        The decoded file content as a string.
    """
    url = f"{GITHUB_API}/repos/{repo}/contents/{file_path}"
    resp = requests.get(url, headers=_headers(), params={"ref": ref})

    if resp.status_code == 404:
        raise FileNotFoundError(f"File '{file_path}' not found in repo '{repo}' at ref '{ref}'")
    resp.raise_for_status()

    # GitHub returns content as base64-encoded bytes
    content_b64 = resp.json()["content"]
    return base64.b64decode(content_b64).decode("utf-8")


def get_default_branch_sha(repo: str, branch: str = "main") -> str:
    """
    Get the latest commit SHA of a branch. Needed to create a new branch from it.
    """
    url = f"{GITHUB_API}/repos/{repo}/git/refs/heads/{branch}"
    resp = requests.get(url, headers=_headers())
    resp.raise_for_status()
    return resp.json()["object"]["sha"]


def create_branch(repo: str, new_branch: str, from_sha: str) -> None:
    """
    Create a new git branch from a given commit SHA.

    Args:
        repo: "owner/repo"
        new_branch: name for the new branch e.g. "shipit/fix-handler-typo"
        from_sha: the commit SHA to branch from (usually the main branch tip)
    """
    url = f"{GITHUB_API}/repos/{repo}/git/refs"
    payload = {
        "ref": f"refs/heads/{new_branch}",
        "sha": from_sha,
    }
    resp = requests.post(url, headers=_headers(), json=payload)
    if resp.status_code == 422:
        # Branch already exists — that's fine, we'll just push to it
        return
    resp.raise_for_status()


def push_file(repo: str, file_path: str, content: str, branch: str, commit_message: str) -> None:
    """
    Create or update a file in a GitHub repository on a specific branch.

    This is equivalent to `git add <file> && git commit && git push`.

    Args:
        repo: "owner/repo"
        file_path: path to the file in the repo
        content: the new file content (plain string, not base64)
        branch: which branch to push to
        commit_message: the git commit message
    """
    url = f"{GITHUB_API}/repos/{repo}/contents/{file_path}"

    # We need the current file's SHA to update it (GitHub requires this)
    current_sha = None
    try:
        resp = requests.get(url, headers=_headers(), params={"ref": branch})
        if resp.status_code == 200:
            current_sha = resp.json()["sha"]
    except Exception:
        pass  # File doesn't exist yet — we'll create it

    # GitHub requires content to be base64-encoded
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": commit_message,
        "content": encoded,
        "branch": branch,
    }
    if current_sha:
        payload["sha"] = current_sha  # required for updates, not for creates

    resp = requests.put(url, headers=_headers(), json=payload)
    resp.raise_for_status()


def open_pull_request(repo: str, title: str, body: str, head_branch: str, base_branch: str = "main") -> str:
    """
    Open a Pull Request on GitHub.

    Args:
        repo: "owner/repo"
        title: PR title
        body: PR description in markdown
        head_branch: the branch with the fix (e.g. "shipit/fix-handler-typo")
        base_branch: the branch to merge into (usually "main")

    Returns:
        The URL of the created PR.
    """
    url = f"{GITHUB_API}/repos/{repo}/pulls"
    payload = {
        "title": title,
        "body": body,
        "head": head_branch,
        "base": base_branch,
    }
    resp = requests.post(url, headers=_headers(), json=payload)
    resp.raise_for_status()
    return resp.json()["html_url"]
