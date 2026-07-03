import os
import requests
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("GITHUB_TOKEN")
repo = "DharhshiniVJ/dx-agent-dummy"
headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github.v3+json"
}

resp = requests.get(f"https://api.github.com/repos/{repo}", headers=headers)
print(f"Repo Info Status: {resp.status_code}")
if resp.status_code == 200:
    print(f"Permissions: {resp.json().get('permissions')}")
    print(f"Default branch: {resp.json().get('default_branch')}")
else:
    print(resp.text)
