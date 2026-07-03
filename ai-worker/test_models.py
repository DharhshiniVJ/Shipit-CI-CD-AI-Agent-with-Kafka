import os
import requests
from dotenv import load_dotenv

load_dotenv()
url = "https://api.cerebras.ai/v1/models"
headers = {"Authorization": f"Bearer {os.getenv('CEREBRAS_API_KEY')}"}
response = requests.get(url, headers=headers)
print(response.json())
