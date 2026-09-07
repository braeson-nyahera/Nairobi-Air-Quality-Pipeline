import requests
import os
from dotenv import load_dotenv

load_dotenv()

url = "https://api.openaq.org/v3/locations/2178"
headers = {
    "X-API-Key": os.getenv('OpenAQ_API_KEY'),
}

response = requests.get(url, headers=headers)
response.raise_for_status()

data = response.json()
print(data)