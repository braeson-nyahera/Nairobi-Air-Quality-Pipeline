import os
import requests
from dotenv import load_dotenv

load_dotenv()

url = "https://api.openaq.org/v3/locations?coordinates=-1.286389,36.817223&radius=25000"
headers = {
    "X-API-Key": os.getenv('OpenAQ_API_KEY'),
}

response = requests.get(url, headers=headers)
response.raise_for_status()

data = response.json()
print(data)