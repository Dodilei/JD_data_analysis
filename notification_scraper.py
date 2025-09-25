# -*- coding: utf-8 -*-

import requests
import json
import calendar
from tqdm import tqdm
import numpy as np

OUTPUT_FILENAME = "notifications_output"

domain = "https://notifications.deere.com"
search_string = "/notifications/search?"

notification_url = domain + search_string

user_agent = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"}

print('Loading organizations: ', end='')
with open('./secrets/orgnames.json', 'r') as f:
    orgnames = json.load(f)

org_ids = list(orgnames.keys())

print('Organizations loaded.')

print('Loading credentials: ', end='')
with open('./secrets/credentials.json', 'r') as f:
  credentials = json.load(f)

client_cookie = credentials.get('notification', {}).get('client')
if client_cookie:
  print('Credentials loaded.')
else:
  print('Failed to get credentials.')


print('\nCreating request session...')
session = requests.Session()

session.cookies.set(name="client", value=client_cookie)

session.headers = user_agent

print('Session created.')
print()

start_date = (2025, 9, 24)
end_date = (2025, 9, 25)

start_date = [str(n).zfill(2) for n in start_date]
end_date = [str(n).zfill(2) for n in end_date]
search_params = f"createdBefore={"-".join(end_date)}T23%3A59%3A59.999Z&createdAfter={"-".join(start_date)}T00%3A00%3A00.000Z"
url = notification_url+search_params

payload = {
  "targetResources": [],
  "targetResourceOrgIds": org_ids,
  "eventTypes": [],
  "severities": ["HIGH"]
}

try:
    print(f"Sending POST request...")
    response = session.post(url, json=payload)
    response.raise_for_status()  # Check for HTTP errors

    print(f"Status Code: {response.status_code}")

    print(response.text)
    with open('../'+OUTPUT_FILENAME+'.json', 'w', encoding='utf-8') as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4)
        print(f"✅ Success! Response saved to '{'../'+OUTPUT_FILENAME+'.json'}'")
    

except requests.exceptions.HTTPError as err:
    print(f"❌ HTTP error occurred: {err}")
    print(f"Status Code: {response.status_code}, Response: {response.text}")
except Exception as err:
    print(f"❌ An other error occurred: {err}")
