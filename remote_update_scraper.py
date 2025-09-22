# -*- coding: utf-8 -*-

import requests
import json

with open('./secrets/pin_list.json') as f:
  pin_list = json.load(f)

domain = "https://serviceadvisor.deere.com"
api_string = "/SAWeb/services/"
csu_string = "controllerSoftwareUpdates/"
machine_string = "startSession/"

csu_url = domain + api_string + csu_string
machine_url = domain + api_string + machine_string

session = requests.Session()

with open('./secrets/credentials.json', 'r') as f:
  credentials = json.load(f)

session_cookie = credentials.get('remote_update', {}).get('SESSION')

session.cookies.set("at_check", "true", domain=".deere.com", path="/")
session.cookies.set("SESSION", session_cookie, domain="serviceadvisor.deere.com", path="/")

header_machine = {"Content-Type": "application/vnd.johndeere.sa.startSession.v1+json"}

def get_update_info(software_info):
  CU = software_info['softwareUpdateId'].split('^')[1]

  if len(software_info['sectionDetails']) != 1:
    raise Exception('Invalid number of section details')

  upd_info = software_info['sectionDetails'][0]

  parsed_info = {
      'controller': CU,
      'title': upd_info.get('tla', None),
      'description': upd_info.get('description', None),
      'current_version': upd_info.get('softwareVersion', None),
      'available_version': upd_info.get('availableVersion', None),
      'remote_update': software_info.get('remoteCertified', None)
  }

  parsed_info['update_available'] = parsed_info['current_version'] != parsed_info['available_version']

  return parsed_info

updates = []

errorbox = {
    'main_parse_error':[],
    'empty_pin':[],
    'in_parse_error':[],
    'missing_value':[],
}

for pin in pin_list:

  try:
    assert isinstance(pin, str)
    response = session.get(csu_url + pin)
    response.raise_for_status()
  except:
    errorbox['request_fail'].append(pin)
    continue

  try:
    response_data = response.json()['controllerSoftwareUpdates']
    assert isinstance(response_data, list)
  except Exception as e:
    errorbox['main_parse_error'].append((pin, e))
    continue

  machine_updates = []
  for idx, sv in enumerate(response_data):
    try:
      update_info = get_update_info(sv)
    except:
      errorbox['in_parse_error'].append((pin, idx, sv))
      continue
    if None in update_info.values(): errorbox['missing_value'].append((pin, idx, sv))
    machine_updates.append({'pin':pin, **update_info})

  if machine_updates:
    updates.extend(machine_updates)
  else:
    errorbox['empty_pin'].append(pin)

print()
print('Finished scraping software update information...')
print()
print(f'\tMachines searched: {len(pin_list)}')
print(f'\tMachines without software info: {len(errorbox["empty_pin"])}')
print(f'\tMachines with unreadable data: {len(errorbox["main_parse_error"])}')
print()
print(f'\tSoftware instances found: {len(updates)}')
print(f'\tCollected instances with missing values: {len(errorbox["missing_value"])}')
print(f'\tInstances with unreadable data: {len(errorbox["in_parse_error"])}')
print()
print(f'\tRemote reprogramming opportunities: {len([u for u in updates if (u["remote_update"] and u["update_available"])])}')

