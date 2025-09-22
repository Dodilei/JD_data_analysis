# -*- coding: utf-8 -*-

import requests
import json
from tqdm import tqdm
import pandas as pd

OUTPUT_FILENAME = "ru_opps"

domain = "https://serviceadvisor.deere.com"
api_string = "/SAWeb/services/"
csu_string = "controllerSoftwareUpdates/"
machine_string = "startSession/"

csu_url = domain + api_string + csu_string
machine_url = domain + api_string + machine_string

startsession_header = {"content-type": "application/vnd.johndeere.sa.startSession.v1+json"}
machineinfo_header = {"content-type": "application/vnd.johndeere.sa.getMachineInfo.v1+json"}


def get_machine_info(session, pin):
  payload = {"PIN": pin}

  response = session.post(
      machine_url,
      headers=startsession_header,
      data=json.dumps(payload) 
  )

  response.raise_for_status()

  minfo = response.json()

  return minfo.get('sessionID', None), minfo.get('isRemoteCapable', None), minfo.get('notRemoteCapableDesc', '')


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


def get_auth_info(session, session_id):

  auth_url = domain + api_string + session_id + '/machineInfo/pt'

  payload = '{"timeStamp":null}'

  response = session.post(auth_url, headers=machineinfo_header, data=payload)

  if 200 <= response.status_code <= 299:
    is_authorized = True
  elif response.status_code == 403:
    is_authorized = False
  else:
    response.raise_for_status()

  return is_authorized





def remote_update_scraper():
  print('Starting Remote Update Scraper\n')

  print('Importing machine data...')
  machines = pd.read_csv('../machine_data.csv', index_col=7, usecols=range(1,9), dtype=str)
  machines.drop_duplicates()

  pin_list = machines[machines.maker == 'JOHN DEERE'].index.tolist()
  print(f"Número de PINs carregados: {len(pin_list)}")

  print('Loading credentials: ', end='')
  with open('./secrets/credentials.json', 'r') as f:
    credentials = json.load(f)

  session_cookie = credentials.get('remote_update', {}).get('SESSION')
  if session_cookie:
    print('Credentials loaded.')
  else:
    print('Failed to get credentials.')
    return 0

  print('\nCreating request session...')

  session = requests.Session()

  session.cookies.set("at_check", "true", domain=".deere.com", path="/")
  session.cookies.set("SESSION", session_cookie, domain="serviceadvisor.deere.com", path="/")

  print('Session created. Starting scraping loop.\n')

  updates = []

  errorbox = {
      'main_parse_error':[],
      'empty_pin':[],
      'in_parse_error':[],
      'missing_value':[],
      'machine_info_missing':[],
      'request_fail':[],
  }

  pb_iterator = tqdm(pin_list, desc="Scraping software update info")

  for pin in pb_iterator:
    pb_iterator.set_postfix_str(f"Processing PIN: {pin}")

    session_id, is_capable, remcap_desc, is_authorized = None, None, None, None
    try:
      session_id, is_capable, remcap_desc = get_machine_info(session, pin)
      is_authorized = get_auth_info(session, session_id)
    except:
      errorbox['machine_info_missing'].append(pin)

    machine_info = {
      'remote_capable':is_capable,
      'capability_description':remcap_desc,
      'is_authorized': is_authorized
    }

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
      machine_updates.append({'pin':pin, **update_info, **machine_info})

    if machine_updates:
      updates.extend(machine_updates)
    else:
      errorbox['empty_pin'].append(pin)

  print()
  print('Finished scraping software update information...')
  print(f'\tRequests failed: {len(errorbox['request_fail'])}')
  print()
  print(f'\tMachines searched: {len(pin_list) - len(errorbox['request_fail'])}')
  print(f'\tMachines without software info: {len(errorbox["empty_pin"])}')
  print(f'\tMachines with unreadable data: {len(errorbox["main_parse_error"])}')
  print(f'\tMachines with no remote capability: {len(set([u['pin'] for u in updates if not u["remote_capable"]]))}')
  print()
  print(f'\tSoftware instances found: {len(updates)}')
  print(f'\tCollected instances with missing values: {len(errorbox["missing_value"])}')
  print(f'\tInstances with unreadable data: {len(errorbox["in_parse_error"])}')
  print()
  print(f'\tRemote reprogramming opportunities: {len([u for u in updates if (u["remote_update"] and u["update_available"])])}')

  print('\n')
  print('Processing data...')

  update_df = pd.DataFrame(updates)

  opportunities = update_df[update_df.update_available].copy().reset_index(drop=True)

  with open('./secrets/orgnames.json', 'r') as f:
      orgnames = json.load(f)

  opportunities['temp_remup'] = (opportunities.remote_capable & opportunities.remote_update)
  opportunities['model'] = opportunities.pin.map(machines.model)
  opportunities['orgname'] = opportunities.pin.map(machines.orgid).map(orgnames)
  opportunities.sort_values('orgname', ascending=False, inplace=True)

  org_opp_count = opportunities.groupby(opportunities.pin.map(machines.orgid)).sum()[['update_available', 'remote_update', 'temp_remup']]
  opportunities.drop('temp_remup', axis=1, inplace=True)
  org_opp_count = org_opp_count.set_axis(org_opp_count.index.map(orgnames))
  org_opp_count = org_opp_count.sort_values('update_available', ascending=False)

  print('Data processed.')
  print('Creating excel file...')

  for i in range(5):
    postfix = str(i) if i != 0 else ''
    try:
      writer = pd.ExcelWriter('../'+OUTPUT_FILENAME+postfix+'.xlsx', engine='xlsxwriter')
    except Exception as e:
      print(f'Failed to start excel writer. Error: {e}')
      print(f'Writing to new filename.')

  org_opp_count.to_excel(writer, sheet_name='Contagem de Oportunidades', index=True)

  opportunities_renamed = opportunities[['pin', 'orgname', 'model', 'title', 'description', 'current_version', 'available_version', 'remote_update', 'remote_capable', 'capability_description', 'is_authorized']].rename(
    columns={
      'pin': 'PIN',
      'orgname': 'Organização',
      'model': 'Modelo',
      'title': 'Título',
      'description': 'Descrição',
      'current_version': 'Versão Atual',
      'available_version': 'Versão Disponível',
      'remote_update': 'Att Remota',
      'remote_capable': 'Conexão remota',
      'capability_description': 'Descrição da conexão',
      'is_authorized': 'Autorizado',
  })

  opportunities_renamed['Att Remota'] = opportunities_renamed['Att Remota'].map({True: 'SIM', False: 'NAO'})
  opportunities_renamed['Conexão remota'] = opportunities_renamed['Conexão remota'].map({True: 'SIM', False: 'NAO'})
  opportunities_renamed['Autorizado'] = opportunities_renamed['Autorizado'].map({True: 'SIM', False: 'NAO'})
  opportunities_renamed['Descrição da conexão'] = opportunities_renamed['Descrição da conexão'].str.replace('_', ' ').str.capitalize()
  opportunities_renamed.to_excel(writer, sheet_name='Oportunidades', index=False)

  workbook = writer.book
  worksheet1 = writer.sheets['Contagem de Oportunidades']
  worksheet2 = writer.sheets['Oportunidades']

  # Autosize columns and apply formatting to the first sheet
  for i, col in enumerate(org_opp_count.columns):
      col_len = org_opp_count[col].astype(str).str.len().max()
      col_len = max(col_len, len(col)) + 2
      worksheet1.set_column(i+1, i+1, col_len)

  # Autosize the index column for the first sheet
  index_col_len = org_opp_count.index.astype(str).str.len().max()
  index_col_len = max(index_col_len, len('Organização')) + 2
  worksheet1.set_column(0, 0, index_col_len)

  worksheet1.add_table(0, 0, org_opp_count.shape[0], org_opp_count.shape[1], {'columns': [{'header': 'Organização'}, {'header': 'Total'}, {'header': 'Remotas'}, {'header': 'Capacidade'}]})

  # Autosize columns and apply formatting to the second sheet
  for i, col in enumerate(opportunities_renamed.columns):
      col_len = opportunities_renamed[col].astype(str).str.len().max()
      col_len = max(col_len, len(col)) + 2
      worksheet2.set_column(i, i, col_len)

  worksheet2.add_table(0, 0, opportunities_renamed.shape[0], opportunities_renamed.shape[1]-1, {'columns': [{'header': col} for col in opportunities_renamed.columns]})

  writer.close()

  print('Excel workbook created.')
  print('Finished.')
  print()
  return 1

if __name__ == '__main__':
  remote_update_scraper()