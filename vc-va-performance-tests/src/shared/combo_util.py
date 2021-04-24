import requests
import itertools
from shared.login import get_user_id

baseUrl = 'https://api01-np.agro.services/vc/dev/volumeApproval'


# Find all harvest + head office combinations with products.
def find_populated_combos(token):
    harvests = get_harvests(token)
    docs = partner_search(token)
    all_combos = list(itertools.product(harvests, docs))
    populated = list()
    for combo in all_combos:
        url = baseUrl + '/volumes?harvestId=' + combo[0] + '&customerHeadOfficeDocNumber=' + combo[1]
        headers = {'Authorization': 'Bearer ' + token}
        resp = requests.get(url, headers=headers, timeout=15)
        resp_json = resp.json()
        if resp_json:
            populated.append(combo)
    return populated


def partner_search(token):
    url = 'https://api-t.monsanto.com/vc/dev/partner/MONSANTO/SOJA/search?partnerType=MULTIPLICADOR' \
          '&affiliateDocumentNumbers=&matrixDocumentNumbers=' + ",".join(get_user_contracts(token)) + '&fetchAll=true'
    headers = {'Authorization': 'Bearer ' + token}
    resp = requests.get(url, headers=headers, timeout=15)
    resp_json = resp.json()
    ho_doc_nums = list()
    for key in resp_json['results']:
        ho_doc_nums.append(str(key['document']['documentNumber']))
    return ho_doc_nums


def get_harvests(token):
    url = 'https://api-t.monsanto.com/vc/dev/masterdata/harvests?companyName=MONSANTO'
    headers = {'Authorization': 'Bearer ' + token}
    resp = requests.get(url, headers=headers, timeout=15)
    resp_json = resp.json()
    harvest_ids = list()
    for key in resp_json:
        harvest_ids.append(str(key['id']))
    return harvest_ids


def get_user_contracts(token):
    url = 'https://api01-np.agro.services/vc-user-info/userInfo/' + get_user_id()
    headers = {'Authorization': 'Bearer ' + token}
    resp = requests.get(url, headers=headers, timeout=15)
    resp_json = resp.json()
    doc_nums = list()
    for key in resp_json['contracts']:
        if key['hierarchyLevel'] == 'HEAD_OFFICE' and key['contractType'] == 'MULTIPLICADOR':
            doc_nums.append(key['customerDocument'])
    return doc_nums







