import requests


def get_bearer_token():
    magic_token = 'UEQtQjJCLUNMSTo='
    username = 'GMATS_P'
    password = 'Milanesa06'
    url = 'https://test.amp.monsanto.com/as/token.oauth2'
    headers = {'Authorization': 'Basic ' + magic_token}
    data = {'grant_type': 'password',
            'username': username,
            'password': password
            }
    resp = requests.post(url, data=data, headers=headers)
    return resp.json()['access_token']


