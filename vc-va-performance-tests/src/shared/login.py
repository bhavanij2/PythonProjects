import requests
import os


def get_bearer_token():
    magic_token = os.environ['PING_MAGIC_TOKEN']
    username = os.environ['LOGIN_PARTICIPANT_USERNAME']
    password = os.environ['LOGIN_PARTICIPANT_PWD']
    url = 'https://test.amp.monsanto.com/as/token.oauth2'
    headers = {'Authorization': 'Basic ' + magic_token}
    data = {'grant_type': 'password',
            'username': username,
            'password': password
            }
    resp = requests.post(url, data=data, headers=headers)
    return resp.json()['access_token']


def get_user_id():
    return os.environ['LOGIN_PARTICIPANT_USERNAME']


def get_user_doc_number():
    return '59677015915'

