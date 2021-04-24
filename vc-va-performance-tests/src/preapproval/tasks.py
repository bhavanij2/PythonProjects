import sys
sys.path.append('../')
from locust import TaskSet, task
from shared.login import get_bearer_token
from shared.combo_util import find_populated_combos
from shared.product_util import build_preapproval_req
import random

print('Preparing data... Please wait...')
bearer_token = get_bearer_token()
pop_combos = find_populated_combos(bearer_token)
print('Tests starting...')


class PreApprovalTasks(TaskSet):

    @task
    def get_volume_list(self):
        if bearer_token is None:
            return
        rnd_combo = random.choice(pop_combos)
        url = '/volumes?harvestId=' + rnd_combo[0] +\
              '&customerHeadOfficeDocNumber=' + rnd_combo[1]
        with self.client.get(url,
                             name='Pre-Approval - GET List of Volumes',
                             headers={'Authorization': 'Bearer ' + bearer_token},
                             catch_response=True) as response:
            try:
                json_response = response.json()
            except ValueError:
                response.failure('Could not parse JSON response')
                return
            if response.status_code != 200:
                response.failure('Response Code not 200')
            response.success()
            return response.json()

    @task
    def submit_single_item(self):
        if bearer_token is None:
            return
        url = '/preapprovedvolume/submit'
        body = build_preapproval_req(self.get_volume_list(), True)
        with self.client.post(url,
                              name='Pre-Approval - Submit Single Item Request',
                              headers={'Authorization': 'Bearer ' + bearer_token, 'content-type': 'application/json'},
                              data=body,
                              catch_response=True) as response:
            try:
                json_response = response.json()
            except ValueError:
                response.failure('Could not parse JSON response')
                return
            if response.status_code != 200:
                response.failure('Response Code not 200')
            response.success()

    @task
    def submit_multiple_items(self):
        if bearer_token is None:
            return
        url = '/preapprovedvolume/submit'
        body = build_preapproval_req(self.get_volume_list(), False)
        with self.client.post(url,
                              name='Pre-Approval - Submit Multiple Items Request',
                              headers={'Authorization': 'Bearer ' + bearer_token, 'content-type': 'application/json'},
                              data=body,
                              catch_response=True) as response:
            try:
                json_response = response.json()
            except ValueError:
                response.failure('Could not parse JSON response')
                return
            if response.status_code != 200:
                response.failure('Response Code not 200')
            response.success()







