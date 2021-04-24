import sys
sys.path.append('../')
from locust import TaskSet, task
from shared.login import get_bearer_token

bearer_token = get_bearer_token()


class PreApprovalTasks(TaskSet):

    @task
    def test(self):
        if bearer_token is None:
            return
        with self.client.get('/volumes/summary/closedRequests', name='Volume Approval API - closedRequests',
                             headers={'Authorization': 'Bearer ' + bearer_token}, catch_response=True) as response:
            try:
                json_response = response.json()
            except ValueError:
                response.failure('Could not parse JSON response')
                return
            if response.status_code != 200:
                response.failure('Response Code not 200')
            response.success()

    '''@task
    def on_stop(self):
        database_connection.clear_test_data()'''

