from locust import HttpLocust
from preapproval.tasks import PreApprovalTasks
from data.delete_after_tests import database_connection
import time


class PreApprovalLocust(HttpLocust):
    task_set = PreApprovalTasks
    min_wait = 5000
    max_wait = 10000
    host = "https://api01-np.agro.services/vc/dev/volumeApproval"
    #host = "https://api01-np.agro.services/vc/ps/volumeApproval"

    def teardown(self):
        print('Waiting for tasks to complete...')
        time.sleep(30)
        database_connection.clear_test_data()