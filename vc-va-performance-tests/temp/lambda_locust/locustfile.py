from locust import HttpLocust
from preapproval.tasks import PreApprovalTasks


class MyLocust(HttpLocust):
    task_set = PreApprovalTasks
    min_wait = 5000
    max_wait = 9000
    host = "https://api01-np.agro.services/vc/dev/volumeApproval"
