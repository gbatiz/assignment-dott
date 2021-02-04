import random
import string
from locust import HttpUser, task, constant


with open('lookup_list', 'r') as lookups_file:
    lookups = lookups_file.read().split()

validchars = string.ascii_letters + string.digits


class QuickstartUser(HttpUser):
    wait_time = constant(1)

    @task(7)
    def valid(self):
        self.client.get(f"/api/v1/vehicles/{random.choice(lookups)}", name='valid')

    @task(2)
    def missing(self):
        missing_id = ''.join(random.choice(validchars) for i in range(random.choice([6, 20])))
        self.client.get(f"/api/v1/vehicles/{missing_id}", name='missing')

    @task(1)
    def invalid(self):
        invalid_id = ''.join(random.choice(validchars) for i in range(random.choice(range(30))))
        self.client.get(f"/api/v1/vehicles/{invalid_id}", name='invalid')
