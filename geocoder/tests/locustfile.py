import random

from locust import HttpLocust, TaskSet, task


class SuggestBehavior(TaskSet):
    min_wait = 100
    max_wait = 500

    @task(2)
    def index(self):
        self.client.get("/suggest/" + "Mannerheimintie"[0:random.randint(1, 14)],
                        name="suggest")


class ReverseBehaviour(TaskSet):
    @task()
    def index(self):
        self.client.get("/reverse/%s,%s" % (random.uniform(24.65, 24.8),
                                            random.uniform(60.16, 60.22)),
                        name='reverse')


class MovingUser(HttpLocust):
    task_set = ReverseBehaviour
    host = 'http://matka.hsl.fi/geocoder'


class TypingUser(HttpLocust):
    task_set = SuggestBehavior
    host = 'http://matka.hsl.fi/geocoder'
