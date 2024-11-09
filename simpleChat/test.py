import random
import subprocess
import time
from locust import User, task, between, events # type: ignore


def sendText(client: subprocess.Popen[str], text: str) :
    client.stdin.write(text+"\n")
    client.stdin.flush()
    time.sleep(0.1)

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    subprocess.Popen(['./server.out'],text=True,stdin=subprocess.PIPE)


class NormalUser(User):
    wait_time = between(1, 5)

    @task
    def hello_world(self):
        sendText(self.client,"Hello World")

    @task(4)
    def fruit(self):
        sendText(self.client,random.choice(["Apple","Banana","Orange","Blueberry","Pineapple","Blueberry"]))

    def on_start(self):
        self.client = subprocess.Popen(['./client.out'],text=True,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL)
    def on_stop(self):
        sendText(self.client,"Good Bye")
        sendText(self.client,"q")

class LazyUser(User):
    wait_time = between(200, 1000)

    @task
    def hello_world(self):
        sendText(self.client,"YOOOO")

    def on_start(self):
        self.client = subprocess.Popen(['./client.out'],text=True,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL)
    def on_stop(self):
        sendText(self.client,"q")