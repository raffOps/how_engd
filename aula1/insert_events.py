import json

import boto3
from fake_web_events import Simulation

client = boto3.client("firehose")


def put_records(event):
    data = json.dumps(event) + "\n"
    response = client.put_record(
        DeliveryStreamName="rjr-howbootcamp-aula1",
        Record={"Data": data}
    )
    print(event)
    return response


simulation = Simulation(user_pool_size=100, sessions_per_day=10000)
events = simulation.run(duration_seconds=600)

for event in events:
    put_records(event)
