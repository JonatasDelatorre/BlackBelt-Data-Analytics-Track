import random
import json
import boto3
import random
import uuid
import pandas as pd
from extra_data import cinemas_list
from extra_data import local_list
from datetime import datetime

STREAM_NAME = "PUT_YOUR_KINESIS_FIREHOSE_DELIVERY_STREAM_NAME"

# Using Default profile from aws cli local
session = boto3.Session()
firehouse = session.client('firehose')

df = pd.read_csv('gerador_de_dados/data/sell_data.csv', low_memory=False)

while(1):
    # Selectig a film to simulate the sell of the ticket
    selected = random.randint(0,43541)
    film = df.iloc[[selected]]

    data = film.to_dict('records')[0]

    cinema_rand = random.randint(0,12)
    data['cinema'] = cinemas_list[cinema_rand]

    local_rand = random.randint(0,52)
    data['local'] = local_list[local_rand]

    data['sell_id'] = str(uuid.uuid4())

    # Forcing the day the data was generated
    day_rand = 7
    month_rand = 12

    # Use to generate data from today
    year = datetime.today().year
    # month = datetime.today().month
    # day = datetime.today().day

    sell_date = f'{year}-{month_rand}-{day_rand}'

    data['sell_date'] = sell_date

    data['updated_at'] = sell_date

    # Future use
    data['blockbuster'] = 'False'

    # showing the film data generated
    print(data)

    # Sending data to kinesis firehose delivery stream
    response = firehouse.put_record(
        DeliveryStreamName=STREAM_NAME,
        Record={
            'Data': json.dumps(data),
        }
    )