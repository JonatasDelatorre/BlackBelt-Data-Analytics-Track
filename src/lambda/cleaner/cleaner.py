import awswrangler as wr
import pandas as pd
import boto3
import json
import os


def handler(event, context):
    print(event)

    source_bucket_name = event['detail']['bucket']['name']
    source_object_key = event['detail']['object']['key']

    destination_bucket_name = os.environ['DESTINATION_BUCKET_NAME']
    destination = f's3://{destination_bucket_name}/cinema_data/'

    s3 = boto3.client('s3')

    obj = s3.get_object(Bucket=source_bucket_name, Key=source_object_key)
    body = obj['Body'].read()

    # json_data = body.replace("\\u", "")

    data = body.decode('utf8').replace("}{", "};{")

    json_list = data.split(";")

    lista = [json.loads(item) for item in json_list]

    df = pd.DataFrame.from_records(lista)

    # try: df['production_companies'] = df['production_companies'].apply(lambda x: x.split("'")[3] if 'name' in x
    # else '') df['production_countries'] = df['production_countries'].apply(lambda x: x.split("'")[7] if 'name' in x
    # else '') df['spoken_languages'] = df['spoken_languages'].apply(lambda x: x.split("'")[7] if 'name' in x else
    # '') except Exception as e: df['production_companies'] = 'INVALID_NAME' df['production_countries'] =
    # 'INVALID_COUNTRIES' df['spoken_languages'] = 'INVALID_LANGUAGE' print(f"ERROR: {e}")

    df = df.dropna(subset=['sell_date'])
    df['year'] = df['sell_date'].apply(lambda x: str(x).split('-')[0])
    df['month'] = df['sell_date'].apply(lambda x: str(x).split('-')[1])
    df['day'] = df['sell_date'].apply(lambda x: str(x).split('-')[2])
    df['cinema_name'] = df['cinema']

    print(df.to_string())

    response = wr.s3.to_parquet(
        df=df,
        path=destination,
        dataset=True,
        partition_cols=['cinema', 'year', 'month', 'day']
    )

    print(response)

    return response
