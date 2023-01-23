import os
import json
import boto3
import awswrangler as wr
from botocore.exceptions import ClientError

day = 7
month = 12
year = 2022


# year = datetime.today().year
# month = datetime.today().month
# day = datetime.today().day

def get_secret():
    secret_name = "dev/opensearch/admin"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    secret_dict = json.loads(secret)
    username = secret_dict['username']
    password = secret_dict['password']

    return username, password


def handler(event, context):
    domain_endpoint = os.environ['OPENSEARCH_DOMAIN']
    support_bucket_name = os.environ['SUPPORT_BUCKET_NAME']

    opensearch_domain = f'https://{domain_endpoint}'

    username, password = get_secret()

    sql = f"""SELECT * FROM "cinema_sell"."cinema_sell_data" WHERE year='{year}' AND month='{month}' AND day='{day}' 
    AND process_timestamp = (SELECT max(process_timestamp) FROM "cinema_sell"."cinema_sell_data" WHERE year='{year}' 
    AND month='{month}' AND day='{day}');"""

    df = wr.athena.read_sql_query(
        sql=sql,
        database='cinema_sell',
        s3_output=f's3://{support_bucket_name}/athena/'
    )

    print(df.head(5))

    client = wr.opensearch.connect(
        host=opensearch_domain,
        username=username,
        password=password
    )

    wr.opensearch.index_df(
        client=client,
        df=df,
        index='cinema_sell_data'
    )
