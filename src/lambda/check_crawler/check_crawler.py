import os
import boto3


def handler(event, context):
    crawler_name = os.environ['CRAWLER_NAME']

    print(crawler_name)

    client = boto3.client('glue')

    response = client.get_crawler(
        Name=crawler_name
    )

    if response['Crawler']['State'] == 'RUNNING':
        return response['Crawler']['State']
    else:
        status = response['Crawler']['LastCrawl']['Status']
        if status != "SUCCEEDED":
            raise Exception(f'ERRO: Crawler status {status}')
        else:
            print(f'Crawler {status}')
            return status
