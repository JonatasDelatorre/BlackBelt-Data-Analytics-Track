import os
import boto3

def handler(event, context):

    crawler_name = os.environ['CRAWLER_NAME']

    print(crawler_name)

    client = boto3.client('glue')

    response = client.get_crawler(
        Name=crawler_name
    )

    status = response['Crawler']['LastCrawl']['Status']

    if(response['Crawler']['State'] == 'READY'):
        client.start_crawler(
            Name=crawler_name
        )
    else:
        print(f'Crawler still {status}')

    return {"Status": status}
