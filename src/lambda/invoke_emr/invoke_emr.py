import boto3
import uuid
import os

def handler(event, context):

    obj_paths = event['paths']

    print(obj_paths)

    paths = ','.join(obj_paths)

    client_token = str(uuid.uuid4())

    application_id = os.environ['PROCESS_APPLICATION_ID']
    execution_role_arn = os.environ['EMR_EXECUTION_ROLE_ARN']
    suport_bucket_name = os.environ['SUPPORT_BUCKET_NAME']

    cleaned_bucket_name = os.environ['CLEANED_BUCKET_NAME']
    curated_bucket_name = os.environ['CURATED_BUCKET_NAME']

    print(application_id)

    client = boto3.client('emr-serverless')

    response = client.start_job_run(
        applicationId=application_id,
        clientToken=client_token,
        executionRoleArn=execution_role_arn,
        jobDriver={
            'sparkSubmit': {
                'entryPoint': f's3://{suport_bucket_name}/process_entry_point/process.py',
                'entryPointArguments': [
                    cleaned_bucket_name, curated_bucket_name, paths
                ]
            }
        },
        configurationOverrides = {
            'monitoringConfiguration': {
                's3MonitoringConfiguration': {
                    'logUri': f's3://{suport_bucket_name}/logs/'
                }
            }
        }
    )

    return {"ApplicationId": response['applicationId'], "JobRunId": response['jobRunId']}
