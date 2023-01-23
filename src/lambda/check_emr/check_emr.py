import boto3

status_is_done = [
    'CANCELLED',
    'FAILED',
    'SUCCESS'
]

def handler(event, context):

    print(event)

    emr_serverless = boto3.client('emr-serverless')
    
    application_id = event["ApplicationId"] 

    job_run_id = event["JobRunId"]

    status = emr_serverless.get_job_run(
        applicationId=application_id,
        jobRunId=job_run_id
    )

    status = status["jobRun"]["state"]

    if status in status_is_done:

        if(status == "FAILED" or status == "CANCELED"):
            raise Exception(f"Falha no Job, Status: {status}") 

        return status
    else:
        return {'ApplicationId': application_id, 'JobRunId': job_run_id}
