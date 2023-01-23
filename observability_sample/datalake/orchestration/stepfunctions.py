from constructs import Construct
from aws_cdk import (
    aws_stepfunctions as _stf,
    aws_stepfunctions_tasks as _stf_tasks,
    aws_lambda as _lambda,
    Duration
)


class DatalakeProcessSTF(Construct):

    @property
    def get_stepfunctions(self):
        return self.stf

    def __init__(self,
                 scope: Construct,
                 id: str,
                 cleaner_lambda: _lambda.Function,
                 invoke_emr_lambda: _lambda.Function,
                 check_emr_lambda: _lambda.Function,
                 load_lambda: _lambda.Function,
                 invoke_crawler_lambda: _lambda.Function,
                 check_crawler_lambda: _lambda.Function,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        clear_task = _stf_tasks.LambdaInvoke(
            self, "Clear Data Task",
            lambda_function=cleaner_lambda,
            output_path="$.Payload",
        )

        invoke_emr_task = _stf_tasks.LambdaInvoke(
            self, "Invoke EMR process Data Task",
            lambda_function=invoke_emr_lambda,
            output_path="$.Payload",
        )

        check_emr_task = _stf_tasks.LambdaInvoke(
            self, "Check EMR process Data Task",
            lambda_function=check_emr_lambda,
            result_path="$.check_emr_result"
        )

        invoke_crawler_task = _stf_tasks.LambdaInvoke(
            self, "Invoke Crawler Catalog Task",
            lambda_function=invoke_crawler_lambda,
            output_path="$.Payload",
        )

        check_crawler_task = _stf_tasks.LambdaInvoke(
            self, "Check Crawler Catalog Task",
            lambda_function=check_crawler_lambda,
            result_path="$.check_crawler_result"
        )

        load_task = _stf_tasks.LambdaInvoke(
            self, "Load data to Opensearch",
            lambda_function=load_lambda,
            output_path="$",
        )

        succeed_job = _stf.Succeed(
            self, "Succeeded",
            comment='AWS Batch Job succeeded'
        )

        wait_emr_job = _stf.Wait(
            self, "Wait EMR 20 Seconds",
            time=_stf.WaitTime.duration(
                Duration.seconds(20))
        )

        wait_crawler_job = _stf.Wait(
            self, "Wait Crawler 20 Seconds",
            time=_stf.WaitTime.duration(
                Duration.seconds(20))
        )

        definition = clear_task \
            .next(invoke_emr_task) \
            .next(check_emr_task) \
            .next(_stf.Choice(self, 'Job Complete?')
                  .when(_stf.Condition.string_equals('$.check_emr_result.Payload', 'SUCCESS'), invoke_crawler_task
                        .next(check_crawler_task)
                        .next(_stf.Choice(self, 'Crawler Complete?')
                              .when(_stf.Condition.string_equals('$.check_crawler_result.Payload', 'SUCCEEDED'),
                                    load_task
                                    .next(succeed_job)
                                    )
                              .otherwise(wait_crawler_job
                                         .next(check_crawler_task)
                                         )
                              )
                        )
                  .otherwise(wait_emr_job
                             .next(check_emr_task)
                             )
                  )

        # Create state machine
        self.stf = _stf.StateMachine(
            self, "StateMachine Pipeline",
            definition=definition,
            timeout=Duration.minutes(15)
        )
