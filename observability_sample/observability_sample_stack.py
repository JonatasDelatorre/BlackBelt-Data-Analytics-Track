from operator import imod
from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as _s3,
)

from .network.vpc import DatalakeVpc
from .network.security_group import DatalakeSGs
from .datalake.data.buckets import DatalakeBuckets
from .datalake.data.kinesis_firehose import DatalakeDeliveryStream
from .datalake.data.opensearch import DatalakeDashboardsDomain
from .datalake.data.glue_crawlers import DataCatalogs
from .datalake.process_pipeline.lambdas import DatalakeLambdas
from .datalake.process_pipeline.emr_serverless import EmrServerlessApplications
from .datalake.orchestration.stepfunctions import DatalakeProcessSTF
from .datalake.orchestration.event_bridge import StfEventBridgeS3


class ObservabilitySampleStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # self.env = construct_id.split('-')[-1]
        # self.stack = construct_id.replace(f'-{self.env}', '')

        # ========================================================================
        # ======================== NETWORK CONFIG ================================
        # ========================================================================
        datalake_vpc = DatalakeVpc(
            self, 'DatalakeVpc'
        )
        vpc = datalake_vpc.get_vpc

        datalake_sgs = DatalakeSGs(
            self, 'DatalakeSGs', vpc=vpc
        )
        workspace_sg, engine_sg, opensearch_sg = datalake_sgs.get_engine_workspace_sg

        # ========================================================================
        # ======================== DATALAKE BUCKETS ==============================
        # ========================================================================
        datalake_buckets = DatalakeBuckets(
            self, 'DatalakeBuckets',
        )
        raw_bucket, cleaned_bucket, curated_bucket, support_bucket = datalake_buckets.bucket_list

        # ========================================================================
        # ======================== DELIVERY STREAM ===============================
        # ========================================================================
        datalake_delivery_stream = DatalakeDeliveryStream(
            self, 'DatalakeDeliveryStream',
            raw_bucket=raw_bucket,
        )
        delivery_stream = datalake_delivery_stream.get_delivery_stream

        # ========================================================================
        # ========================== OPENSEARCH ==================================
        # ========================================================================
        opensearch = DatalakeDashboardsDomain(
            self, 'DatalakeDashboardsDomain',
            vpc=vpc,
            opensearch_sg=opensearch_sg
        )

        domain = opensearch.get_domain

        # ========================================================================
        # ========================== DATA CATALOG ================================
        # ========================================================================

        data_catalogs = DataCatalogs(
            self, 'DataCatalogs',
            curated_bucket=curated_bucket
        )

        crawler_name = data_catalogs.get_crawler_name

        # ========================================================================
        # ======================== PROCESS PIPELINE ==============================
        # ========================================================================

        emr_serverless_applications = EmrServerlessApplications(
            self, 'EmrServerlessApplications',
            support_bucket=support_bucket,
            vpc=vpc,
            workspace_sg=workspace_sg,
            engine_sg=engine_sg
        )

        process_emr_application = emr_serverless_applications.process_emr_application

        lambda_functions = DatalakeLambdas(
            self, 'DatalakeLambdas',
            raw_bucket=raw_bucket,
            cleaned_bucket=cleaned_bucket,
            curated_bucket=curated_bucket,
            support_bucket=support_bucket,
            process_emr_application=process_emr_application,
            opensearch_domain=domain,
            crawler_name=crawler_name
        )

        cleaner_lambda, invoke_emr_lambda, check_emr_lambda, load_lambda, invoke_crawler_lambda, check_crawler_lambda = lambda_functions.functions_list

        # ========================================================================
        # ========================= ORCHESTRATION ================================
        # ========================================================================

        # Stepfunctions definition
        step_functions = DatalakeProcessSTF(
            self, 'DatalakeProcessSTF',
            cleaner_lambda=cleaner_lambda,
            invoke_emr_lambda=invoke_emr_lambda,
            check_emr_lambda=check_emr_lambda,
            load_lambda=load_lambda,
            invoke_crawler_lambda=invoke_crawler_lambda,
            check_crawler_lambda=check_crawler_lambda
        )

        state_machine = step_functions.get_stepfunctions

        # Event Bridge definition
        stf_event_from_s3 = StfEventBridgeS3(
            self, 'StfEventBridgeS3',
            raw_bucket=raw_bucket,
            state_machine=state_machine
        )
