from constructs import Construct
from aws_cdk import (
    aws_emrserverless as _emrs,
    aws_emr as _emr,
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_ec2 as _ec2,
    aws_s3_deployment as _s3_deploy,
    Expiration,
    Duration
)

class EmrServerlessApplications(Construct):

    # @property
    def get_emr_applications(self):
        return self.process_emr_application

    def __init__(self, 
        scope: Construct, 
        id: str, 
        support_bucket: _s3.Bucket,
        vpc: _ec2.Vpc,
        workspace_sg: _ec2.SecurityGroup,
        engine_sg: _ec2.SecurityGroup,
        **kwargs
    ):

        super().__init__(scope, id, **kwargs)

# ========================================================================
# ====================== EMR SERVERLESS APPLICATION ======================
# ========================================================================
        self.process_emr_application = _emrs.CfnApplication(
            self, 'process_emr_application',
            release_label= 'emr-6.6.0',
            type='Spark',
            auto_start_configuration=_emrs.CfnApplication.AutoStartConfigurationProperty(
                enabled=True
            ),
            auto_stop_configuration=_emrs.CfnApplication.AutoStopConfigurationProperty(
                enabled=True,
                idle_timeout_minutes=15
            ),
            initial_capacity=[
                _emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=_emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_configuration=_emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4 vCPU",
                            memory="16 GB",
                            disk="20 GB"
                        ),
                        worker_count=1
                    )
                ),
                _emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=_emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_configuration=_emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4 vCPU",
                            memory="16 GB",
                            disk="20 GB"
                        ),
                        worker_count=3
                    )
                ),
            ],
            maximum_capacity=_emrs.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="32 vCPU",
                memory="128 GB",
                disk="500 GB"
            ),
        )


        deploy_process_emr_entrypoint = _s3_deploy.BucketDeployment(
            self, 'emr_entrypoint_file',
            sources=[_s3_deploy.Source.asset("src/emr/")],
            destination_bucket=support_bucket,
            destination_key_prefix='process_entry_point/'
        )

# ========================================================================
# ========================== EMR STUDIO CONFIG ===========================
# ========================================================================
        emr_studio_service_role = _iam.Role(
            self, 'emr-studio-service-role',
            assumed_by= _iam.ServicePrincipal("elasticmapreduce.amazonaws.com")
        )

        emr_studio_service_policy = _iam.Policy(
            self, 'emr-studio-service-role-policy',
            roles=[emr_studio_service_role],
            statements=[_iam.PolicyStatement(
                effect= _iam.Effect.ALLOW,
                actions= [
                    "*",
                ],
                resources= [
                    "*",
                    "arn:aws:s3:::*/*"
                ]
            )]
        )
        
        cfn_studio = _emr.CfnStudio(
            self, "emr-studio-us-east-1",
            auth_mode="IAM",
            default_s3_location= f"s3://{support_bucket.bucket_name}",
            engine_security_group_id=engine_sg.security_group_id,
            name="DatalakeStudio",
            service_role=emr_studio_service_role.role_arn,
            subnet_ids=[ ids.subnet_id for ids in vpc.private_subnets],
            vpc_id=vpc.vpc_id,
            workspace_security_group_id=workspace_sg.security_group_id,
        )

