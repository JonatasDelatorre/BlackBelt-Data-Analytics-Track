from constructs import Construct
from aws_cdk import (
    aws_opensearchservice as _opensearch,
    aws_ec2 as _ec2,
    aws_iam as _iam,
    RemovalPolicy, SecretValue, CfnOutput
)

import boto3
import json
from botocore.exceptions import ClientError


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

    return secret_dict


class DatalakeDashboardsDomain(Construct):

    @property
    def get_domain(self):
        return self.domain

    def __init__(self,
                 scope: Construct,
                 id: str,
                 vpc: _ec2.Vpc,
                 opensearch_sg: _ec2.SecurityGroup,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()

        self.domain = _opensearch.Domain(
            self, 'DatalakeDomain',
            version=_opensearch.EngineVersion.OPENSEARCH_1_3,
            domain_name='opensearch-datalake-viewer',
            removal_policy=RemovalPolicy.DESTROY,
            capacity=_opensearch.CapacityConfig(
                data_node_instance_type='t3.small.search',
                data_nodes=1
            ),
            ebs=_opensearch.EbsOptions(
                enabled=True,
                volume_size=50,
                volume_type=_ec2.EbsDeviceVolumeType.GP3
            ),
            # vpc=vpc,
            # vpc_subnets=[_ec2.SubnetSelection(subnet_type=_ec2.SubnetType.PUBLIC, availability_zones=['us-east-1a'])],
            # security_groups=[opensearch_sg],
            enforce_https=True,
            node_to_node_encryption=True,
            encryption_at_rest={
                "enabled": True
            },
            # use_unsigned_basic_auth=True,
            fine_grained_access_control={
                "master_user_name": get_secret()['username'],
                "master_user_password": SecretValue.unsafe_plain_text(get_secret()['password'])
            }
        )

        self.domain.add_access_policies(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.AnyPrincipal()],
                actions=[
                    "es:*"
                ],
                resources=[
                    f"{self.domain.domain_arn}/*"
                ]
            )
        )

        # CfnOutput(self, "MasterUser",
        #                 value=DOMAIN_ADMIN_UNAME,
        #                 description="Master User Name for Amazon OpenSearch Service")

        # CfnOutput(self, "MasterPW",
        #                 value=DOMAIN_ADMIN_PW,
        #                 description="Master User Password for Amazon OpenSearch Service")
