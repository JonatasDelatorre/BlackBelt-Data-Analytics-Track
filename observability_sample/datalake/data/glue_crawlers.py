from constructs import Construct
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_glue as _glue
)


class DataCatalogs(Construct):

    @property
    def get_crawler_name(self):
        return self.crawler.name

    def __init__(self,
                 scope: Construct,
                 id: str,
                 curated_bucket: _s3.Bucket,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()

        glue_role = _iam.Role(
            self, 'glue-role',
            assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                              _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')]
        )

        self.crawler = _glue.CfnCrawler(
            self, 'curated-crawler',
            name=f'{self.stack_name}-curated-crawler-new',
            role=glue_role.role_arn,
            database_name='cinema_sell',
            targets={
                's3Targets': [{"path": f's3://{curated_bucket.bucket_name}/cinema_sell_data/'}]
            }
        )
