from constructs import Construct
from aws_cdk import (
    RemovalPolicy,
    aws_s3 as _s3
)


class DatalakeBuckets(Construct):

    @property
    def bucket_list(self):
        return self._raw_bucket, self._cleaned_bucket, self._curated_bucket, self._support_bucket

    def __init__(self,
                 scope: Construct,
                 id: str,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()

        self._raw_bucket = _s3.Bucket(
            self, 'raw-bucket',
            bucket_name=f'{self.stack_name}-raw-{scope.account}',
            encryption=_s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True
        )

        self._cleaned_bucket = _s3.Bucket(
            self, 'cleaned-bucket',
            bucket_name=f'{self.stack_name}-cleaned-{scope.account}',
            encryption=_s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        self._curated_bucket = _s3.Bucket(
            self, 'curated-bucket',
            bucket_name=f'{self.stack_name}-curated-{scope.account}',
            encryption=_s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        self._support_bucket = _s3.Bucket(
            self, 'support-bucket',
            bucket_name=f'{self.stack_name}-support-{scope.account}',
            encryption=_s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )
