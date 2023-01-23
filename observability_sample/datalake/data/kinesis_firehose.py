from constructs import Construct
from aws_cdk import (
    aws_kinesisfirehose as _kinesis_firehose,
    aws_iam as _iam,
    aws_s3 as _s3
)


class DatalakeDeliveryStream(Construct):

    @property
    def get_delivery_stream(self):
        return self._delivery_stream

    def __init__(self,
                 scope: Construct,
                 id: str,
                 raw_bucket: _s3.Bucket,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()

        kinesis_firehose_role = _iam.Role(
            self, 'kinesis-firehose-role',
            assumed_by=_iam.ServicePrincipal("firehose.amazonaws.com")
        )

        kinesis_firehose_policy = _iam.Policy(
            self, 'kinesis-firehose-role-policy',
            roles=[kinesis_firehose_role],
            statements=[_iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                actions=[
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                    "s3:PutObject"
                ],
                resources=[
                    raw_bucket.bucket_arn,
                    f'{raw_bucket.bucket_arn}/*'
                ]
            )]
        )

        self._delivery_stream = _kinesis_firehose.CfnDeliveryStream(
            self, "IngestionDataStream",
            delivery_stream_name='cinema-data-delivery-stream',
            extended_s3_destination_configuration=_kinesis_firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=raw_bucket.bucket_arn,
                role_arn=kinesis_firehose_role.role_arn,
                prefix="data/",
                error_output_prefix="errors/",
                buffering_hints=_kinesis_firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=120,
                    size_in_m_bs=2
                )
            )
        )
