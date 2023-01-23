from constructs import Construct
from aws_cdk import (
    aws_lambda as _lambda,
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_emrserverless as _emrs,
    aws_opensearchservice as _opensearch,
    aws_glue as _glue,
    Duration
)

class DatalakeLambdas(Construct):

    @property
    def functions_list(self):
        return self.cleaner_lambda, self.invoke_emr_lambda, self.check_emr_lambda, self.load_lambda, self.invoke_crawler_lambda, self.check_crawler_lambda

    def __init__(self, 
        scope: Construct, 
        id: str, 
        raw_bucket: _s3.Bucket, 
        cleaned_bucket: _s3.Bucket, 
        curated_bucket: _s3.Bucket,
        support_bucket: _s3.Bucket,
        process_emr_application: _emrs.CfnApplication,
        opensearch_domain: _opensearch.Domain,
        crawler_name: str,
        **kwargs
    ):

        super().__init__(scope, id, **kwargs)


# ========================================================================
# ====================== CLEANER LAMBDA FUNCTION =========================
# ========================================================================
        cleaner_lambda_role = _iam.Role(
            self, 'cleaner-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        cleaner_lambda_policy = _iam.Policy(
            self, 'cleaner-lambda-role-policy',
            roles=[cleaner_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                        "s3:PutObject"
                    ],
                    resources= [
                        raw_bucket.bucket_arn,
                        f'{raw_bucket.bucket_arn}/*',
                        cleaned_bucket.bucket_arn,
                        f'{cleaned_bucket.bucket_arn}/*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

        wrangler_lambda_layer = _lambda.LayerVersion(self, 'wrangler-layer',
                  code = _lambda.AssetCode('src/lambda/layers/awswrangler-layer-2.17.0-py3.9.zip'),
                  compatible_runtimes = [_lambda.Runtime.PYTHON_3_9],
        )      

        self.cleaner_lambda = _lambda.Function(
            self, 'cleaner-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=1024,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/cleaner/'),
            handler='cleaner.handler',
            role=cleaner_lambda_role,
            layers = [wrangler_lambda_layer],
            environment={
                "DESTINATION_BUCKET_NAME": cleaned_bucket.bucket_name
            }
        )

# ========================================================================
# ================= INVOKE EMR SERVERLESS LAMBDA =========================
# ======================================================================== 

# ======================== EMR EXECUTION ROLE ============================
        process_emr_role = _iam.Role(
            self, 'process-emr-role',
            assumed_by= _iam.ServicePrincipal("emr-serverless.amazonaws.com")
        )

        process_emr_policy = _iam.Policy(
            self, 'process-emr-role-policy',
            roles=[process_emr_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                        "s3:PutObject",
                        "s3:Delete*"
                    ],
                    resources= [
                        cleaned_bucket.bucket_arn,
                        f'{cleaned_bucket.bucket_arn}/*',
                        curated_bucket.bucket_arn,
                        f'{curated_bucket.bucket_arn}/*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                        "s3:PutObject"
                    ],
                    resources= [
                        support_bucket.bucket_arn,
                        f'{support_bucket.bucket_arn}/*'
                    ]
            )]
        )

# ======================== INVOKE LAMBDA ROLE ============================
        invoke_emr_lambda_role = _iam.Role(
            self, 'invoke-emr-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        invoke_emr_lambda_policy = _iam.Policy(
            self, 'invoke-emr-lambda-role-policy',
            roles=[invoke_emr_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "emr-serverless:GetJobRun",
                        "emr-serverless:ListJobRuns",
                        "emr-serverless:StartApplication",
                        "emr-serverless:StartJobRun",
                        "emr-serverless:GetApplication",
                        "emr-serverless:GetJobRun"
                    ],
                    resources= [
                        process_emr_application.attr_arn,
                        f'{process_emr_application.attr_arn}*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "iam:PassRole"
                    ],
                    resources= [
                        process_emr_role.role_arn
                    ]
            )]
        )

# ===================== INVOKE LAMBDA FUNCTION ===========================

        updated_boto3_layer = _lambda.LayerVersion(self, 'updated-boto3-layer',
                  code = _lambda.AssetCode('src/lambda/layers/updated_boto3_layer.zip'),
                  compatible_runtimes = [_lambda.Runtime.PYTHON_3_9],
        )  


        self.invoke_emr_lambda = _lambda.Function(
            self, 'invoke-emr-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/invoke_emr/'),
            handler='invoke_emr.handler',
            role=invoke_emr_lambda_role,
            layers = [updated_boto3_layer],
            environment={
                "PROCESS_APPLICATION_ID": process_emr_application.attr_application_id,
                "EMR_EXECUTION_ROLE_ARN": process_emr_role.role_arn,
                "SUPPORT_BUCKET_NAME": support_bucket.bucket_name,
                "CLEANED_BUCKET_NAME": cleaned_bucket.bucket_name,
                "CURATED_BUCKET_NAME": curated_bucket.bucket_name
            }
        )

# ========================================================================
# ================== CHECK EMR SERVERLESS LAMBDA =========================
# ======================================================================== 

# ======================== CHECK LAMBDA ROLE ============================
        check_emr_lambda_role = _iam.Role(
            self, 'check-emr-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        check_emr_lambda_policy = _iam.Policy(
            self, 'check-emr-lambda-role-policy',
            roles=[check_emr_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "emr-serverless:GetJobRun",
                        "emr-serverless:ListJobRuns",
                        "emr-serverless:GetApplication",
                        "emr-serverless:GetJobRun"
                    ],
                    resources= [
                        process_emr_application.attr_arn,
                        f'{process_emr_application.attr_arn}*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

# ==================== CHECK EMR LAMBDA FUNCTION =========================
        self.check_emr_lambda = _lambda.Function(
            self, 'check-emr-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/check_emr/'),
            handler='check_emr.handler',
            role=check_emr_lambda_role,
            layers = [updated_boto3_layer]
        )

# ========================================================================
# =============== INVOKE CRAWLER SERVERLESS LAMBDA =======================
# ======================================================================== 

# ======================== INVOKE LAMBDA ROLE ============================
        invoke_crawler_lambda_role = _iam.Role(
            self, 'invoke-crawler-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        invoke_crawler_lambda_policy = _iam.Policy(
            self, 'invoke-crawler-lambda-role-policy',
            roles=[invoke_crawler_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "glue:StartCrawler",
                        "glue:GetCrawler"
                    ],
                    resources= [
                        f'arn:aws:glue:{scope.region}:{scope.account}:crawler/{crawler_name}'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

# ===================== INVOKE LAMBDA FUNCTION ===========================

        self.invoke_crawler_lambda = _lambda.Function(
            self, 'invoke-crawler-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/invoke_crawler/'),
            handler='invoke_crawler.handler',
            role=invoke_crawler_lambda_role,
            environment={
                "CRAWLER_NAME": crawler_name
            }
        )

# ========================================================================
# =============== CHECK CRAWLER SERVERLESS LAMBDA =======================
# ======================================================================== 

# ======================== CHECK LAMBDA ROLE ============================
        check_crawler_lambda_role = _iam.Role(
            self, 'check-crawler-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        check_crawler_lambda_policy = _iam.Policy(
            self, 'check-crawler-lambda-role-policy',
            roles=[check_crawler_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "glue:GetCrawler"
                    ],
                    resources= [
                        f'arn:aws:glue:{scope.region}:{scope.account}:crawler/{crawler_name}'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

# ===================== CHECK LAMBDA FUNCTION ===========================

        self.check_crawler_lambda = _lambda.Function(
            self, 'check-crawler-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/check_crawler/'),
            handler='check_crawler.handler',
            role=check_crawler_lambda_role,
            environment={
                "CRAWLER_NAME": crawler_name
            }
        )


# ========================================================================
# ======================= LOAD LAMBDA FUNCTION ===========================
# ======================================================================== 
        load_lambda_role = _iam.Role(
            self, 'load-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        load_lambda_policy = _iam.Policy(
            self, 'load-lambda-role-policy',
            roles=[load_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                        "s3:PutObject",
                        "s3:Delete*"
                    ],
                    resources= [
                        curated_bucket.bucket_arn,
                        f'{curated_bucket.bucket_arn}/*',
                        support_bucket.bucket_arn,
                        f'{support_bucket.bucket_arn}/*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:ListSecrets",
                        "secretsmanager:ListSecretVersionIds"
                    ],
                    resources= [
                        '*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "glue:*",
                        "athena:*"
                    ],
                    resources= [
                        '*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

        self.load_lambda = _lambda.Function(
            self, 'load-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/load/'),
            handler='load.handler',
            role=load_lambda_role,
            layers = [wrangler_lambda_layer],
            environment={
                "OPENSEARCH_DOMAIN": opensearch_domain.domain_endpoint,
                "CURATED_BUCKET_NAME": curated_bucket.bucket_name,
                "SUPPORT_BUCKET_NAME": support_bucket.bucket_name
            }
        )
