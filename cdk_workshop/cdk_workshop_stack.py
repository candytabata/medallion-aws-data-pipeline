from datetime import datetime
from constructs import Construct
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue_alpha as glue_alpha,
    aws_s3_deployment as s3_deploy,
    aws_secretsmanager as secretsmanager,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)

# Upload raw data into the Bronze bucket using a CDK asset deployment
today = datetime.today()
hive_prefix = f"year={today.year}/month={today.month:02d}/day={today.day:02d}/"


class StorageStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 buckets for the Bronze, Silver, and Gold layers with appropriate configurations

        self.bronze_bucket = s3.Bucket(self, "Bronze-amzn-2026-03",
            removal_policy=RemovalPolicy.DESTROY, # In production we want to retain the data, but for testing we can set it to DESTROY to avoid incurring costs
            auto_delete_objects=True, # Disable this in production to avoid accidental data loss, but for testing it allows us to clean up the bucket when the stack is destroyed
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="Move to Glacier after 90 days",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(90)
                        )
                    ]
                )
            ]
        )

        self.silver_bucket = s3.Bucket(self, "Silver-amzn-2026-03",
            removal_policy=RemovalPolicy.DESTROY,  # In production this will be RETAIN, but for testing we can set it to DESTROY to avoid incurring costs
            auto_delete_objects=True, # Disable this in production to avoid accidental data loss, but for testing it allows us to clean up the bucket when the stack is destroyed
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        self.gold_bucket = s3.Bucket(self, "Gold-amzn-2026-03",
            removal_policy=RemovalPolicy.DESTROY, # In production this will be RETAIN, but for testing we can set it to DESTROY to avoid incurring costs
            auto_delete_objects=True, # Disable this in production to avoid accidental data loss, but for testing it allows us to clean up the bucket when the stack is destroyed
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        global hive_prefix

        s3_deploy.BucketDeployment(self, "UploadTransactionsData",
            sources=[s3_deploy.Source.asset("data/bronze/")],
            destination_bucket=self.bronze_bucket,
            destination_key_prefix=hive_prefix
        )
        



class EtlStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, bronze_bucket: s3.Bucket, silver_bucket: s3.Bucket, gold_bucket: s3.Bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Glue ETL Role with permissions to read from Bronze and write to Silver and Gold buckets
        glue_etl_role = iam.Role(self, "GlueETLRole",
                                    assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                    description="Glue role for ETL jobs reading S3 data",
                                    managed_policies=[
                                        iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
                                    ]                         
        )

        bronze_bucket.grant_read(glue_etl_role)
        silver_bucket.grant_read_write(glue_etl_role)
        gold_bucket.grant_read_write(glue_etl_role)

        # Script bucket to store Glue ETL scripts, with permissions for the Glue ETL role to read from it
        self.script_bucket = s3.Bucket(self, "scripts-amzn-2026-03",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True, # Disable this in production to avoid accidental data loss, but for testing it allows us to clean up the bucket when the stack is destroyed
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True
        )

        self.script_bucket.grant_read(glue_etl_role)

        # Upload the Glue ETL script to the script bucket using a CDK asset deployment
        s3_deploy.BucketDeployment(self, "UploadGlueETLScripts",
            sources=[s3_deploy.Source.asset("scripts/glue/")],
            destination_bucket=self.script_bucket,
            destination_key_prefix="GlueScripts/"
        )

        # Create a Secrets Manager secret to store the hashing salt, with permissions for the Glue ETL role to read it
        salt_secret = secretsmanager.Secret(self, "HashingSalt",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                exclude_punctuation=True,   
                password_length=32 
            )
        )

        salt_secret.grant_read(glue_etl_role)

        # Glue ETL Job definition
        global hive_prefix

        bronze_to_silver_job = glue_alpha.PySparkEtlJob(self,
            "BronzeToSilverJob",
            job_name="TransactionsBronzeToSilverJob",
            role=glue_etl_role,
            script=glue_alpha.Code.from_bucket(self.script_bucket, "GlueScripts/bronze_to_silver.py"),
            enable_metrics=False,
            enable_observability_metrics=False,
            worker_type=glue_alpha.WorkerType.G_1X,
            number_of_workers=2,
            default_arguments={
                "--BRONZE_BUCKET": bronze_bucket.bucket_name,
                "--SILVER_BUCKET": silver_bucket.bucket_name,
                "--SALT_SECRET_ARN": salt_secret.secret_arn,
            }
        )

        validate_silver_job = glue_alpha.PySparkEtlJob(self,
            "ValidateSilverJob",
            job_name="TransactionsValidateSilverJob",
            role=glue_etl_role,
            script=glue_alpha.Code.from_bucket(self.script_bucket, "GlueScripts/validate_silver.py"),
            enable_metrics=False,
            enable_observability_metrics=False,
            worker_type=glue_alpha.WorkerType.G_1X,
            number_of_workers=2,
            default_arguments={
                "--SILVER_BUCKET": silver_bucket.bucket_name,
                "--GOLD_BUCKET": gold_bucket.bucket_name,
            }
        )

        silver_to_gold_job = glue_alpha.PySparkEtlJob(self,
            "SilverToGoldJob",
            job_name="TransactionsSilverToGoldJob",
            role=glue_etl_role,
            script=glue_alpha.Code.from_bucket(self.script_bucket, "GlueScripts/silver_to_gold.py"),
            enable_metrics=False,
            enable_observability_metrics=False,
            worker_type=glue_alpha.WorkerType.G_1X,
            number_of_workers=2,
            default_arguments={
                "--SILVER_BUCKET": silver_bucket.bucket_name,
                "--GOLD_BUCKET": gold_bucket.bucket_name,
            }
        )

        # Expose job names derived from the job objects — the OrchestrationStack
        # references these to wire Step Functions to the correct Glue jobs.
        self.bronze_to_silver_job_name = bronze_to_silver_job.job_name
        self.validate_silver_job_name  = validate_silver_job.job_name
        self.silver_to_gold_job_name   = silver_to_gold_job.job_name


class OrchestrationStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 bronze_to_silver_job_name: str,
                 validate_silver_job_name: str,
                 silver_to_gold_job_name: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Terminal states ───────────────────────────────────────────────────
        pipeline_failed = sfn.Fail(self, "PipelineFailed",
            error="PipelineFailed",
            cause="A Glue job failed. Check CloudWatch logs for details.",
        )

        pipeline_succeeded = sfn.Succeed(self, "PipelineSucceeded")

        # Bronze → Silver step with retry and error handling.
        bronze_to_silver = sfn_tasks.GlueStartJobRun(self, "BronzeToSilver",
            glue_job_name=bronze_to_silver_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.bronze_to_silver_result",
        )
        bronze_to_silver.add_retry(
            errors=["Glue.ConcurrentRunsExceededException", "States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2,
        )
        bronze_to_silver.add_catch(pipeline_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Validate Silver data before proceeding to the next step to ensure data quality and prevent bad data.
        validate_silver = sfn_tasks.GlueStartJobRun(self, "ValidateSilver",
            glue_job_name=validate_silver_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.validate_silver_result",
        )
        validate_silver.add_retry(
            errors=["Glue.ConcurrentRunsExceededException"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2,
        )
        validate_silver.add_catch(pipeline_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Silver → Gold step with retry and error handling.

        silver_to_gold = sfn_tasks.GlueStartJobRun(self, "SilverToGold",
            glue_job_name=silver_to_gold_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.silver_to_gold_result",
        )
        silver_to_gold.add_retry(
            errors=["Glue.ConcurrentRunsExceededException", "States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2,
        )
        silver_to_gold.add_catch(pipeline_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # State machine logic: Bronze → Silver → Validate Silver → Gold.
        definition = (
            bronze_to_silver
            .next(validate_silver)
            .next(silver_to_gold)
            .next(pipeline_succeeded)
        )

        sfn.StateMachine(self, "TransactionsPipeline",
            state_machine_name="TransactionsPipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
        )




