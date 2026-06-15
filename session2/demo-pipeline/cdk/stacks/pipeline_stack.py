"""
CDK Stack: ETL Pipeline Orchestration — Session 2

Covers all four task statements:
- Task 1.1: Data ingestion (Kinesis Data Streams + Data Firehose)
- Task 1.2: Transform & process (Glue ETL: JSON → Parquet)
- Task 1.3: Orchestrate pipelines (Step Functions workflow)
- Task 1.4: Programming concepts (CI/CD, Lambda, Athena)
"""
import json
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
)
from constructs import Construct


class EtlPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ============================================================
        # STORAGE — S3 Data Lake with zones
        # ============================================================
        data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireRawAfter30Days",
                    prefix="raw/",
                    expiration=Duration.days(30),
                ),
            ],
        )

        # Firehose destination bucket (separate prefix)
        firehose_bucket = s3.Bucket(
            self,
            "FirehoseBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # Athena results bucket
        athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(7)),
            ],
        )

        # ============================================================
        # INGESTION — Kinesis Data Streams (Task 1.1)
        # ============================================================
        stream = kinesis.Stream(
            self,
            "IngestionStream",
            stream_name="dea-s2-ingestion-stream",
            shard_count=2,
            retention_period=Duration.hours(24),
            stream_mode=kinesis.StreamMode.PROVISIONED,
        )

        # ============================================================
        # INGESTION — Data Firehose (Task 1.1 comparison)
        # ============================================================
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        firehose_bucket.grant_read_write(firehose_role)

        delivery_stream = firehose.CfnDeliveryStream(
            self,
            "DeliveryStream",
            delivery_stream_name="dea-s2-firehose",
            delivery_stream_type="DirectPut",
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=firehose_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix="delivered/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
                error_output_prefix="errors/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=1,
                ),
                compression_format="GZIP",
            ),
        )

        # ============================================================
        # PROCESSING — Lambda Consumer (Kinesis → S3 raw zone)
        # ============================================================
        consumer_log_group = logs.LogGroup(
            self,
            "ConsumerLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        consumer_fn = lambda_.Function(
            self,
            "ConsumerFunction",
            function_name="dea-s2-stream-consumer",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_fns/consumer"),
            timeout=Duration.minutes(2),
            memory_size=256,
            environment={
                "DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
                "RAW_PREFIX": "raw/",
            },
            log_group=consumer_log_group,
        )

        data_lake_bucket.grant_read_write(consumer_fn)

        consumer_fn.add_event_source(
            event_sources.KinesisEventSource(
                stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=Duration.seconds(30),
                retry_attempts=3,
                parallelization_factor=2,
            )
        )

        # ============================================================
        # ORCHESTRATION — Step Functions ETL Workflow (Task 1.3)
        # ============================================================

        # Validator Lambda
        validator_fn = lambda_.Function(
            self,
            "ValidatorFunction",
            function_name="dea-s2-validator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_fns/validator"),
            timeout=Duration.minutes(1),
            memory_size=256,
            environment={
                "DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
                "RAW_PREFIX": "raw/",
            },
        )
        data_lake_bucket.grant_read(validator_fn)

        # Notifier Lambda
        notification_topic = sns.Topic(
            self,
            "PipelineNotifications",
            topic_name="dea-s2-pipeline-notifications",
            display_name="DEA Pipeline Notifications",
        )

        notifier_fn = lambda_.Function(
            self,
            "NotifierFunction",
            function_name="dea-s2-notifier",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_fns/notifier"),
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "SNS_TOPIC_ARN": notification_topic.topic_arn,
            },
        )
        notification_topic.grant_publish(notifier_fn)

        # Glue ETL Job
        glue_script_asset = s3_assets.Asset(
            self,
            "GlueScriptAsset",
            path="glue_scripts/transform_etl.py",
        )

        glue_role = iam.Role(
            self,
            "GlueJobRole",
            role_name="dea-s2-glue-etl-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        data_lake_bucket.grant_read_write(glue_role)
        glue_script_asset.grant_read(glue_role)

        glue_job = glue.CfnJob(
            self,
            "TransformEtlJob",
            name="dea-s2-transform-etl",
            description="Session 2: JSON → Parquet (partitioned, deduplicated, compressed)",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=glue_script_asset.s3_object_url,
            ),
            default_arguments={
                "--job-language": "python",
                "--DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
            },
            glue_version="4.0",
            number_of_workers=2,
            worker_type="G.1X",
            timeout=10,
            max_retries=0,
        )

        # Step Functions State Machine
        validate_step = sfn_tasks.LambdaInvoke(
            self,
            "ValidateData",
            lambda_function=validator_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=True,
        )

        run_glue_job = sfn_tasks.GlueStartJobRun(
            self,
            "RunGlueETL",
            glue_job_name="dea-s2-transform-etl",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
            }),
            result_path="$.glueResult",
        )
        run_glue_job.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0,
        )

        notify_success = sfn_tasks.LambdaInvoke(
            self,
            "NotifySuccess",
            lambda_function=notifier_fn,
            payload=sfn.TaskInput.from_object({
                "status": "SUCCESS",
                "message": "ETL pipeline completed successfully",
                "jobName": "dea-s2-transform-etl",
            }),
            result_path="$.notifyResult",
        )

        notify_failure = sfn_tasks.LambdaInvoke(
            self,
            "NotifyFailure",
            lambda_function=notifier_fn,
            payload=sfn.TaskInput.from_object({
                "status": "FAILURE",
                "message": "ETL pipeline failed",
                "jobName": "dea-s2-transform-etl",
            }),
            result_path="$.notifyResult",
        )

        # Error handling
        handle_error = sfn.Pass(self, "HandleError", result=sfn.Result.from_object({"error": "true"}))
        handle_error.next(notify_failure)

        # Build the workflow
        definition = (
            validate_step
            .next(
                run_glue_job
                .add_catch(handle_error, errors=["States.ALL"], result_path="$.error")
            )
            .next(notify_success)
        )

        state_machine = sfn.StateMachine(
            self,
            "EtlOrchestrator",
            state_machine_name="dea-s2-etl-orchestrator",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(30),
            tracing_enabled=True,
        )

        # ============================================================
        # ANALYTICS — Glue Catalog + Athena (Task 1.4)
        # ============================================================
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="dea_s2_pipeline",
                description="Session 2 - ETL Pipeline Orchestration",
            ),
        )

        # Raw table (JSON)
        glue.CfnTable(
            self,
            "RawTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="dea_s2_pipeline",
            table_input=glue.CfnTable.TableInputProperty(
                name="events_raw",
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "json"},
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{data_lake_bucket.bucket_name}/raw/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="event_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_type", type="string"),
                        glue.CfnTable.ColumnProperty(name="source", type="string"),
                        glue.CfnTable.ColumnProperty(name="timestamp", type="string"),
                        glue.CfnTable.ColumnProperty(name="device_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="payload", type="string"),
                    ],
                ),
            ),
        ).add_dependency(glue_database)

        # Curated table (Parquet, partitioned)
        glue.CfnTable(
            self,
            "CuratedTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="dea_s2_pipeline",
            table_input=glue.CfnTable.TableInputProperty(
                name="events_curated",
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "parquet", "compressionType": "snappy"},
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{data_lake_bucket.bucket_name}/curated/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="event_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_type", type="string"),
                        glue.CfnTable.ColumnProperty(name="source", type="string"),
                        glue.CfnTable.ColumnProperty(name="timestamp", type="string"),
                        glue.CfnTable.ColumnProperty(name="device_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="payload", type="string"),
                    ],
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="string"),
                    glue.CfnTable.ColumnProperty(name="month", type="string"),
                    glue.CfnTable.ColumnProperty(name="day", type="string"),
                ],
            ),
        ).add_dependency(glue_database)

        # Athena workgroup
        athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name="dea-s2-pipeline",
            state="ENABLED",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/results/",
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
            ),
        )

        # ============================================================
        # OUTPUTS
        # ============================================================
        CfnOutput(self, "StreamName", value=stream.stream_name)
        CfnOutput(self, "StreamArn", value=stream.stream_arn)
        CfnOutput(self, "FirehoseName", value="dea-s2-firehose")
        CfnOutput(self, "DataLakeBucket", value=data_lake_bucket.bucket_name)
        CfnOutput(self, "FirehoseBucket", value=firehose_bucket.bucket_name)
        CfnOutput(self, "AthenaResultsBucket", value=athena_results_bucket.bucket_name)
        CfnOutput(self, "AthenaWorkgroup", value="dea-s2-pipeline")
        CfnOutput(self, "GlueDatabase", value="dea_s2_pipeline")
        CfnOutput(self, "GlueJobName", value="dea-s2-transform-etl")
        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(self, "StateMachineName", value=state_machine.state_machine_name)
        CfnOutput(self, "ConsumerFunctionName", value=consumer_fn.function_name)
        CfnOutput(self, "SnsTopicArn", value=notification_topic.topic_arn)
