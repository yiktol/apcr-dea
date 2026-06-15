"""
CDK Stack: End-to-end data pipeline covering the 5 V's of Big Data.

Components:
- Kinesis Data Stream (Velocity/Ingestion)
- Lambda Transform (Velocity/Processing)
- S3 Data Lake with zones (Volume/Storage)
- Glue Catalog + Athena workgroup (Value/Analytics)
- Macie + EventBridge + Masking Lambda (Veracity/Governance)
- VPC with private subnets (Networking/Security)
"""
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
)
from constructs import Construct


class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ============================================================
        # NETWORKING (Security Layer)
        # ============================================================
        vpc = ec2.Vpc(
            self,
            "PipelineVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # S3 VPC Endpoint (gateway - free)
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # ============================================================
        # INGESTION (Velocity) - Kinesis Data Stream
        # ============================================================
        stream = kinesis.Stream(
            self,
            "IngestionStream",
            stream_name="dea-ingestion-stream",
            shard_count=2,
            retention_period=Duration.hours(24),
            stream_mode=kinesis.StreamMode.PROVISIONED,
        )

        # ============================================================
        # STORAGE (Volume) - S3 Data Lake with zones
        # ============================================================
        data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=None,  # Auto-generated
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

        # Masked output bucket
        masked_bucket = s3.Bucket(
            self,
            "MaskedBucket",
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
                s3.LifecycleRule(
                    expiration=Duration.days(7),
                ),
            ],
        )

        # ============================================================
        # PROCESSING (Velocity) - Lambda Transform
        # ============================================================
        transform_log_group = logs.LogGroup(
            self,
            "TransformLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        transform_fn = lambda_.Function(
            self,
            "TransformFunction",
            function_name="dea-stream-transform",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_fns/transform"),
            timeout=Duration.minutes(2),
            memory_size=256,
            environment={
                "DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
                "RAW_PREFIX": "raw/",
                "CURATED_PREFIX": "curated/",
                "ERRORS_PREFIX": "errors/",
            },
            log_group=transform_log_group,
        )

        # Grant Lambda write access to S3
        data_lake_bucket.grant_read_write(transform_fn)

        # Connect Kinesis → Lambda
        transform_fn.add_event_source(
            event_sources.KinesisEventSource(
                stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=Duration.seconds(60),
                retry_attempts=3,
                parallelization_factor=1,
            )
        )

        # ============================================================
        # ANALYTICS (Value) - Glue Catalog + Athena
        # ============================================================
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="dea_data_lake",
                description="Data Engineer Associate - Data Lake",
            ),
        )

        # Glue table for curated data (Parquet)
        glue.CfnTable(
            self,
            "CuratedTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="dea_data_lake",
            table_input=glue.CfnTable.TableInputProperty(
                name="events_curated",
                description="Curated event data in Parquet format",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "compressionType": "none",
                },
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
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="device_id", type="string"),
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

        # Raw JSON table for Athena queries
        glue.CfnTable(
            self,
            "RawTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="dea_data_lake",
            table_input=glue.CfnTable.TableInputProperty(
                name="events_raw",
                description="Raw event data in JSON format",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "json",
                },
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
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="device_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="payload", type="string"),
                    ],
                ),
            ),
        ).add_dependency(glue_database)

        # Athena workgroup
        athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name="dea-pipeline",
            description="Data Pipeline Analytics Workgroup",
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

        # Athena saved queries
        athena.CfnNamedQuery(
            self,
            "RepairCuratedPartitions",
            database="dea_data_lake",
            work_group="dea-pipeline",
            name="Repair events_curated partitions",
            description="Run after Glue ETL job to register new partitions in the catalog",
            query_string="MSCK REPAIR TABLE dea_data_lake.events_curated;",
        )

        athena.CfnNamedQuery(
            self,
            "CountAllEvents",
            database="dea_data_lake",
            work_group="dea-pipeline",
            name="Count all events (raw)",
            description="Quick count of all records in the raw events table",
            query_string="SELECT COUNT(*) as total_events FROM dea_data_lake.events_raw;",
        )

        athena.CfnNamedQuery(
            self,
            "EventsBySource",
            database="dea_data_lake",
            work_group="dea-pipeline",
            name="Events by source",
            description="Breakdown of events by source type",
            query_string="SELECT source, COUNT(*) as count FROM dea_data_lake.events_raw GROUP BY source ORDER BY count DESC;",
        )

        athena.CfnNamedQuery(
            self,
            "CuratedEventsByDay",
            database="dea_data_lake",
            work_group="dea-pipeline",
            name="Curated events by day (partitioned)",
            description="Query curated Parquet data using partition pruning",
            query_string="SELECT year, month, day, COUNT(*) as count FROM dea_data_lake.events_curated GROUP BY year, month, day ORDER BY year, month, day;",
        )

        # ============================================================
        # ETL (Volume → Value) - Glue Job: Raw JSON → Curated Parquet
        # ============================================================

        # Upload Glue script to S3 via Asset
        glue_script_asset = s3_assets.Asset(
            self,
            "GlueScriptAsset",
            path="glue_scripts/raw_to_curated.py",
        )

        # Glue job IAM role
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            role_name="dea-glue-etl-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        data_lake_bucket.grant_read_write(glue_role)
        glue_script_asset.grant_read(glue_role)

        # Glue ETL job
        glue_job = glue.CfnJob(
            self,
            "RawToCuratedJob",
            name="dea-raw-to-curated",
            description="Converts raw JSON to curated Parquet (partitioned, deduplicated)",
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
            timeout=10,  # minutes
            max_retries=0,
        )

        # ============================================================
        # GOVERNANCE (Veracity) - Macie + EventBridge + Masking Lambda
        # ============================================================
        masker_log_group = logs.LogGroup(
            self,
            "MaskerLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        masker_fn = lambda_.Function(
            self,
            "MaskerFunction",
            function_name="dea-pii-masker",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_fns/masker"),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "SOURCE_BUCKET": data_lake_bucket.bucket_name,
                "MASKED_BUCKET": masked_bucket.bucket_name,
            },
            log_group=masker_log_group,
        )

        # Grant masker access to both buckets
        data_lake_bucket.grant_read(masker_fn)
        masked_bucket.grant_write(masker_fn)

        # EventBridge rule: Macie findings → Masker Lambda
        macie_rule = events.Rule(
            self,
            "MacieFindingRule",
            rule_name="dea-macie-pii-detected",
            description="Triggers PII masking when Macie detects sensitive data",
            event_pattern=events.EventPattern(
                source=["aws.macie"],
                detail_type=["Macie Finding"],
                detail={
                    "type": [{"prefix": "SensitiveData"}],
                },
            ),
        )
        macie_rule.add_target(targets.LambdaFunction(masker_fn))

        # Macie session - enables Macie in the account
        macie_session = cdk.CfnResource(
            self,
            "MacieSession",
            type="AWS::Macie::Session",
            properties={
                "Status": "ENABLED",
            },
        )

        # ============================================================
        # IAM - Pipeline execution role for the web app
        # ============================================================
        pipeline_role = iam.Role(
            self,
            "PipelineAppRole",
            role_name="dea-pipeline-pipeline-role",
            assumed_by=iam.AccountPrincipal(cdk.Aws.ACCOUNT_ID),
            description="Role for the pipeline web application",
        )

        # Kinesis permissions (produce + describe)
        stream.grant_read_write(pipeline_role)

        # S3 permissions
        data_lake_bucket.grant_read_write(pipeline_role)
        masked_bucket.grant_read(pipeline_role)
        athena_results_bucket.grant_read_write(pipeline_role)

        # Athena + Glue permissions
        pipeline_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                    "glue:GetTable",
                    "glue:GetDatabase",
                    "glue:GetPartitions",
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun",
                ],
                resources=["*"],
            )
        )

        # CloudWatch read (for monitoring)
        pipeline_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "cloudwatch:GetMetricData",
                    "cloudwatch:GetMetricStatistics",
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords",
                    "kinesis:ListShards",
                    "lambda:GetFunction",
                    "lambda:ListEventSourceMappings",
                    "logs:GetLogEvents",
                    "logs:DescribeLogStreams",
                ],
                resources=["*"],
            )
        )

        # ============================================================
        # OUTPUTS
        # ============================================================
        CfnOutput(self, "OutputStreamName", value=stream.stream_name, export_name="StreamName")
        CfnOutput(self, "OutputStreamArn", value=stream.stream_arn)
        CfnOutput(self, "OutputDataLakeBucket", value=data_lake_bucket.bucket_name)
        CfnOutput(self, "OutputMaskedBucket", value=masked_bucket.bucket_name)
        CfnOutput(self, "OutputAthenaResultsBucket", value=athena_results_bucket.bucket_name)
        CfnOutput(self, "OutputAthenaWorkgroup", value="dea-pipeline")
        CfnOutput(self, "OutputGlueDatabase", value="dea_data_lake")
        CfnOutput(self, "OutputTransformFunctionName", value=transform_fn.function_name)
        CfnOutput(self, "OutputMaskerFunctionName", value=masker_fn.function_name)
        CfnOutput(self, "OutputGlueJobName", value="dea-raw-to-curated")
        CfnOutput(self, "OutputPipelineRoleArn", value=pipeline_role.role_arn)
        CfnOutput(self, "OutputVpcId", value=vpc.vpc_id)
