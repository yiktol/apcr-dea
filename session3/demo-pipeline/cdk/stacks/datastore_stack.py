"""
CDK Stack: Data Store Management & Lifecycle — Session 3

Covers Domain 2 task statements:
- Task 2.1: Choose a data store (S3 classes, formats, access patterns)
- Task 2.2: Data cataloging (Glue Crawler, Data Catalog, Schema Registry)
- Task 2.3: Lifecycle management (S3 Lifecycle, DynamoDB TTL, COPY/UNLOAD)
- Task 2.4: Schema design (Glue tables with partitions, sort/dist patterns)
"""
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
    aws_glue as glue,
    aws_athena as athena,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct


class DataStoreStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ============================================================
        # STORAGE — S3 Data Lake with Lifecycle Rules (Task 2.1 & 2.3)
        # ============================================================
        data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                # Raw zone: expire after 60 days
                s3.LifecycleRule(
                    id="RawExpire60Days",
                    prefix="raw/",
                    expiration=Duration.days(60),
                ),
                # Curated zone: transition to IA after 30 days
                s3.LifecycleRule(
                    id="CuratedToIA",
                    prefix="curated/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        ),
                    ],
                ),
                # Archive zone: Glacier IR after 1 day (demo), expire 365
                s3.LifecycleRule(
                    id="ArchiveToGlacier",
                    prefix="archive/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER_INSTANT_RETRIEVAL,
                            transition_after=Duration.days(1),
                        ),
                    ],
                    expiration=Duration.days(365),
                ),
            ],
        )

        # Versioned bucket (separate for versioning demo)
        versioned_bucket = s3.Bucket(
            self,
            "VersionedBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
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
        # DATA CATALOG — Glue Database + Crawler (Task 2.2)
        # ============================================================
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="dea_s3_datastore",
                description="Session 3 - Data Store Management",
            ),
        )

        # Crawler IAM role
        crawler_role = iam.Role(
            self,
            "CrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        data_lake_bucket.grant_read(crawler_role)

        # Glue Crawler — discovers schema from S3
        glue.CfnCrawler(
            self,
            "DataLakeCrawler",
            name="dea-s3-datalake-crawler",
            role=crawler_role.role_arn,
            database_name="dea_s3_datastore",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{data_lake_bucket.bucket_name}/raw/"),
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{data_lake_bucket.bucket_name}/curated/"),
                ],
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVERYTHING",
            ),
        )

        # Pre-defined raw table (JSON)
        glue.CfnTable(
            self,
            "RawTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="dea_s3_datastore",
            table_input=glue.CfnTable.TableInputProperty(
                name="sales_raw",
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
                        glue.CfnTable.ColumnProperty(name="order_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="customer_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="product", type="string"),
                        glue.CfnTable.ColumnProperty(name="category", type="string"),
                        glue.CfnTable.ColumnProperty(name="quantity", type="int"),
                        glue.CfnTable.ColumnProperty(name="price", type="double"),
                        glue.CfnTable.ColumnProperty(name="timestamp", type="string"),
                        glue.CfnTable.ColumnProperty(name="region", type="string"),
                    ],
                ),
            ),
        ).add_dependency(glue_database)

        # Pre-defined curated table (Parquet, partitioned)
        glue.CfnTable(
            self,
            "CuratedTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="dea_s3_datastore",
            table_input=glue.CfnTable.TableInputProperty(
                name="sales_curated",
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
                        glue.CfnTable.ColumnProperty(name="order_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="customer_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="product", type="string"),
                        glue.CfnTable.ColumnProperty(name="category", type="string"),
                        glue.CfnTable.ColumnProperty(name="quantity", type="int"),
                        glue.CfnTable.ColumnProperty(name="price", type="double"),
                        glue.CfnTable.ColumnProperty(name="timestamp", type="string"),
                        glue.CfnTable.ColumnProperty(name="region", type="string"),
                    ],
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="string"),
                    glue.CfnTable.ColumnProperty(name="month", type="string"),
                    glue.CfnTable.ColumnProperty(name="day", type="string"),
                ],
            ),
        ).add_dependency(glue_database)

        # ============================================================
        # ETL — Glue Job (Task 2.2 / 2.4)
        # ============================================================
        glue_script_asset = s3_assets.Asset(
            self,
            "GlueScriptAsset",
            path="glue_scripts/raw_to_curated.py",
        )

        glue_role = iam.Role(
            self,
            "GlueJobRole",
            role_name="dea-s3-glue-etl-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        data_lake_bucket.grant_read_write(glue_role)
        glue_script_asset.grant_read(glue_role)

        glue.CfnJob(
            self,
            "RawToCuratedJob",
            name="dea-s3-raw-to-curated",
            description="Session 3: JSON → Parquet with partitioning by year/month/day",
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

        # ============================================================
        # DynamoDB with TTL (Task 2.3)
        # ============================================================
        orders_table = dynamodb.Table(
            self,
            "OrdersTable",
            table_name="dea-s3-orders",
            partition_key=dynamodb.Attribute(
                name="order_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
            stream=dynamodb.StreamViewType.OLD_IMAGE,
        )

        # Lambda to capture TTL deletions via DynamoDB Streams
        ttl_log_group = logs.LogGroup(
            self,
            "TtlLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        ttl_handler = lambda_.Function(
            self,
            "TtlHandler",
            function_name="dea-s3-ttl-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_fns/ttl_handler"),
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
            },
            log_group=ttl_log_group,
        )

        data_lake_bucket.grant_write(ttl_handler)

        ttl_handler.add_event_source(
            event_sources.DynamoEventSource(
                orders_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=10,
                retry_attempts=2,
            )
        )

        # ============================================================
        # ANALYTICS — Athena Workgroup (Task 2.1 / 2.4)
        # ============================================================
        athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name="dea-s3-datastore",
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
        CfnOutput(self, "DataLakeBucket", value=data_lake_bucket.bucket_name)
        CfnOutput(self, "VersionedBucket", value=versioned_bucket.bucket_name)
        CfnOutput(self, "AthenaResultsBucket", value=athena_results_bucket.bucket_name)
        CfnOutput(self, "AthenaWorkgroup", value="dea-s3-datastore")
        CfnOutput(self, "GlueDatabase", value="dea_s3_datastore")
        CfnOutput(self, "GlueJobName", value="dea-s3-raw-to-curated")
        CfnOutput(self, "CrawlerName", value="dea-s3-datalake-crawler")
        CfnOutput(self, "DynamoTableName", value=orders_table.table_name)
        CfnOutput(self, "TtlHandlerName", value=ttl_handler.function_name)
