"""
Session 1 Architecture Diagrams
Generates architecture diagrams illustrating the core data pipeline concept
from Session 1: Ingestion → Processing → Storage → Analytics → Governance

Diagrams:
1. Overall Pipeline (end-to-end)
2. Ingestion Layer (Velocity - Kinesis + producers)
3. Processing Layer (Velocity - Lambda transformation)
4. Storage Layer (Volume - S3 Data Lake + Lake Formation)
5. Analytics Layer (Value - Redshift + Athena)
6. Governance Layer (Veracity - Macie + EventBridge)
7. Networking Layer (VPC + Security)
"""
import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import KinesisDataStreams, Glue, Athena, Redshift, LakeFormation, ManagedStreamingForKafka
from diagrams.aws.compute import Lambda, EC2
from diagrams.aws.database import Dynamodb, RDS
from diagrams.aws.storage import S3
from diagrams.aws.integration import Eventbridge
from diagrams.aws.network import VPC, NATGateway, PrivateSubnet, PublicSubnet
from diagrams.aws.security import IAM
from diagrams.custom import Custom

# Icon paths
ICONS = os.path.abspath("../../aws-icons/Architecture-Service-Icons_04302026")
MACIE_ICON = f"{ICONS}/Arch_Security-Identity/48/Arch_Amazon-Macie_48.png"
CLOUDWATCH_ICON = f"{ICONS}/Arch_Management-Tools/48/Arch_Amazon-CloudWatch_48.png"
EMR_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-EMR_48.png"
LAKE_FORMATION_ICON = f"{ICONS}/Arch_Analytics/48/Arch_AWS-Lake-Formation_48.png"
IAM_ICON = f"{ICONS}/Arch_Security-Identity/48/Arch_AWS-Identity-and-Access-Management_48.png"
MSK_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-Managed-Streaming-for-Apache-Kafka_48.png"
FIREHOSE_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-Data-Firehose_48.png"
GLUE_ICON = f"{ICONS}/Arch_Analytics/48/Arch_AWS-Glue_48.png"
REDSHIFT_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-Redshift_48.png"
ATHENA_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-Athena_48.png"
OPENSEARCH_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-OpenSearch-Service_48.png"

# Cluster styles
INGESTION_CLUSTER = {"bgcolor": "#fff3e0", "style": "rounded", "pencolor": "#e65100", "penwidth": "2", "label": "Ingestion (Velocity)"}
PROCESSING_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "2", "label": "Processing (Velocity)"}
STORAGE_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2", "label": "Storage (Volume)"}
ANALYTICS_CLUSTER = {"bgcolor": "#f3e5f5", "style": "rounded", "pencolor": "#6a1b9a", "penwidth": "2", "label": "Analytics (Value)"}
GOVERNANCE_CLUSTER = {"bgcolor": "#fce4ec", "style": "rounded", "pencolor": "#c62828", "penwidth": "2", "label": "Governance (Veracity)"}
SOURCES_CLUSTER = {"bgcolor": "#eceff1", "style": "rounded", "pencolor": "#37474f", "penwidth": "1.5", "label": "Data Sources (Variety)"}
VPC_CLUSTER = {"bgcolor": "#e8eaf6", "style": "rounded", "pencolor": "#283593", "penwidth": "2", "label": "VPC"}
PRIVATE_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5", "label": "Private Subnet"}
PUBLIC_CLUSTER = {"bgcolor": "#fff9c4", "style": "rounded", "pencolor": "#f57f17", "penwidth": "1.5", "label": "Public Subnet"}

base_graph_attr = {
    "fontsize": "14",
    "bgcolor": "#fafafa",
    "pad": "0.8",
    "splines": "spline",
    "nodesep": "1.0",
    "ranksep": "1.2",
    "fontname": "Helvetica",
}

# ============================================================
# DIAGRAM 1: Overall Pipeline (End-to-End)
# ============================================================
print("Generating: 01_overall_pipeline...")
with Diagram("",
             filename="01_overall_pipeline",
             show=False, direction="LR",
             graph_attr={**base_graph_attr, "ranksep": "1.5"}):

    with Cluster("Data Sources\n(Variety)", graph_attr=SOURCES_CLUSTER):
        iot = EC2("IoT Devices")
        apps = EC2("Applications")
        db = RDS("Databases")

    with Cluster("Ingestion\n(Velocity)", graph_attr=INGESTION_CLUSTER):
        kinesis = KinesisDataStreams("Kinesis\nData Streams")

    with Cluster("Processing\n(Velocity)", graph_attr=PROCESSING_CLUSTER):
        transform = Lambda("Lambda\nTransform")

    with Cluster("Storage\n(Volume)", graph_attr=STORAGE_CLUSTER):
        s3_raw = S3("S3 Raw")
        s3_curated = S3("S3 Curated")

    with Cluster("Analytics\n(Value)", graph_attr=ANALYTICS_CLUSTER):
        athena = Athena("Athena")
        redshift = Redshift("Redshift")

    with Cluster("Governance\n(Veracity)", graph_attr=GOVERNANCE_CLUSTER):
        macie = Custom("Macie", MACIE_ICON)
        eventbridge = Eventbridge("EventBridge")

    # Data flow
    iot >> Edge(color="#e65100", style="bold") >> kinesis
    apps >> Edge(color="#e65100", style="bold") >> kinesis
    db >> Edge(color="#e65100", style="bold") >> kinesis

    kinesis >> Edge(label="Real-time", color="#1565c0", style="bold") >> transform
    transform >> Edge(label="Parquet", color="#2e7d32", style="bold") >> s3_raw
    s3_raw >> Edge(label="ETL", color="#2e7d32", style="bold") >> s3_curated

    s3_curated >> Edge(color="#6a1b9a", style="bold") >> athena
    s3_curated >> Edge(color="#6a1b9a", style="bold") >> redshift

    s3_raw >> Edge(label="Scan", color="#c62828", style="dashed") >> macie
    macie >> Edge(label="Finding", color="#c62828", style="bold") >> eventbridge


# ============================================================
# DIAGRAM 2: Ingestion Layer Detail
# ============================================================
print("Generating: 02_ingestion_layer...")
with Diagram("",
             filename="02_ingestion_layer",
             show=False, direction="LR",
             graph_attr=base_graph_attr):

    with Cluster("Data Producers", graph_attr=SOURCES_CLUSTER):
        iot = EC2("IoT Sensors\n(clickstream)")
        mobile = EC2("Mobile Apps\n(user events)")
        logs = EC2("Server Logs\n(application)")

    with Cluster("Kinesis Data Streams", graph_attr=INGESTION_CLUSTER):
        shard1 = KinesisDataStreams("Shard 1")
        shard2 = KinesisDataStreams("Shard 2")
        shard3 = KinesisDataStreams("Shard 3")

    with Cluster("Consumers", graph_attr=PROCESSING_CLUSTER):
        lambda_fn = Lambda("Lambda\n(Transform)")
        emr = Custom("EMR\n(Batch)", EMR_ICON)

    iot >> Edge(label="Partition Key: device_id", color="#e65100", style="bold") >> shard1
    mobile >> Edge(label="Partition Key: user_id", color="#e65100", style="bold") >> shard2
    logs >> Edge(label="Partition Key: app_name", color="#e65100", style="bold") >> shard3

    shard1 >> Edge(label="2 MB/s per shard", color="#1565c0", style="bold") >> lambda_fn
    shard2 >> Edge(color="#1565c0", style="bold") >> lambda_fn
    shard3 >> Edge(color="#1565c0", style="bold") >> emr


# ============================================================
# DIAGRAM 3: Processing Layer Detail
# ============================================================
print("Generating: 03_processing_layer...")
with Diagram("",
             filename="03_processing_layer",
             show=False, direction="LR",
             graph_attr=base_graph_attr):

    kinesis = KinesisDataStreams("Kinesis\nData Streams")

    with Cluster("Serverless Processing", graph_attr=PROCESSING_CLUSTER):
        lambda_fn = Lambda("Lambda\nTransform\n• Validate\n• Enrich\n• Convert to Parquet")

    with Cluster("Data Lake", graph_attr=STORAGE_CLUSTER):
        s3_raw = S3("s3://data-lake/raw/\nyyyy/mm/dd/")
        s3_err = S3("s3://data-lake/errors/")

    kinesis >> Edge(label="Event Trigger\n(batch of records)", color="#1565c0", style="bold") >> lambda_fn
    lambda_fn >> Edge(label="Valid Records", color="#2e7d32", style="bold") >> s3_raw
    lambda_fn >> Edge(label="Failed Records", color="#c62828", style="dashed") >> s3_err


# ============================================================
# DIAGRAM 4: Storage Layer Detail (Volume)
# ============================================================
print("Generating: 04_storage_layer...")
with Diagram("",
             filename="04_storage_layer",
             show=False, direction="LR",
             graph_attr=base_graph_attr):

    with Cluster("S3 Data Lake", graph_attr=STORAGE_CLUSTER):
        raw = S3("Raw Zone\n(JSON, CSV)")
        curated = S3("Curated Zone\n(Parquet, partitioned)")
        published = S3("Published Zone\n(aggregated)")

    with Cluster("Catalog & Governance", graph_attr={**GOVERNANCE_CLUSTER, "label": "Lake Formation"}):
        lake = Custom("Lake Formation\n(Permissions)", LAKE_FORMATION_ICON)
        glue_cat = Custom("Glue Catalog\n(Metadata)", GLUE_ICON)

    with Cluster("ETL", graph_attr=PROCESSING_CLUSTER):
        glue_job = Custom("Glue ETL\n(Spark)", GLUE_ICON)

    raw >> Edge(label="Crawl & Catalog", color="#6a1b9a", style="dashed") >> glue_cat
    raw >> Edge(label="Transform", color="#1565c0", style="bold") >> glue_job
    glue_job >> Edge(label="Optimized Parquet", color="#2e7d32", style="bold") >> curated
    curated >> Edge(label="Aggregate", color="#2e7d32", style="bold") >> published
    lake >> Edge(label="Column-level\naccess control", color="#c62828", style="bold") >> curated


# ============================================================
# DIAGRAM 5: Analytics Layer Detail (Value)
# ============================================================
print("Generating: 05_analytics_layer...")
with Diagram("",
             filename="05_analytics_layer",
             show=False, direction="LR",
             graph_attr=base_graph_attr):

    with Cluster("Data Lake", graph_attr=STORAGE_CLUSTER):
        s3 = S3("S3 Curated Zone\n(Parquet)")

    with Cluster("Query Engines", graph_attr=ANALYTICS_CLUSTER):
        athena = Custom("Athena\n(Ad-hoc SQL)", ATHENA_ICON)
        redshift = Custom("Redshift\n(Data Warehouse)", REDSHIFT_ICON)
        opensearch = Custom("OpenSearch\n(Log Analytics)", OPENSEARCH_ICON)

    with Cluster("Consumers", graph_attr={"bgcolor": "#e0f2f1", "style": "rounded", "pencolor": "#00695c", "penwidth": "1.5", "label": "Business Users"}):
        bi = EC2("BI Dashboards\n(QuickSight)")
        analysts = EC2("Data Analysts\n(Notebooks)")

    s3 >> Edge(label="Serverless Query", color="#6a1b9a", style="bold") >> athena
    s3 >> Edge(label="COPY / Spectrum", color="#6a1b9a", style="bold") >> redshift
    s3 >> Edge(label="Index", color="#6a1b9a", style="dashed") >> opensearch

    athena >> Edge(color="#00695c", style="bold") >> analysts
    redshift >> Edge(color="#00695c", style="bold") >> bi
    opensearch >> Edge(color="#00695c", style="bold") >> bi


# ============================================================
# DIAGRAM 6: Governance Layer Detail (Veracity)
# ============================================================
print("Generating: 06_governance_layer...")
with Diagram("",
             filename="06_governance_layer",
             show=False, direction="LR",
             graph_attr=base_graph_attr):

    with Cluster("Data Lake", graph_attr=STORAGE_CLUSTER):
        s3 = S3("S3 Bucket\n(PII data)")

    with Cluster("PII Detection", graph_attr=GOVERNANCE_CLUSTER):
        macie = Custom("Amazon Macie\n(ML-powered scan)", MACIE_ICON)

    eventbridge = Eventbridge("EventBridge\n(Default Bus)")

    with Cluster("Automated Response", graph_attr=PROCESSING_CLUSTER):
        masker = Lambda("Masking Lambda\n• Tokenize SSN\n• Hash emails\n• Redact names")

    with Cluster("Output", graph_attr=STORAGE_CLUSTER):
        s3_clean = S3("S3 Clean Zone\n(PII masked)")

    s3 >> Edge(label="Continuous\nDiscovery Job", color="#c62828", style="bold") >> macie
    macie >> Edge(label="SensitiveData:S3Object\nfinding event", color="#c62828", style="bold") >> eventbridge
    eventbridge >> Edge(label="Rule target\n(real-time)", color="#1565c0", style="bold") >> masker
    masker >> Edge(label="Masked output", color="#2e7d32", style="bold") >> s3_clean


# ============================================================
# DIAGRAM 7: Networking Layer (VPC Security)
# ============================================================
print("Generating: 07_networking_layer...")
with Diagram("",
             filename="07_networking_layer",
             show=False, direction="LR",
             graph_attr=base_graph_attr):

    internet = EC2("Internet")

    with Cluster("VPC (10.0.0.0/16)", graph_attr=VPC_CLUSTER):
        with Cluster("Public Subnet", graph_attr=PUBLIC_CLUSTER):
            nat = NATGateway("NAT Gateway")

        with Cluster("Private Subnet (Processing)", graph_attr=PRIVATE_CLUSTER):
            ec2_proc = EC2("EC2 Processing\n(Data Pipeline)")
            lambda_fn = Lambda("Lambda\n(Transform)")

    with Cluster("AWS Services", graph_attr=STORAGE_CLUSTER):
        s3 = S3("S3\n(VPC Endpoint)")
        kinesis = KinesisDataStreams("Kinesis")

    internet >> Edge(label="Inbound blocked\n(no IGW route)", color="#c62828", style="dashed") >> ec2_proc
    ec2_proc >> Edge(label="Outbound via NAT", color="#1565c0", style="bold") >> nat
    nat >> Edge(color="#1565c0", style="bold") >> internet

    ec2_proc >> Edge(label="Private link\n(no internet)", color="#2e7d32", style="bold") >> s3
    lambda_fn >> Edge(label="VPC Endpoint", color="#2e7d32", style="bold") >> s3
    lambda_fn >> Edge(color="#1565c0", style="bold") >> kinesis


print("\n✅ All 7 architecture diagrams generated successfully!")
print("Files created:")
for f in sorted(os.listdir(".")):
    if f.endswith(".png"):
        print(f"  • {f}")
