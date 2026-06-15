"""
Session 2 Architecture Diagrams
Generates diagrams for each pipeline step using the `diagrams` library.
"""
import os
os.environ["PATH"] += os.pathsep + "/opt/homebrew/bin"
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.iot import IotCore, IotRule, IotCar
from diagrams.aws.analytics import KinesisDataStreams, KinesisDataFirehose, Glue, Athena, Quicksight
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.integration import StepFunctions, SNS, Eventbridge
from diagrams.onprem.client import User

# Common graph attributes
GRAPH_ATTR = {
    "fontsize": "16",
    "fontname": "Helvetica Neue Bold,Helvetica,Arial,sans-serif",
    "bgcolor": "white",
    "pad": "0.3",
    "nodesep": "0.6",
    "ranksep": "1.4",
    "ratio": "0.45",
    "size": "13.33,7.5!",
    "dpi": "200",
    "label": "",
    "splines": "curved",
}

NODE_ATTR = {
    "fontsize": "12",
    "fontname": "Helvetica Neue Bold,Helvetica,Arial,sans-serif",
    "fontcolor": "#232F3E",
    "height": "1.2",
    "width": "1.2",
}

EDGE_ATTR = {
    "fontsize": "11",
    "fontname": "Helvetica Neue,Helvetica,Arial,sans-serif",
    "fontcolor": "#333333",
    "penwidth": "2.5",
    "arrowsize": "1.0",
    "color": "#555555",
    "splines": "curved",
}

# Tight cluster style helper
def cluster_style(label_color, bg_color, border_color):
    return {
        "bgcolor": bg_color,
        "style": "rounded",
        "pencolor": border_color,
        "penwidth": "2",
        "fontsize": "11",
        "fontname": "Helvetica Neue Bold",
        "fontcolor": label_color,
        "margin": "6",
        "labeljust": "l",

    }


# ============================================================
# 01: Ingest
# ============================================================
print("Generating: 01_ingest...")
with Diagram("", filename="01_ingest", show=False, direction="LR",
             graph_attr=GRAPH_ATTR, node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    cars = IotCar("Self-Driving\nCars")

    with Cluster("AWS IoT", graph_attr=cluster_style("#1B9E77", "#E8F5E9", "#1B9E77")):
        iot = IotCore("IoT Core")
        rule = IotRule("IoT Rule")

    with Cluster("Streaming Ingestion", graph_attr=cluster_style("#E65100", "#FFF8E1", "#FF9900")):
        kinesis = KinesisDataStreams("Kinesis Data\nStreams")
        firehose = KinesisDataFirehose("Data Firehose")

    s3 = S3("S3 Raw Zone\n(JSON)")

    cars >> Edge(color="#1B9E77", penwidth="3", label="MQTT publish") >> iot
    iot >> Edge(color="#1B9E77", penwidth="2.5") >> rule
    rule >> Edge(color="#FF9900", penwidth="3", label="route to stream") >> kinesis
    kinesis >> Edge(color="#FF9900", penwidth="3", label="delivery stream") >> firehose
    firehose >> Edge(color="#2E7D32", penwidth="3", label="deliver to S3") >> s3


# ============================================================
# 02: Crawl & Catalog
# ============================================================
print("Generating: 02_crawl_catalog...")
with Diagram("", filename="02_crawl_catalog", show=False, direction="LR",
             graph_attr=GRAPH_ATTR, node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    s3 = S3("S3 Raw Zone")

    with Cluster("AWS Glue", graph_attr=cluster_style("#0D47A1", "#E3F2FD", "#1565C0")):
        crawler = Glue("Crawler")
        catalog = Glue("Data Catalog")

    athena = Athena("Athena")
    emr = Lambda("EMR / Spark")

    s3 >> Edge(color="#1565C0", penwidth="3", label="scan files") >> crawler
    crawler >> Edge(color="#1565C0", penwidth="3", label="write schema\nand partitions") >> catalog
    catalog >> Edge(color="#6A1B9A", penwidth="2.5", label="table metadata") >> athena
    catalog >> Edge(color="#6A1B9A", penwidth="2", style="dashed") >> emr


# ============================================================
# 03: Transform
# ============================================================
print("Generating: 03_transform...")
with Diagram("", filename="03_transform", show=False, direction="LR",
             graph_attr=GRAPH_ATTR, node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    raw = S3("S3 Raw\n(JSON)")

    with Cluster("Glue ETL", graph_attr=cluster_style("#2E7D32", "#E8F5E9", "#4CAF50")):
        glue = Glue("PySpark Job\nDedup + Partition\n+ Compress")

    curated = S3("S3 Curated\n(Parquet)")

    raw >> Edge(color="#E65100", penwidth="3", label="read JSON files") >> glue
    glue >> Edge(color="#2E7D32", penwidth="3", label="write Parquet\n(Snappy compressed)") >> curated


# ============================================================
# 04: Orchestrate
# ============================================================
print("Generating: 04_orchestrate...")
with Diagram("", filename="04_orchestrate", show=False, direction="LR",
             graph_attr=GRAPH_ATTR, node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    trigger = Eventbridge("EventBridge\nSchedule")

    with Cluster("Step Functions Workflow", graph_attr=cluster_style("#E65100", "#FFF8E1", "#FF9900")):
        validate = Lambda("Validate")
        etl = Glue("Glue ETL")
        notify = SNS("Notify")

    trigger >> Edge(color="#FF9900", penwidth="3", label="start execution") >> validate
    validate >> Edge(color="#1565C0", penwidth="3", label="data valid") >> etl
    etl >> Edge(color="#2E7D32", penwidth="3", label="job succeeded") >> notify


# ============================================================
# 05: Query (no clusters — flat flow)
# ============================================================
print("Generating: 05_query...")
with Diagram("", filename="05_query", show=False, direction="LR",
             graph_attr=GRAPH_ATTR, node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    catalog = Glue("Glue Catalog")
    s3_raw = S3("S3 Raw\n(JSON)")
    s3_cur = S3("S3 Curated\n(Parquet)")
    athena = Athena("Amazon\nAthena")
    user = User("Data Engineer")

    catalog >> Edge(color="#6A1B9A", penwidth="2.5", label="table metadata") >> athena
    s3_raw >> Edge(color="#C62828", penwidth="2", style="dashed", label="full scan\n(expensive)") >> athena
    s3_cur >> Edge(color="#2E7D32", penwidth="3", label="column and\npartition pruning") >> athena
    athena >> Edge(color="#232F3E", penwidth="3", label="query results") >> user


# ============================================================
# 06: Visualize (no clusters — flat flow)
# ============================================================
print("Generating: 06_visualize...")
with Diagram("", filename="06_visualize", show=False, direction="LR",
             graph_attr=GRAPH_ATTR, node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    s3 = S3("S3 Curated\n(Parquet)")
    athena = Athena("Athena")
    qs = Quicksight("QuickSight")
    user = User("Stakeholders")

    s3 >> Edge(color="#2E7D32", penwidth="3") >> athena
    athena >> Edge(color="#FF9900", penwidth="3", label="SPICE or\nDirect Query") >> qs
    qs >> Edge(color="#232F3E", penwidth="3", label="dashboards") >> user


# ============================================================
# 00: Full Pipeline (no clusters — linear flow)
# ============================================================
print("Generating: 00_full_pipeline...")
with Diagram("", filename="00_full_pipeline", show=False, direction="LR",
             graph_attr={**GRAPH_ATTR, "ranksep": "1.3", "nodesep": "0.6"},
             node_attr=NODE_ATTR, edge_attr=EDGE_ATTR, outformat="png"):

    cars = IotCar("IoT Devices")
    iot = IotCore("IoT Core")
    kinesis = KinesisDataStreams("Kinesis")
    firehose = KinesisDataFirehose("Firehose")
    s3_raw = S3("S3 Raw")
    crawler = Glue("Crawler")
    catalog = Glue("Catalog")
    sfn = StepFunctions("Step\nFunctions")
    glue = Glue("Glue ETL")
    s3_cur = S3("S3 Curated")
    athena = Athena("Athena")
    qs = Quicksight("QuickSight")

    cars >> Edge(color="#1B9E77", penwidth="2.5") >> iot
    iot >> Edge(color="#FF9900", penwidth="2.5") >> kinesis
    kinesis >> Edge(color="#FF9900", penwidth="2.5") >> firehose
    firehose >> Edge(color="#2E7D32", penwidth="2.5") >> s3_raw
    s3_raw >> Edge(color="#1565C0", penwidth="2") >> crawler
    crawler >> Edge(color="#1565C0", penwidth="2") >> catalog
    s3_raw >> Edge(color="#FF9900", penwidth="2.5") >> sfn
    sfn >> Edge(color="#FF9900", penwidth="2.5") >> glue
    glue >> Edge(color="#2E7D32", penwidth="2.5") >> s3_cur
    s3_cur >> Edge(color="#6A1B9A", penwidth="2.5") >> athena
    athena >> Edge(color="#FF9900", penwidth="2.5") >> qs


print("\n✅ All session 2 diagrams generated!")
for f in sorted(os.listdir(".")):
    if f.endswith(".png"):
        print(f"  • {f}")
