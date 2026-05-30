"""
Q2: Data Pipeline Architecture
Generate enhanced visual diagrams for each option.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import Redshift, Glue, Athena
from diagrams.aws.database import RDS
from diagrams.aws.storage import S3
from diagrams.custom import Custom

ICONS = os.path.abspath("../../../aws-icons/Architecture-Service-Icons_04302026")
LAKE_ICON = f"{ICONS}/Arch_Analytics/48/Arch_AWS-Lake-Formation_48.png"

# Common styling
CORRECT_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
INCORRECT_CLUSTER = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
SOURCE_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5"}
BI_CLUSTER = {"bgcolor": "#f3e5f5", "style": "rounded", "pencolor": "#6a1b9a", "penwidth": "1.5"}

base_graph_attr = {
    "fontsize": "14",
    "bgcolor": "#fafafa",
    "pad": "0.8",
    "splines": "spline",
    "nodesep": "1.0",
    "ranksep": "1.0",
    "fontname": "Helvetica",
}

# Option A: Direct analytics on transactional DBs (INCORRECT)
with Diagram("",
             filename="option_a_direct_queries",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Production Databases", graph_attr=INCORRECT_CLUSTER):
        db1 = RDS("Transactional DB 1\n⚠️ Under Load")
        db2 = RDS("Transactional DB 2\n⚠️ Under Load")
    with Cluster("BI Team", graph_attr=BI_CLUSTER):
        bi = Athena("Analytics Queries")
    bi >> Edge(label="Complex JOINs\n& Aggregations", color="#c62828", style="bold", penwidth="2.0") >> db1
    bi >> Edge(label="Full table scans", color="#c62828", style="bold", penwidth="2.0") >> db2

# Option B: Lake to Warehouse (CORRECT)
with Diagram("",
             filename="option_b_lake_warehouse",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Sources", graph_attr=SOURCE_CLUSTER):
        db1 = RDS("DB 1")
        db2 = RDS("DB 2")
    with Cluster("Data Lake (Landing Zone)", graph_attr=CORRECT_CLUSTER):
        lake = S3("S3 Raw Data")
        etl = Glue("AWS Glue\n(Clean & Transform)")
    warehouse = Redshift("Amazon Redshift\n(Curated Data)")
    with Cluster("BI & Reporting", graph_attr=BI_CLUSTER):
        bi = Athena("QuickSight / Athena")
    db1 >> Edge(label="Ingest", color="#1565c0", style="bold") >> lake
    db2 >> Edge(label="Ingest", color="#1565c0", style="bold") >> lake
    lake >> Edge(color="#2e7d32", style="bold") >> etl
    etl >> Edge(label="Prepared data", color="#2e7d32", style="bold") >> warehouse
    warehouse >> Edge(label="Fast queries", color="#6a1b9a", style="bold") >> bi

# Option C: Direct to warehouse, transform at query time (INCORRECT)
with Diagram("",
             filename="option_c_direct_warehouse",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Sources", graph_attr=SOURCE_CLUSTER):
        db1 = RDS("DB 1")
        db2 = RDS("DB 2")
    with Cluster("Data Warehouse", graph_attr=INCORRECT_CLUSTER):
        warehouse = Redshift("Redshift\n(Raw Data + ELT)")
    with Cluster("BI Team", graph_attr=BI_CLUSTER):
        bi = Athena("Analytics")
    db1 >> Edge(label="Raw dump", color="#e65100", style="bold") >> warehouse
    db2 >> Edge(label="Raw dump", color="#e65100", style="bold") >> warehouse
    warehouse >> Edge(label="Transform at query time\n⚠️ Slow & expensive", color="#c62828", style="dashed") >> bi

# Option D: Separate data marts (INCORRECT)
with Diagram("",
             filename="option_d_data_marts",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Sources", graph_attr=SOURCE_CLUSTER):
        db1 = RDS("Production DB")
    with Cluster("Business Unit A", graph_attr=INCORRECT_CLUSTER):
        mart_a = Redshift("Data Mart A")
    with Cluster("Business Unit B", graph_attr=INCORRECT_CLUSTER):
        mart_b = Redshift("Data Mart B")
    with Cluster("Business Unit C", graph_attr=INCORRECT_CLUSTER):
        mart_c = Redshift("Data Mart C")
    db1 >> Edge(label="Pipeline A\n(duplicated)", color="#c62828", style="bold") >> mart_a
    db1 >> Edge(label="Pipeline B\n(duplicated)", color="#c62828", style="bold") >> mart_b
    db1 >> Edge(label="Pipeline C\n(duplicated)", color="#c62828", style="bold") >> mart_c

print("Q2 diagrams generated successfully.")
