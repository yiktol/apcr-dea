"""Auto-generated diagram script."""
import os, sys
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import KinesisDataStreams, Redshift, Glue, Athena
from diagrams.aws.compute import Lambda, EC2
from diagrams.aws.storage import S3, ElasticFileSystemEFS, ElasticBlockStoreEBS
from diagrams.aws.database import Dynamodb, RDS
from diagrams.aws.integration import Eventbridge
from diagrams.aws.network import VPC, NATGateway
from diagrams.aws.security import Shield
from diagrams.aws.general import Users

CORRECT_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
INCORRECT_CLUSTER = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
NEUTRAL_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5"}

base_graph_attr = {"fontsize": "14", "bgcolor": "#fafafa", "pad": "0.8", "splines": "spline", "nodesep": "1.0", "ranksep": "1.0", "fontname": "Helvetica"}

with Diagram("", filename="option_a", show=False, direction="LR", graph_attr=base_graph_attr):
    with Cluster("A: Incorrect", graph_attr=INCORRECT_CLUSTER):
        svc = S3("A")

with Diagram("", filename="option_b", show=False, direction="LR", graph_attr=base_graph_attr):
    with Cluster("B: Incorrect", graph_attr=INCORRECT_CLUSTER):
        svc = S3("B")

with Diagram("", filename="option_c", show=False, direction="LR", graph_attr=base_graph_attr):
    with Cluster("C: Incorrect", graph_attr=INCORRECT_CLUSTER):
        svc = S3("C")

with Diagram("", filename="option_d", show=False, direction="LR", graph_attr=base_graph_attr):
    with Cluster("D: Correct", graph_attr=CORRECT_CLUSTER):
        svc = S3("D")

with Diagram("", filename="option_e", show=False, direction="LR", graph_attr=base_graph_attr):
    with Cluster("E: Correct", graph_attr=CORRECT_CLUSTER):
        svc = S3("E")

print("q2 diagrams generated.")
