"""
Q4: NFS Migration for Lambda
Generate enhanced visual diagrams for each option.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3, ElasticFileSystemEFS, ElasticBlockStoreEBS
from diagrams.aws.database import Dynamodb as DynamoDB
from diagrams.custom import Custom

ICONS = os.path.abspath("../../../aws-icons/Architecture-Service-Icons_04302026")
EFS_ICON = f"{ICONS}/Arch_Storage/48/Arch_Amazon-EFS_48.png"
EBS_ICON = f"{ICONS}/Arch_Storage/48/Arch_Amazon-Elastic-Block-Store_48.png"
DYNAMODB_ICON = f"{ICONS}/Arch_Databases/48/Arch_Amazon-DynamoDB_48.png"

# Common styling
CORRECT_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
INCORRECT_CLUSTER = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
LAMBDA_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5"}
STORAGE_CLUSTER = {"bgcolor": "#fff3e0", "style": "rounded", "pencolor": "#e65100", "penwidth": "1.5"}

base_graph_attr = {
    "fontsize": "14",
    "bgcolor": "#fafafa",
    "pad": "0.8",
    "splines": "spline",
    "nodesep": "1.0",
    "ranksep": "1.0",
    "fontname": "Helvetica",
}

# Option A: Lambda local storage (INCORRECT)
with Diagram("",
             filename="option_a_local_storage",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Concurrent Lambda Invocations", graph_attr=INCORRECT_CLUSTER):
        with Cluster("Invocation 1"):
            fn1 = Lambda("Lambda 1\n/tmp (10GB max)")
        with Cluster("Invocation 2"):
            fn2 = Lambda("Lambda 2\n/tmp (10GB max)")
        with Cluster("Invocation 3"):
            fn3 = Lambda("Lambda 3\n/tmp (10GB max)")
    fn1 >> Edge(label="❌ No sharing\nbetween invocations", color="#c62828", style="dashed") >> fn2
    fn2 >> Edge(label="❌ Isolated storage", color="#c62828", style="dashed") >> fn3

# Option B: EBS volumes (INCORRECT)
with Diagram("",
             filename="option_b_ebs",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Storage", graph_attr=INCORRECT_CLUSTER):
        ebs = Custom("Amazon EBS\n(EC2 only)", EBS_ICON)
    with Cluster("Lambda Functions", graph_attr=LAMBDA_CLUSTER):
        fn1 = Lambda("Lambda 1")
        fn2 = Lambda("Lambda 2")
    fn1 >> Edge(label="❌ CANNOT attach\nEBS to Lambda", color="#c62828", style="dashed", penwidth="2.0") >> ebs
    fn2 >> Edge(label="❌ Not supported", color="#c62828", style="dashed", penwidth="2.0") >> ebs

# Option C: DynamoDB (INCORRECT)
with Diagram("",
             filename="option_c_dynamodb",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Database (Not a File System)", graph_attr=INCORRECT_CLUSTER):
        ddb = Custom("DynamoDB\n📋 Key-Value Store\n(Not NFS)", DYNAMODB_ICON)
    with Cluster("Lambda Functions", graph_attr=LAMBDA_CLUSTER):
        fn1 = Lambda("Lambda 1")
        fn2 = Lambda("Lambda 2")
    fn1 >> Edge(label="API calls only\n⚠️ Requires app rewrite", color="#c62828", style="bold") >> ddb
    fn2 >> Edge(label="Not file-based access", color="#c62828", style="dashed") >> ddb

# Option D: EFS (CORRECT)
with Diagram("",
             filename="option_d_efs",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Shared File System (NFS v4)", graph_attr=CORRECT_CLUSTER):
        efs = Custom("Amazon EFS\n📁 NFS Compatible\n(Shared Access)", EFS_ICON)
    with Cluster("Concurrent Lambda Functions", graph_attr=LAMBDA_CLUSTER):
        fn1 = Lambda("Lambda 1")
        fn2 = Lambda("Lambda 2")
        fn3 = Lambda("Lambda 3")
    fn1 >> Edge(label="Mount /mnt/efs", color="#2e7d32", style="bold", penwidth="2.0") >> efs
    fn2 >> Edge(label="Mount /mnt/efs", color="#2e7d32", style="bold", penwidth="2.0") >> efs
    fn3 >> Edge(label="Mount /mnt/efs", color="#2e7d32", style="bold", penwidth="2.0") >> efs

print("Q4 diagrams generated successfully.")
