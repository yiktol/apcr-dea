"""
Q5: VPC Security for Data Pipeline
Generate enhanced visual diagrams for each option.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import EC2
from diagrams.aws.storage import S3
from diagrams.aws.network import VPC, NATGateway
from diagrams.aws.security import Shield
from diagrams.aws.general import Users
from diagrams.custom import Custom

ICONS = os.path.abspath("../../../aws-icons/Architecture-Service-Icons_04302026")
VPC_ICON = f"{ICONS}/Arch_Networking-Content-Delivery/48/Arch_Amazon-Virtual-Private-Cloud_48.png"
PRIVATELINK_ICON = f"{ICONS}/Arch_Networking-Content-Delivery/48/Arch_AWS-PrivateLink_48.png"

# Common styling
CORRECT_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
INCORRECT_CLUSTER = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
VPC_CLUSTER = {"bgcolor": "#e8eaf6", "style": "rounded", "pencolor": "#283593", "penwidth": "2"}
PUBLIC_SUBNET = {"bgcolor": "#fff9c4", "style": "rounded", "pencolor": "#f57f17", "penwidth": "1.5"}
PRIVATE_SUBNET = {"bgcolor": "#e0f2f1", "style": "rounded", "pencolor": "#00695c", "penwidth": "1.5"}
INTERNET_CLUSTER = {"bgcolor": "#ffebee", "style": "rounded", "pencolor": "#b71c1c", "penwidth": "1.5"}

base_graph_attr = {
    "fontsize": "14",
    "bgcolor": "#fafafa",
    "pad": "0.8",
    "splines": "spline",
    "nodesep": "1.0",
    "ranksep": "1.0",
    "fontname": "Helvetica",
}

# Option A: Public subnets + Security Groups only (INCORRECT)
with Diagram("",
             filename="option_a_public_sg",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Internet", graph_attr=INTERNET_CLUSTER):
        internet = Users("Attackers / Internet")
    with Cluster("VPC", graph_attr=VPC_CLUSTER):
        with Cluster("Public Subnet ⚠️", graph_attr=INCORRECT_CLUSTER):
            ec2_1 = EC2("EC2 (Public IP)")
            ec2_2 = EC2("EC2 (Public IP)")
            sg = Shield("Security Group\n(only defense)")
    s3 = S3("S3 Results")
    internet >> Edge(label="Direct access possible\n⚠️ Exposed to internet", color="#c62828", style="bold", penwidth="2.0") >> ec2_1
    ec2_1 >> Edge(color="#1565c0") >> sg
    ec2_2 >> Edge(color="#1565c0") >> s3

# Option B: Private subnets + NAT + SGs (CORRECT)
with Diagram("",
             filename="option_b_private_nat",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Internet", graph_attr=INTERNET_CLUSTER):
        internet = Users("Internet")
    with Cluster("VPC", graph_attr=VPC_CLUSTER):
        with Cluster("Public Subnet", graph_attr=PUBLIC_SUBNET):
            nat = NATGateway("NAT Gateway\n(Outbound only)")
        with Cluster("Private Subnet (No public IPs)", graph_attr=CORRECT_CLUSTER):
            ec2_1 = EC2("EC2 Instance 1")
            ec2_2 = EC2("EC2 Instance 2")
            sg = Shield("Security Groups\n(Instance-level)")
    s3 = S3("S3 Results")
    internet >> Edge(label="❌ No inbound access\nto private subnet", color="#c62828", style="dashed", penwidth="1.5") >> ec2_1
    ec2_1 >> Edge(label="Outbound via NAT", color="#2e7d32", style="bold") >> nat
    nat >> Edge(color="#2e7d32", style="bold") >> internet
    ec2_2 >> Edge(label="SG controlled", color="#2e7d32", style="bold") >> s3

# Option C: VPC Endpoints + Public Subnets + NACLs (INCORRECT)
with Diagram("",
             filename="option_c_vpc_endpoints_public",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Internet", graph_attr=INTERNET_CLUSTER):
        internet = Users("Internet")
    with Cluster("VPC", graph_attr=VPC_CLUSTER):
        with Cluster("Public Subnet ⚠️ (Still exposed)", graph_attr=INCORRECT_CLUSTER):
            ec2 = EC2("EC2 (Public IP)\n⚠️ Reachable")
        endpoint = Custom("VPC Endpoint\nfor S3", PRIVATELINK_ICON)
    s3 = S3("S3")
    internet >> Edge(label="⚠️ Still reachable\nvia public IP", color="#c62828", style="bold", penwidth="2.0") >> ec2
    ec2 >> Edge(label="Private path ✓\n(good practice)", color="#2e7d32", style="bold") >> endpoint
    endpoint >> Edge(color="#2e7d32", style="bold") >> s3

# Option D: VPC Peering (INCORRECT)
with Diagram("",
             filename="option_d_vpc_peering",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("VPC A - Ingestion", graph_attr=INCORRECT_CLUSTER):
        ec2_a = EC2("EC2\n(Ingest Data)")
    with Cluster("VPC B - Processing", graph_attr=INCORRECT_CLUSTER):
        ec2_b = EC2("EC2\n(Process Data)")
    with Cluster("VPC C - Storage", graph_attr=INCORRECT_CLUSTER):
        s3 = S3("S3 Results")
    ec2_a >> Edge(label="VPC Peering\n⚠️ Over-engineered", color="#e65100", style="bold", penwidth="2.0") >> ec2_b
    ec2_b >> Edge(label="VPC Peering\n⚠️ Complex routing", color="#e65100", style="bold", penwidth="2.0") >> s3

print("Q5 diagrams generated successfully.")
