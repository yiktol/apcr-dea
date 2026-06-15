"""
Architecture diagram: Amazon ECS with Fargate
User → ALB → ECS Service (3 containers on Fargate) ← ECR
Includes Region, VPC, and Availability Zones.
"""
import os
os.environ["PATH"] += os.pathsep + "/opt/homebrew/bin"

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import ECS, Fargate, ElasticContainerServiceContainer, ElasticContainerServiceService, ECR
from diagrams.aws.network import ElbApplicationLoadBalancer
from diagrams.onprem.client import User

OUTPUT_PATH = "/Users/erictole/demo/apcr-dea/web/img/ecs_fargate_architecture"

graph_attr = {
    "fontsize": "20",
    "fontname": "Helvetica Neue Bold,Helvetica,Arial,sans-serif",
    "bgcolor": "white",
    "pad": "0.6",
    "nodesep": "1.0",
    "ranksep": "2.0",
    "ratio": "0.5",
    "size": "13.33,7.5!",
    "dpi": "200",
    "label": "",
    "splines": "curved",
}

node_attr = {
    "fontsize": "14",
    "fontname": "Helvetica Neue Bold,Helvetica,Arial,sans-serif",
    "fontcolor": "#232F3E",
}

edge_attr = {
    "fontsize": "13",
    "fontname": "Helvetica Neue,Helvetica,Arial,sans-serif",
    "fontcolor": "#333333",
    "penwidth": "2.5",
    "arrowsize": "1.2",
    "color": "#555555",
    "decorate": "true",
    "splines": "curved",
}

with Diagram(
    "",
    filename=OUTPUT_PATH,
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
    outformat="png",
):
    user = User("User")

    ecr = ECR("Amazon ECR")

    with Cluster(
        "AWS Region (ap-southeast-1)",
        graph_attr={
            "bgcolor": "#F5F5F5",
            "style": "rounded",
            "pencolor": "#232F3E",
            "penwidth": "3",
            "fontsize": "17",
            "fontname": "Helvetica Neue Bold",
            "fontcolor": "#232F3E",
            "margin": "30",
        },
    ):
        with Cluster(
            "VPC",
            graph_attr={
                "bgcolor": "#FAFAFA",
                "style": "rounded",
                "pencolor": "#4CAF50",
                "penwidth": "3",
                "fontsize": "16",
                "fontname": "Helvetica Neue Bold",
                "fontcolor": "#2E7D32",
                "margin": "25",
            },
        ):
            alb = ElbApplicationLoadBalancer("ALB")

            with Cluster(
                "ECS Cluster",
                graph_attr={
                    "bgcolor": "#FFF8E1",
                    "style": "rounded",
                    "pencolor": "#FF9900",
                    "penwidth": "3",
                    "fontsize": "15",
                    "fontname": "Helvetica Neue Bold",
                    "fontcolor": "#E65100",
                    "margin": "20",
                },
            ):
                service = ElasticContainerServiceService("ECS Service")

                with Cluster(
                    "AZ-1 (Fargate)",
                    graph_attr={
                        "bgcolor": "#E3F2FD",
                        "style": "rounded",
                        "pencolor": "#1976D2",
                        "penwidth": "2.5",
                        "fontsize": "14",
                        "fontname": "Helvetica Neue Bold",
                        "fontcolor": "#0D47A1",
                        "margin": "15",
                    },
                ):
                    task1 = ElasticContainerServiceContainer("Task 1")
                    task2 = ElasticContainerServiceContainer("Task 2")

                with Cluster(
                    "AZ-2 (Fargate)",
                    graph_attr={
                        "bgcolor": "#E8F5E9",
                        "style": "rounded",
                        "pencolor": "#388E3C",
                        "penwidth": "2.5",
                        "fontsize": "14",
                        "fontname": "Helvetica Neue Bold",
                        "fontcolor": "#1B5E20",
                        "margin": "15",
                    },
                ):
                    task3 = ElasticContainerServiceContainer("Task 3")

    # === Flow ===

    user >> Edge(color="#232F3E", penwidth="3", label="HTTPS") >> alb

    alb >> Edge(color="#FF9900", penwidth="3.5", label="route") >> service

    service >> Edge(color="#2E7D32", penwidth="3") >> task1
    service >> Edge(color="#2E7D32", penwidth="3") >> task2
    service >> Edge(color="#2E7D32", penwidth="3") >> task3

    ecr >> Edge(color="#1565C0", penwidth="2.5", style="dashed", label="pull image") >> task1
    ecr >> Edge(color="#1565C0", penwidth="2.5", style="dashed") >> task2
    ecr >> Edge(color="#1565C0", penwidth="2.5", style="dashed") >> task3
