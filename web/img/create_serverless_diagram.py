"""
Generate architecture diagram for 06_serverless.yaml
Serverless Coffee Shop: API Gateway + Lambda + SQS + SNS + DynamoDB

Uses the `diagrams` library (mingrammer/diagrams) with Graphviz.
Horizontal (LR) layout optimized for 16:9 PPT slide.
"""
import os
os.environ["PATH"] += os.pathsep + "/opt/homebrew/bin"

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.database import DynamodbTable
from diagrams.aws.integration import SQS, SNS
from diagrams.aws.network import APIGateway, Route53
from diagrams.aws.security import SecretsManager
from diagrams.aws.management import Cloudwatch

OUTPUT_PATH = "/Users/erictole/demo/apcr-dea/web/img/06_serverless_architecture"

graph_attr = {
    "fontsize": "20",
    "fontname": "Helvetica Neue Bold,Helvetica,Arial,sans-serif",
    "bgcolor": "white",
    "pad": "0.5",
    "nodesep": "0.8",
    "ranksep": "1.8",
    "ratio": "0.5",
    "size": "13.33,7.5!",
    "dpi": "200",
    "label": "",
    "splines": "curved",
}

node_attr = {
    "fontsize": "15",
    "fontname": "Helvetica Neue Bold,Helvetica,Arial,sans-serif",
    "fontcolor": "#232F3E",
    "width": "1.8",
    "height": "1.8",
}

edge_attr = {
    "fontsize": "13",
    "fontname": "Helvetica Neue,Helvetica,Arial,sans-serif",
    "fontcolor": "#333333",
    "penwidth": "2.5",
    "arrowsize": "1.2",
    "color": "#555555",
    "decorate": "true",
    "labelfloat": "false",
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
    # --- DNS ---
    dns = Route53("Route 53\nCustom Domain")

    # --- API Layer ---
    with Cluster(
        "API Layer",
        graph_attr={
            "bgcolor": "#FFF8E1",
            "style": "rounded",
            "pencolor": "#FF9900",
            "penwidth": "3.5",
            "fontsize": "18",
            "fontname": "Helvetica Neue Bold",
            "fontcolor": "#E65100",
            "margin": "25",
        },
    ):
        apigw = APIGateway("API Gateway\nREST")
        authorizer = Lambda("Authorizer")
        secrets = SecretsManager("Secrets\nManager")

    # --- Compute ---
    with Cluster(
        "Compute (Lambda)",
        graph_attr={
            "bgcolor": "#E8F5E9",
            "style": "rounded",
            "pencolor": "#4CAF50",
            "penwidth": "3.5",
            "fontsize": "18",
            "fontname": "Helvetica Neue Bold",
            "fontcolor": "#2E7D32",
            "margin": "25",
        },
    ):
        cashier = Lambda("Cashier\nPOST /cashier")
        barista = Lambda("Barista\nQueue Consumer")
        ddb_writer = Lambda("DDB Writer\nSNS Subscriber")
        waiter = Lambda("Waiter\nDelivery")

    # --- Messaging ---
    with Cluster(
        "Messaging (SQS / SNS)",
        graph_attr={
            "bgcolor": "#E3F2FD",
            "style": "rounded",
            "pencolor": "#1976D2",
            "penwidth": "3.5",
            "fontsize": "18",
            "fontname": "Helvetica Neue Bold",
            "fontcolor": "#0D47A1",
            "margin": "25",
        },
    ):
        order_queue = SQS("Order\nProcessing")
        dlq = SQS("Dead Letter\nQueue")
        sns_topic = SNS("SNS Topic\nFan-Out")
        delivery_queue = SQS("Order\nDelivery")

    # --- Storage ---
    with Cluster(
        "Storage (DynamoDB)",
        graph_attr={
            "bgcolor": "#FCE4EC",
            "style": "rounded",
            "pencolor": "#C62828",
            "penwidth": "3.5",
            "fontsize": "18",
            "fontname": "Helvetica Neue Bold",
            "fontcolor": "#B71C1C",
            "margin": "25",
        },
    ):
        dynamodb = DynamodbTable("Global Table\n3 Regions")

    # Monitoring
    cw = Cloudwatch("CloudWatch\nDashboard")

    # === Main Flow (thick, curved, labeled) ===

    dns >> Edge(color="#232F3E", penwidth="3") >> apigw

    apigw - Edge(color="#FF9900", style="dashed", penwidth="2", label="auth check") - authorizer
    secrets - Edge(color="#888888", style="dotted", penwidth="1.5") - authorizer

    apigw >> Edge(color="#FF9900", penwidth="3.5", label="order request") >> cashier

    cashier >> Edge(color="#2E7D32", penwidth="3.5", label="enqueue order") >> order_queue

    order_queue >> Edge(color="#1565C0", penwidth="3.5", label="trigger") >> barista

    order_queue >> Edge(color="#B71C1C", style="dashed", penwidth="2", label="max retries") >> dlq

    barista >> Edge(color="#2E7D32", penwidth="3.5", label="publish event") >> sns_topic

    sns_topic >> Edge(color="#1565C0", penwidth="3", label="fan-out") >> ddb_writer
    sns_topic >> Edge(color="#1565C0", penwidth="3", label="fan-out") >> delivery_queue

    delivery_queue >> Edge(color="#1565C0", penwidth="2.5", label="trigger") >> waiter

    ddb_writer >> Edge(color="#C62828", penwidth="3.5", label="persist") >> dynamodb

    apigw >> Edge(color="#C62828", style="dashed", penwidth="2", label="GET orders/sales") >> dynamodb

    cashier >> Edge(color="#BBBBBB", style="dotted", penwidth="1.2") >> cw
    barista >> Edge(color="#BBBBBB", style="dotted", penwidth="1.2") >> cw
