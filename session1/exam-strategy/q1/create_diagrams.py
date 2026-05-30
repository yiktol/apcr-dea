"""
Q1: Kinesis ProvisionedThroughputExceededException
Generate enhanced visual diagrams for each option.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import KinesisDataStreams
from diagrams.aws.compute import Lambda
from diagrams.custom import Custom

ICONS = os.path.abspath("../../../aws-icons/Architecture-Service-Icons_04302026")
KINESIS_ICON = f"{ICONS}/Arch_Analytics/48/Arch_Amazon-Kinesis-Data-Streams_48.png"
CLOUDWATCH_ICON = f"{ICONS}/Arch_Management-Tools/48/Arch_Amazon-CloudWatch_48.png"

# Common styling
CORRECT_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
INCORRECT_CLUSTER = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
NEUTRAL_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5"}
STREAM_CLUSTER = {"bgcolor": "#fff3e0", "style": "rounded", "pencolor": "#e65100", "penwidth": "1.5"}

base_graph_attr = {
    "fontsize": "14",
    "bgcolor": "#fafafa",
    "pad": "0.8",
    "splines": "spline",
    "nodesep": "1.0",
    "ranksep": "1.0",
    "fontname": "Helvetica",
}

# Option A: Configure enhanced fan-out (INCORRECT)
with Diagram("",
             filename="option_a_enhanced_fanout",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Kinesis Data Stream", graph_attr=STREAM_CLUSTER):
        stream = KinesisDataStreams("Data Stream\n(Enhanced Fan-Out)")
    with Cluster("Consumers (Dedicated Throughput)", graph_attr=NEUTRAL_CLUSTER):
        consumer1 = Lambda("Consumer 1\n(2 MB/s dedicated)")
        consumer2 = Lambda("Consumer 2\n(2 MB/s dedicated)")
    stream >> Edge(label="HTTP/2 Push", color="#1565c0", style="bold") >> consumer1
    stream >> Edge(label="HTTP/2 Push", color="#1565c0", style="bold") >> consumer2

# Option B: Enable enhanced monitoring (INCORRECT)
with Diagram("",
             filename="option_b_enhanced_monitoring",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Kinesis Data Stream", graph_attr=STREAM_CLUSTER):
        stream = KinesisDataStreams("Data Stream")
    monitoring = Custom("CloudWatch\nEnhanced Metrics", CLOUDWATCH_ICON)
    with Cluster("Application", graph_attr=INCORRECT_CLUSTER):
        consumer = Lambda("Consumer App\n⚠️ Still Throttled")
    stream >> Edge(label="Shard-level metrics", color="#6a1b9a", style="dashed") >> monitoring
    stream >> Edge(label="Read REJECTED\nProvisionedThroughput\nExceededException", color="#c62828", style="bold") >> consumer

# Option C: Increase GetRecords size (INCORRECT)
with Diagram("",
             filename="option_c_larger_getrecords",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Kinesis Data Stream", graph_attr=STREAM_CLUSTER):
        stream = KinesisDataStreams("Single Shard\n⚡ 2 MB/s MAX")
    with Cluster("Application", graph_attr=INCORRECT_CLUSTER):
        consumer = Lambda("Consumer App")
    consumer >> Edge(
        label="GetRecords(Limit=10000)\n━━━━━━━━━━━━━━━━━━━━\nLarger batch ≠ more throughput\nStill capped at 2 MB/s per shard",
        color="#c62828", style="bold"
    ) >> stream

# Option D: Increase number of shards (CORRECT)
with Diagram("",
             filename="option_d_more_shards",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Kinesis Data Stream (Resharded)", graph_attr=CORRECT_CLUSTER):
        shard1 = KinesisDataStreams("Shard 1\n2 MB/s read")
        shard2 = KinesisDataStreams("Shard 2\n2 MB/s read")
        shard3 = KinesisDataStreams("Shard 3\n2 MB/s read")
        shard4 = KinesisDataStreams("Shard 4\n2 MB/s read")
    with Cluster("Application", graph_attr=NEUTRAL_CLUSTER):
        consumer = Lambda("Consumer App\n✅ 8 MB/s total")
    shard1 >> Edge(color="#2e7d32", style="bold") >> consumer
    shard2 >> Edge(color="#2e7d32", style="bold") >> consumer
    shard3 >> Edge(color="#2e7d32", style="bold") >> consumer
    shard4 >> Edge(color="#2e7d32", style="bold") >> consumer

# Option E: Retry reads (CORRECT)
with Diagram("",
             filename="option_e_retry",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Kinesis Data Stream", graph_attr=STREAM_CLUSTER):
        stream = KinesisDataStreams("Data Stream")
    with Cluster("Application (Retry Logic)", graph_attr=CORRECT_CLUSTER):
        consumer = Lambda("Consumer App\n🔄 Exponential Backoff")
    consumer >> Edge(label="① Read attempt → Throttled", color="#c62828", style="dashed") >> stream
    consumer >> Edge(label="② Wait 100ms → Retry", color="#f57c00", style="dashed") >> stream
    consumer >> Edge(label="③ Wait 200ms → Retry", color="#f9a825", style="dashed") >> stream
    consumer >> Edge(label="④ Success ✓", color="#2e7d32", style="bold") >> stream

print("Q1 diagrams generated successfully.")
