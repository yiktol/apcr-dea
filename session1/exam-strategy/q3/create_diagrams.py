"""
Q3: PII Detection and Masking
Generate enhanced visual diagrams for each option.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import Eventbridge
from diagrams.custom import Custom

ICONS = os.path.abspath("../../../aws-icons/Architecture-Service-Icons_04302026")
MACIE_ICON = f"{ICONS}/Arch_Security-Identity/48/Arch_Amazon-Macie_48.png"

# Common styling
CORRECT_CLUSTER = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
INCORRECT_CLUSTER = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
DATA_CLUSTER = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5"}
DETECTION_CLUSTER = {"bgcolor": "#fff3e0", "style": "rounded", "pencolor": "#e65100", "penwidth": "1.5"}
ACTION_CLUSTER = {"bgcolor": "#f3e5f5", "style": "rounded", "pencolor": "#6a1b9a", "penwidth": "1.5"}

base_graph_attr = {
    "fontsize": "14",
    "bgcolor": "#fafafa",
    "pad": "0.8",
    "splines": "spline",
    "nodesep": "1.0",
    "ranksep": "1.0",
    "fontname": "Helvetica",
}

# Option A: Custom Lambda for PII detection (INCORRECT)
with Diagram("",
             filename="option_a_custom_lambda",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Lake", graph_attr=DATA_CLUSTER):
        s3 = S3("S3 Bucket")
    with Cluster("Custom Detection (High Effort)", graph_attr=INCORRECT_CLUSTER):
        detector = Lambda("Lambda\n🔧 Custom PII Logic\n(build & maintain)")
    with Cluster("Masking", graph_attr=ACTION_CLUSTER):
        masker = Lambda("Masking App")
    s3 >> Edge(label="S3 Event Notification\n(new objects only)", color="#e65100", style="bold") >> detector
    detector >> Edge(label="PII found → invoke", color="#6a1b9a", style="bold") >> masker

# Option B: S3 Event + EventBridge (no PII detection) (INCORRECT)
with Diagram("",
             filename="option_b_eventbridge_upload",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Lake", graph_attr=DATA_CLUSTER):
        s3 = S3("S3 Bucket")
    with Cluster("Event Routing", graph_attr=INCORRECT_CLUSTER):
        eb = Eventbridge("EventBridge\n(All uploads)")
    with Cluster("Masking", graph_attr=ACTION_CLUSTER):
        masker = Lambda("Masking App\n⚠️ Triggered on ALL files")
    s3 >> Edge(label="Every object upload\n(no PII filtering)", color="#c62828", style="bold", penwidth="2.0") >> eb
    eb >> Edge(label="Blind trigger\n(wasteful)", color="#c62828", style="bold") >> masker

# Option C: Macie + EventBridge (CORRECT)
with Diagram("",
             filename="option_c_macie_eventbridge",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Lake", graph_attr=DATA_CLUSTER):
        s3 = S3("S3 Data Lake\n(New & Existing)")
    with Cluster("PII Discovery (Managed)", graph_attr=CORRECT_CLUSTER):
        macie = Custom("Amazon Macie\n🔍 Auto PII Detection", MACIE_ICON)
    with Cluster("Event-Driven Response", graph_attr=CORRECT_CLUSTER):
        eb = Eventbridge("EventBridge\n(Macie Findings)")
    with Cluster("Remediation", graph_attr=ACTION_CLUSTER):
        masker = Lambda("Masking App")
    s3 >> Edge(label="Scans all data", color="#1565c0", style="bold") >> macie
    macie >> Edge(label="PII Finding event", color="#2e7d32", style="bold", penwidth="2.0") >> eb
    eb >> Edge(label="Real-time trigger", color="#2e7d32", style="bold", penwidth="2.0") >> masker

# Option D: Macie + Polling Lambda (INCORRECT)
with Diagram("",
             filename="option_d_macie_polling",
             show=False, direction="LR",
             graph_attr=base_graph_attr):
    with Cluster("Data Lake", graph_attr=DATA_CLUSTER):
        s3 = S3("S3 Data Lake")
    with Cluster("PII Discovery", graph_attr=DETECTION_CLUSTER):
        macie = Custom("Amazon Macie\n🔍 PII Detection", MACIE_ICON)
    with Cluster("Polling (Delayed)", graph_attr=INCORRECT_CLUSTER):
        poller = Lambda("Lambda\n⏰ Scheduled\n(every 5 min?)")
    with Cluster("Remediation", graph_attr=ACTION_CLUSTER):
        masker = Lambda("Masking App")
    s3 >> Edge(label="Scans", color="#1565c0", style="bold") >> macie
    poller >> Edge(label="Poll findings\n⚠️ Delayed response", color="#c62828", style="dashed", penwidth="1.5") >> macie
    poller >> Edge(label="Invoke masker", color="#6a1b9a", style="bold") >> masker

print("Q3 diagrams generated successfully.")
