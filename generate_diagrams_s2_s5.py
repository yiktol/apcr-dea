"""
Generate meaningful architecture diagrams for sessions 2-5.
Each option gets a unique diagram showing the proposed architecture.
"""
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import KinesisDataStreams, Redshift, Glue, Athena
from diagrams.aws.analytics import ManagedStreamingForKafka as MSK
from diagrams.aws.compute import Lambda, EC2
from diagrams.aws.storage import S3, ElasticFileSystemEFS, ElasticBlockStoreEBS
from diagrams.aws.database import Dynamodb, RDS
from diagrams.aws.integration import Eventbridge
from diagrams.aws.network import VPC, NATGateway, CloudFront
from diagrams.aws.security import Shield, Macie
from diagrams.aws.migration import DatabaseMigrationService, Snowball
from diagrams.aws.general import Users
from diagrams.custom import Custom

ICONS = os.path.abspath("aws-icons/Architecture-Service-Icons_04302026")

C_OK = {"bgcolor": "#e8f5e9", "style": "rounded", "pencolor": "#2e7d32", "penwidth": "2"}
C_NO = {"bgcolor": "#fbe9e7", "style": "rounded", "pencolor": "#c62828", "penwidth": "2"}
C_N = {"bgcolor": "#e3f2fd", "style": "rounded", "pencolor": "#1565c0", "penwidth": "1.5"}
C_SRC = {"bgcolor": "#fff3e0", "style": "rounded", "pencolor": "#e65100", "penwidth": "1.5"}

G = {"fontsize": "14", "bgcolor": "#fafafa", "pad": "0.8", "splines": "spline",
     "nodesep": "1.0", "ranksep": "1.0", "fontname": "Helvetica"}


def d(folder, filename, direction="LR"):
    """Helper to create Diagram context."""
    os.makedirs(folder, exist_ok=True)
    return Diagram("", filename=os.path.join(folder, filename),
                   show=False, direction=direction, graph_attr=G)


# ============================================================
# SESSION 2, Q1: Data Transfer to S3
# ============================================================
F = "session2/exam-strategy/q1"

with d(F, "option_a"):
    with Cluster("On-Premises (5 PB)", graph_attr=C_SRC):
        src = EC2("Data Center")
    dc = Custom("Direct Connect\n1-400 Gbps", f"{ICONS}/Arch_Networking-Content-Delivery/48/Arch_AWS-Direct-Connect_48.png")
    s3 = S3("Amazon S3")
    src >> Edge(label="5 PB + 3 TB/mo\nSlow for bulk", color="#c62828", style="bold") >> dc >> s3

with d(F, "option_b"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = EC2("NFS/SMB Source")
    ds = Custom("AWS DataSync\n10 Gbps/task", f"{ICONS}/Arch_Migration-Modernization/48/Arch_AWS-DataSync_48.png") if os.path.exists(f"{ICONS}/Arch_Migration-Modernization/48/Arch_AWS-DataSync_48.png") else Lambda("DataSync")
    s3 = S3("Amazon S3")
    src >> Edge(label="3 TB/month\nScheduled sync", color="#2e7d32", style="bold") >> ds >> Edge(label="< 2 days ✓", color="#2e7d32") >> s3

with d(F, "option_c"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = EC2("Data Center")
    cf = CloudFront("Transfer\nAcceleration")
    s3 = S3("Amazon S3")
    src >> Edge(label="3 TB/mo\nNo scheduling", color="#c62828", style="bold") >> cf >> s3

with d(F, "option_d"):
    with Cluster("On-Premises (5 PB)", graph_attr=C_SRC):
        src = EC2("Data Center")
    snow = Snowball("AWS Snowball\n~210 TB each")
    s3 = S3("Amazon S3")
    src >> Edge(label="Physical ship\n5 PB one-time", color="#2e7d32", style="bold") >> snow >> Edge(label="Import to S3", color="#2e7d32") >> s3

with d(F, "option_e"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = EC2("Data Center")
    s3 = S3("Amazon S3")
    src >> Edge(label="3 TB via Internet\n~2.8 days at 100Mbps\nExceeds SLA", color="#c62828", style="bold") >> s3

print("Session 2 Q1 done")

# ============================================================
# SESSION 2, Q2: Redshift Data Offloading
# ============================================================
F = "session2/exam-strategy/q2"

with d(F, "option_a"):
    rs = Redshift("Redshift")
    s3 = S3("S3\n(JSON files)")
    rs >> Edge(label="UNLOAD\nas JSON (row-based)", color="#c62828", style="bold") >> s3

with d(F, "option_b"):
    rs = Redshift("Redshift")
    with Cluster("Federated Query", graph_attr=C_NO):
        rds = RDS("RDS/Aurora\n(Not S3)")
    rs >> Edge(label="Federated Query\nWrong target", color="#c62828", style="bold") >> rds

with d(F, "option_c"):
    s3 = S3("S3 (Source)")
    rs = Redshift("Redshift")
    s3 >> Edge(label="COPY = S3→Redshift\nWrong direction!", color="#c62828", style="bold") >> rs

with d(F, "option_d"):
    rs = Redshift("Redshift\n(Local Tables)")
    with Cluster("Redshift Spectrum", graph_attr=C_OK):
        ext = S3("S3 External\nTables")
    athena = Athena("SQL JOINs")
    rs >> Edge(color="#2e7d32") >> athena
    ext >> Edge(label="Query in-place", color="#2e7d32", style="bold") >> athena

with d(F, "option_e"):
    rs = Redshift("Redshift")
    s3 = S3("S3\n(Parquet - columnar)")
    rs >> Edge(label="UNLOAD\nas Parquet ✓", color="#2e7d32", style="bold") >> s3

print("Session 2 Q2 done")

# ============================================================
# SESSION 2, Q3: Data Lake Integration
# ============================================================
F = "session2/exam-strategy/q3"

with d(F, "option_a"):
    s3 = S3("S3 Data Lake")
    with Cluster("Shared Catalog", graph_attr=C_OK):
        glue = Glue("AWS Glue\nData Catalog")
    with Cluster("Analysis Tools", graph_attr=C_N):
        a1 = Athena("Athena")
        a2 = Redshift("Spectrum")
    s3 >> Edge(label="Crawl", color="#2e7d32") >> glue
    glue >> Edge(color="#2e7d32", style="bold") >> a1
    glue >> Edge(color="#2e7d32", style="bold") >> a2

with d(F, "option_b"):
    s3 = S3("S3 Data Lake")
    with Cluster("Each Tool Configured Separately", graph_attr=C_NO):
        a1 = Athena("Athena\n(own schema)")
        a2 = Redshift("Redshift\n(own schema)")
        a3 = Lambda("Hive\n(own schema)")
    s3 >> Edge(label="Manual config", color="#c62828") >> a1
    s3 >> Edge(label="Manual config", color="#c62828") >> a2
    s3 >> Edge(label="Manual config", color="#c62828") >> a3

with d(F, "option_c"):
    s3 = S3("S3 Data Lake")
    ec2 = EC2("EC2 Server\n(Self-managed)")
    s3 >> Edge(label="Copy data", color="#c62828", style="bold") >> ec2

with d(F, "option_d"):
    s3 = S3("S3 Data Lake")
    with Cluster("4x Data Copies", graph_attr=C_NO):
        c1 = Athena("Copy 1")
        c2 = Redshift("Copy 2")
        c3 = Lambda("Copy 3")
        c4 = EC2("Copy 4")
    s3 >> Edge(label="Duplicate", color="#c62828") >> c1
    s3 >> Edge(label="Duplicate", color="#c62828") >> c2
    s3 >> Edge(label="Duplicate", color="#c62828") >> c3
    s3 >> Edge(label="Duplicate", color="#c62828") >> c4

print("Session 2 Q3 done")

# ============================================================
# SESSION 2, Q4: Schema Conversion & Migration
# ============================================================
F = "session2/exam-strategy/q4"

with d(F, "option_a"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = RDS("Source DB")
    dms = DatabaseMigrationService("AWS DMS\n(Data only)")
    tgt = RDS("Amazon RDS")
    src >> Edge(label="No schema conversion!", color="#c62828", style="bold") >> dms >> tgt

with d(F, "option_b"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = RDS("Source DB")
    manual = EC2("Manual SQL\nScripts")
    dms = DatabaseMigrationService("AWS DMS")
    tgt = RDS("Amazon RDS")
    src >> Edge(label="Error-prone", color="#c62828") >> manual >> Edge(label="Schema") >> tgt
    src >> Edge(label="Data") >> dms >> tgt

with d(F, "option_c"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = RDS("Source DB")
    sct = Glue("AWS SCT\n(Schema + SQL)")
    dms = DatabaseMigrationService("AWS DMS\n(Data + CDC)")
    tgt = RDS("Amazon RDS")
    src >> Edge(label="Convert schema", color="#2e7d32", style="bold") >> sct >> Edge(color="#2e7d32") >> tgt
    src >> Edge(label="Migrate data", color="#2e7d32", style="bold") >> dms >> Edge(color="#2e7d32") >> tgt

with d(F, "option_d"):
    with Cluster("On-Premises", graph_attr=C_SRC):
        src = RDS("Source DB")
    sct = Glue("AWS SCT")
    tf = Lambda("Transfer Family\n(SFTP only!)")
    tgt = RDS("Amazon RDS")
    src >> Edge(label="Schema ✓", color="#2e7d32") >> sct >> tgt
    src >> Edge(label="Can't migrate DB data!", color="#c62828", style="bold") >> tf

print("Session 2 Q4 done")

# ============================================================
# SESSION 2, Q5: S3 Storage Classes
# ============================================================
F = "session2/exam-strategy/q5"

with d(F, "option_a"):
    with Cluster("Frequently Accessed", graph_attr=C_OK):
        hot = S3("S3 Standard\n(No retrieval fee)")
    with Cluster("Quarterly Access", graph_attr=C_OK):
        cold = S3("Glacier Instant\nRetrieval\n(68% cheaper)")
    hot >> Edge(label="Lifecycle rule\n> 90 days", color="#2e7d32", style="bold") >> cold

with d(F, "option_b"):
    with Cluster("Frequently Accessed", graph_attr=C_N):
        hot = S3("S3 Standard ✓")
    with Cluster("Quarterly Access", graph_attr=C_NO):
        cold = S3("S3 Standard-IA\n(68% more expensive\nthan Glacier IR)")
    hot >> Edge(label="Works but\nnot cheapest", color="#e65100", style="dashed") >> cold

with d(F, "option_c"):
    with Cluster("Frequently Accessed", graph_attr=C_NO):
        hot = S3("Glacier IR\n(High retrieval cost!)")
    with Cluster("Quarterly Access", graph_attr=C_NO):
        cold = S3("Deep Archive\n(12-48hr retrieval!)")
    hot >> Edge(label="Both wrong tier", color="#c62828", style="bold") >> cold

with d(F, "option_d"):
    with Cluster("Frequently Accessed", graph_attr=C_NO):
        hot = S3("Standard-IA\n($0.01/GB retrieval!)")
    with Cluster("Quarterly Access", graph_attr=C_N):
        cold = S3("Glacier IR ✓")
    hot >> Edge(label="Hot tier wrong", color="#c62828", style="bold") >> cold

print("Session 2 Q5 done")

# ============================================================
# SESSION 3, Q1: Athena Query Optimization
# ============================================================
F = "session3/exam-strategy/q1"

with d(F, "option_a"):
    s3 = S3("S3\n(Raw logs, no partition)")
    athena = Athena("Athena\nFull table scan!")
    s3 >> Edge(label="Scan everything\n$$$ per query", color="#c62828", style="bold") >> athena

with d(F, "option_b"):
    with Cluster("S3 (Year/Month Partition)", graph_attr=C_NO):
        s3 = S3("s3://logs/2024/03/\n(gzip, row-based)")
    athena = Athena("Athena\nScans full month")
    s3 >> Edge(label="Too coarse for\ndaily queries", color="#e65100", style="bold") >> athena

with d(F, "option_c"):
    s3 = S3("S3\n(Single huge file)")
    athena = Athena("Athena\nFull scan always!")
    s3 >> Edge(label="No partition pruning\nMax cost", color="#c62828", style="bold") >> athena

with d(F, "option_d"):
    with Cluster("S3 (Day Partition + Parquet)", graph_attr=C_OK):
        s3 = S3("s3://logs/day=2024-03-15/\n(Parquet, columnar)")
    athena = Athena("Athena\nScan 1 day only")
    s3 >> Edge(label="Partition pruning\n+ column pruning\nMin cost ✓", color="#2e7d32", style="bold") >> athena

print("Session 3 Q1 done")

# ============================================================
# SESSION 3, Q2: EMR Cost Optimization
# ============================================================
F = "session3/exam-strategy/q2"

with d(F, "option_a"):
    with Cluster("EMR Cluster (Instance Fleets)", graph_attr=C_OK):
        master = EC2("Master\n(On-Demand)")
        core = EC2("Core\n(On-Demand)")
        task = EC2("Task\n(Mixed types)")
    master >> Edge(label="Provisioning\ntimeout set", color="#2e7d32") >> core

with d(F, "option_b"):
    with Cluster("EMR (Uniform Groups)", graph_attr=C_NO):
        master = EC2("Master")
        core = EC2("Core\n(Single type)")
    master >> Edge(label="Less flexible\nNo timeout", color="#c62828") >> core

with d(F, "option_c"):
    with Cluster("EMR Cluster", graph_attr=C_NO):
        master = EC2("Master")
        core = EC2("Core (Spot!)\nData loss risk!")
    master >> Edge(label="HDFS on Spot\n= data loss", color="#c62828", style="bold") >> core

with d(F, "option_d"):
    with Cluster("EMR Cluster", graph_attr=C_OK):
        master = EC2("Master\n(On-Demand)")
        core = EC2("Core\n(On-Demand)")
        task = EC2("Task (Spot)\nNo HDFS data")
    core >> Edge(label="Safe: no data\non task nodes", color="#2e7d32", style="bold") >> task

with d(F, "option_e"):
    with Cluster("EMR (All Spot!)", graph_attr=C_NO):
        master = EC2("Master (Spot!)")
        core = EC2("Core (Spot!)")
        task = EC2("Task (Spot!)")
    master >> Edge(label="Cluster fails\nif interrupted!", color="#c62828", style="bold") >> core >> task

print("Session 3 Q2 done")

# ============================================================
# SESSION 3, Q3: Glue Workflow Orchestration
# ============================================================
F = "session3/exam-strategy/q3"

with d(F, "option_a"):
    with Cluster("AWS Glue (Native)", graph_attr=C_OK):
        trigger = Glue("Scheduled\nTrigger")
        j1 = Glue("Job A")
        j2 = Glue("Job B")
        j3 = Glue("Job C")
    trigger >> Edge(label="Concurrent", color="#2e7d32", style="bold") >> j1
    trigger >> Edge(label="Concurrent", color="#2e7d32", style="bold") >> j2
    trigger >> Edge(label="Concurrent", color="#2e7d32", style="bold") >> j3

with d(F, "option_b"):
    eb = Eventbridge("EventBridge")
    lam = Lambda("Lambda\n(Custom logic)")
    with Cluster("Glue Jobs", graph_attr=C_N):
        j1 = Glue("Job A")
        j2 = Glue("Job B")
    eb >> Edge(color="#e65100") >> lam >> Edge(label="No visualization", color="#c62828") >> j1
    lam >> j2

with d(F, "option_c"):
    eb = Eventbridge("EventBridge")
    sf = Lambda("Step Functions")
    with Cluster("Glue Jobs", graph_attr=C_N):
        j1 = Glue("Job A")
        j2 = Glue("Job B")
    eb >> Edge(color="#e65100") >> sf >> Edge(label="Over-engineered", color="#c62828") >> j1
    sf >> j2

with d(F, "option_d"):
    ec2 = EC2("EC2\n(Apache Airflow)")
    with Cluster("Glue Jobs", graph_attr=C_N):
        j1 = Glue("Job A")
        j2 = Glue("Job B")
    ec2 >> Edge(label="Self-managed\ninfrastructure", color="#c62828", style="bold") >> j1
    ec2 >> j2

print("Session 3 Q3 done")

# ============================================================
# SESSION 3, Q4: EMR Idle Cluster Termination
# ============================================================
F = "session3/exam-strategy/q4"

with d(F, "option_a"):
    with Cluster("Billing Alarm", graph_attr=C_NO):
        cw = Custom("CloudWatch\nBilling Alarm", f"{ICONS}/Arch_Management-Tools/48/Arch_Amazon-CloudWatch_48.png")
    lam = Lambda("Lambda\nTerminate")
    emr = EC2("EMR Cluster")
    cw >> Edge(label="Reacts to cost\nnot idle state", color="#c62828", style="bold") >> lam >> emr

with d(F, "option_b"):
    with Cluster("EMR Cluster", graph_attr=C_OK):
        emr = EC2("EMR\n(IsIdle metric)")
    cw = Custom("CloudWatch\nIsIdle Alarm", f"{ICONS}/Arch_Management-Tools/48/Arch_Amazon-CloudWatch_48.png")
    sns = Lambda("SNS → Lambda")
    cw >> Edge(label="IsIdle = true", color="#2e7d32", style="bold") >> sns >> Edge(label="Terminate", color="#2e7d32") >> emr

with d(F, "option_c"):
    with Cluster("EMR Primary Node", graph_attr=C_NO):
        emr = EC2("Primary Node")
        script = Lambda("Bash Script\n(every 5 min)")
    emr >> Edge(label="Hardcoded 8hr\nNot flexible", color="#c62828", style="bold") >> script

with d(F, "option_d"):
    ta = Shield("Trusted Advisor")
    user = Users("Manual Action")
    emr = EC2("EMR Cluster")
    ta >> Edge(label="Recommendation\nonly", color="#c62828", style="dashed") >> user >> Edge(label="Manual CLI", color="#c62828") >> emr

print("Session 3 Q4 done")

# ============================================================
# SESSION 3, Q5: Redshift ML
# ============================================================
F = "session3/exam-strategy/q5"

with d(F, "option_a"):
    with Cluster("Amazon Redshift", graph_attr=C_OK):
        rs = Redshift("Redshift ML\n(SQL interface)")
    model = Lambda("SageMaker\nAutopilot\n(behind scenes)")
    rs >> Edge(label="CREATE MODEL\nusing SQL ✓", color="#2e7d32", style="bold") >> model

with d(F, "option_b"):
    rs = Redshift("Redshift")
    s3 = S3("S3 (Extract)")
    sm = Lambda("SageMaker")
    rs >> Edge(label="UNLOAD", color="#e65100") >> s3 >> Edge(label="Train", color="#e65100") >> sm

with d(F, "option_c"):
    rs = Redshift("Redshift")
    ec2 = EC2("EC2 + Spark\n(Self-managed)")
    rs >> Edge(label="Extract + Process\nHigh overhead", color="#c62828", style="bold") >> ec2

with d(F, "option_d"):
    rs = Redshift("Redshift")
    glue = Glue("AWS Glue\n(Extract + Transform)")
    sm = Lambda("SageMaker")
    rs >> Edge(color="#e65100") >> glue >> Edge(label="Multiple services", color="#e65100") >> sm

print("Session 3 Q5 done")

# ============================================================
# SESSION 4, Q1: S3 Upload Integrity
# ============================================================
F = "session4/exam-strategy/q1"

with d(F, "option_a"):
    src = EC2("On-Premises")
    s3 = S3("S3\n(SSE encryption)")
    src >> Edge(label="--sse flag\nEncrypts at rest\nNot integrity check!", color="#c62828", style="bold") >> s3

with d(F, "option_b"):
    src = EC2("On-Premises\n(100 MB file)")
    with Cluster("Multipart Upload", graph_attr=C_OK):
        p1 = S3("Part 1\nMD5 ✓")
        p2 = S3("Part 2\nMD5 ✓")
        p3 = S3("Part 3\nMD5 ✓")
    src >> Edge(label="Upload + verify\neach part", color="#2e7d32", style="bold") >> p1

with d(F, "option_c"):
    src = EC2("On-Premises")
    with Cluster("S3 (Versioning)", graph_attr=C_NO):
        v1 = S3("v1 (corrupt)")
        v2 = S3("v2 (corrupt)")
    src >> Edge(label="Keeps versions\nbut doesn't prevent\ncorruption", color="#c62828", style="bold") >> v1

with d(F, "option_d"):
    src = EC2("On-Premises")
    cf = CloudFront("Transfer\nAcceleration")
    s3 = S3("S3")
    src >> Edge(label="Faster upload\nbut no integrity\nvalidation", color="#c62828", style="bold") >> cf >> s3

print("Session 4 Q1 done")

# ============================================================
# SESSION 4, Q2: S3 Object Lambda
# ============================================================
F = "session4/exam-strategy/q2"

with d(F, "option_a"):
    s3 = S3("S3 Logs")
    athena = Athena("Athena SQL")
    app = Lambda("App")
    s3 >> Edge(label="Ad-hoc query\nNot real-time API", color="#c62828") >> athena >> app

with d(F, "option_b"):
    s3 = S3("S3 Logs")
    app = Lambda("Client App\n(Filter logic)")
    s3 >> Edge(label="GET full object\nFilter client-side", color="#c62828", style="bold") >> app

with d(F, "option_c"):
    s3 = S3("S3 Logs")
    with Cluster("S3 Object Lambda", graph_attr=C_OK):
        lam = Lambda("Lambda\n(Filter + Compress)")
    app = Lambda("Downstream App")
    s3 >> Edge(color="#2e7d32") >> lam >> Edge(label="Standard S3 GET\nTransformed response ✓", color="#2e7d32", style="bold") >> app

with d(F, "option_d"):
    s3 = S3("S3 Logs")
    with Cluster("EMR Cluster", graph_attr=C_NO):
        emr = EC2("Spark\n(Batch processing)")
    s3 >> Edge(label="Batch, not per-request\nHigh overhead", color="#c62828", style="bold") >> emr

print("Session 4 Q2 done")

# ============================================================
# SESSION 4, Q3: PII Detection (same as session 1 q3 concept)
# ============================================================
F = "session4/exam-strategy/q3"

with d(F, "option_a"):
    s3 = S3("S3 Bucket")
    lam = Lambda("Lambda\n(Custom PII)")
    mask = Lambda("Masking App")
    s3 >> Edge(label="New objects only", color="#c62828") >> lam >> mask

with d(F, "option_b"):
    s3 = S3("S3 Bucket")
    eb = Eventbridge("EventBridge")
    mask = Lambda("Masking App")
    s3 >> Edge(label="All uploads\nNo PII check!", color="#c62828", style="bold") >> eb >> mask

with d(F, "option_c"):
    s3 = S3("S3 Data Lake")
    macie = Macie("Amazon Macie")
    eb = Eventbridge("EventBridge")
    mask = Lambda("Masking App")
    s3 >> Edge(label="Scan", color="#2e7d32") >> macie >> Edge(label="Finding", color="#2e7d32", style="bold") >> eb >> Edge(label="Real-time", color="#2e7d32") >> mask

with d(F, "option_d"):
    s3 = S3("S3 Data Lake")
    macie = Macie("Amazon Macie")
    lam = Lambda("Lambda\n(Poll schedule)")
    mask = Lambda("Masking App")
    s3 >> Edge(color="#1565c0") >> macie
    lam >> Edge(label="Poll (delayed)", color="#c62828", style="dashed") >> macie
    lam >> mask

print("Session 4 Q3 done")

# ============================================================
# SESSION 4, Q4: Redshift Data Sharing
# ============================================================
F = "session4/exam-strategy/q4"

with d(F, "option_a"):
    with Cluster("Source Clusters", graph_attr=C_SRC):
        rs1 = Redshift("Cluster 1")
    rs2 = Redshift("New Cluster\n(Exported copy)")
    rs1 >> Edge(label="Export/Import\nData becomes stale", color="#c62828", style="bold") >> rs2

with d(F, "option_b"):
    with Cluster("Production Cluster", graph_attr=C_NO):
        rs = Redshift("Redshift\n(Shared access)")
    user = Users("Marketing Team\n(Direct access)")
    rs >> Edge(label="No workload isolation\nImpacts production", color="#c62828", style="bold") >> user

with d(F, "option_c"):
    with Cluster("Producer Cluster", graph_attr=C_OK):
        rs1 = Redshift("ETL Cluster\n(Producer)")
    with Cluster("Consumer Cluster", graph_attr=C_OK):
        rs2 = Redshift("Marketing Cluster\n(Consumer)")
    rs1 >> Edge(label="Data Sharing\nLive, read-only ✓", color="#2e7d32", style="bold") >> rs2

with d(F, "option_d"):
    rs = Redshift("Redshift")
    s3 = S3("S3\n(UNLOAD)")
    athena = Athena("Athena")
    rs >> Edge(label="UNLOAD", color="#e65100") >> s3 >> Edge(label="Stale data\nSync needed", color="#c62828") >> athena

print("Session 4 Q4 done")

# ============================================================
# SESSION 4, Q5: Credential Management
# ============================================================
F = "session4/exam-strategy/q5"

with d(F, "option_a"):
    ec2 = EC2("EC2 App")
    rds = RDS("RDS SQL Server")
    ec2 >> Edge(label="IAM DB Auth\nNot supported for\nSQL Server!", color="#c62828", style="bold") >> rds

with d(F, "option_b"):
    ec2 = EC2("EC2 App")
    ssm = Shield("Parameter Store")
    rds = RDS("RDS SQL Server")
    ec2 >> Edge(color="#e65100") >> ssm >> Edge(label="No native\nauto-rotation", color="#c62828", style="dashed") >> rds

with d(F, "option_c"):
    ec2 = EC2("EC2 App")
    sts = Shield("AWS STS")
    rds = RDS("RDS SQL Server")
    ec2 >> Edge(label="STS = IAM roles\nNot DB auth!", color="#c62828", style="bold") >> sts

with d(F, "option_d"):
    ec2 = EC2("EC2 App")
    sm = Custom("Secrets Manager\n(Auto-rotation)", f"{ICONS}/Arch_Security-Identity/48/Arch_AWS-Secrets-Manager_48.png")
    rds = RDS("RDS SQL Server")
    ec2 >> Edge(label="Retrieve secret", color="#2e7d32", style="bold") >> sm >> Edge(label="Auto-rotate\nevery 30 days ✓", color="#2e7d32") >> rds

print("Session 4 Q5 done")

# ============================================================
# SESSION 5, Q1: Redshift Data Sharing (Cross-Account)
# ============================================================
F = "session5/exam-strategy/q1"

with d(F, "option_a"):
    rs = Redshift("Central Cluster")
    with Cluster("Region B", graph_attr=C_NO):
        snap = S3("Snapshot Copy\n(Point-in-time)")
        rs2 = Redshift("Restored Cluster")
    rs >> Edge(label="Snapshot\n(stale)", color="#c62828", style="bold") >> snap >> rs2

with d(F, "option_b"):
    with Cluster("Central ETL", graph_attr=C_OK):
        rs = Redshift("Producer Cluster")
    with Cluster("BI Team A (Account 2)", graph_attr=C_OK):
        c1 = Redshift("Consumer A")
    with Cluster("BI Team B (Account 3)", graph_attr=C_OK):
        c2 = Redshift("Consumer B")
    rs >> Edge(label="Data Share\n(Live, no copy)", color="#2e7d32", style="bold") >> c1
    rs >> Edge(label="Data Share", color="#2e7d32", style="bold") >> c2

with d(F, "option_c"):
    rs = Redshift("Central Cluster")
    dms = DatabaseMigrationService("AWS DMS\n(Replication)")
    rs2 = Redshift("Target Cluster")
    rs >> Edge(label="Replication lag\nNot for distribution", color="#c62828", style="bold") >> dms >> rs2

with d(F, "option_d"):
    with Cluster("Region A", graph_attr=C_NO):
        etl1 = Glue("ETL Process A")
    with Cluster("Region B", graph_attr=C_NO):
        etl2 = Glue("ETL Process B")
    with Cluster("Region C", graph_attr=C_NO):
        etl3 = Glue("ETL Process C")
    etl1 >> Edge(label="Duplicate\npipelines", color="#c62828", style="dashed") >> etl2
    etl2 >> Edge(color="#c62828", style="dashed") >> etl3

print("Session 5 Q1 done")

# ============================================================
# SESSION 5, Q2: Redshift Encryption
# ============================================================
F = "session5/exam-strategy/q2"

with d(F, "option_a"):
    with Cluster("Redshift (RA3)", graph_attr=C_NO):
        rs = Redshift("Cluster\nSTOPPED!")
    users = Users("Users\nBlocked!")
    rs >> Edge(label="No queries\nduring encryption", color="#c62828", style="bold") >> users

with d(F, "option_b"):
    with Cluster("Redshift (RA3)", graph_attr=C_NO):
        rs = Redshift("Cluster\n(Encrypting)")
    users = Users("Read only\nNo writes!")
    rs >> Edge(label="Writes suspended\n(too restrictive)", color="#c62828", style="bold") >> users

with d(F, "option_c"):
    with Cluster("Redshift (RA3)", graph_attr=C_NO):
        rs = Redshift("Cluster\n(Encrypting)")
    resize = EC2("Elastic Resize\nAFTER = conflict!")
    rs >> Edge(label="Resize after\ncauses issues", color="#c62828", style="bold") >> resize

with d(F, "option_d"):
    resize = EC2("Elastic Resize\n(Do first)")
    with Cluster("Redshift (RA3)", graph_attr=C_OK):
        rs = Redshift("Cluster\n(Read+Write OK)")
    encrypt = Shield("Encryption\n(Background)")
    resize >> Edge(label="1. Resize first", color="#2e7d32", style="bold") >> rs
    rs >> Edge(label="2. Enable encryption\nFull access continues ✓", color="#2e7d32", style="bold") >> encrypt

print("Session 5 Q2 done")

# ============================================================
# SESSION 5, Q3: Lake Formation Permissions
# ============================================================
F = "session5/exam-strategy/q3"

with d(F, "option_a"):
    with Cluster("More IAM Roles + Policies", graph_attr=C_NO):
        iam1 = Shield("Role A\n+ S3 Policy")
        iam2 = Shield("Role B\n+ S3 Policy")
        iam3 = Shield("Role C\n+ S3 Policy")
    s3 = S3("S3 Data Lake")
    iam1 >> Edge(label="No column-level\naccess possible", color="#c62828", style="bold") >> s3

with d(F, "option_b"):
    s3 = S3("S3 Data Lake")
    with Cluster("AWS Lake Formation", graph_attr=C_OK):
        lf = Glue("Lake Formation\n(Column + Row level)")
    with Cluster("Analyst Groups", graph_attr=C_N):
        g1 = Users("Group A\n(Columns 1-5)")
        g2 = Users("Group B\n(Columns 1-3)")
    s3 >> Edge(color="#2e7d32") >> lf
    lf >> Edge(label="Fine-grained\naccess ✓", color="#2e7d32", style="bold") >> g1
    lf >> Edge(color="#2e7d32", style="bold") >> g2

with d(F, "option_c"):
    sm = Custom("Secrets Manager", f"{ICONS}/Arch_Security-Identity/48/Arch_AWS-Secrets-Manager_48.png")
    users = Users("Analyst Groups")
    s3 = S3("S3 Data Lake")
    sm >> Edge(label="Stores credentials\nNot access policies!", color="#c62828", style="bold") >> users

with d(F, "option_d"):
    vpc = VPC("VPC Endpoint")
    s3 = S3("S3 Data Lake")
    users = Users("Analysts")
    vpc >> Edge(label="Network path only\nNo data filtering", color="#c62828", style="bold") >> s3
    users >> vpc

print("Session 5 Q3 done")

# ============================================================
# SESSION 5, Q4: Glue Data Catalog Encryption
# ============================================================
F = "session5/exam-strategy/q4"

with d(F, "option_a"):
    with Cluster("AWS KMS", graph_attr=C_OK):
        kms = Shield("Customer Managed Key\n(Full control)")
    glue = Glue("Glue Data Catalog\n(Encrypted)")
    ct = Custom("CloudTrail\n(Audit log)", f"{ICONS}/Arch_Management-Tools/48/Arch_AWS-CloudTrail_48.png")
    kms >> Edge(label="Encrypt + Rotate\n+ Modify policy ✓", color="#2e7d32", style="bold") >> glue
    kms >> Edge(label="Log all usage", color="#2e7d32") >> ct

with d(F, "option_b"):
    with Cluster("AWS KMS", graph_attr=C_NO):
        kms = Shield("AWS Managed Key\n(Limited control)")
    glue = Glue("Glue Data Catalog")
    kms >> Edge(label="No policy control\nNo custom rotation", color="#c62828", style="bold") >> glue

with d(F, "option_c"):
    with Cluster("AWS KMS", graph_attr=C_NO):
        kms = Shield("AWS Managed Key")
    glue = Glue("Glue Data Catalog")
    kms >> Edge(label="Context only\nNo audit logging", color="#c62828", style="bold") >> glue

with d(F, "option_d"):
    sm = Custom("Secrets Manager\n(Wrong service!)", f"{ICONS}/Arch_Security-Identity/48/Arch_AWS-Secrets-Manager_48.png")
    glue = Glue("Glue Data Catalog")
    sm >> Edge(label="Can't integrate\nwith Glue encryption", color="#c62828", style="bold") >> glue

print("Session 5 Q4 done")

# ============================================================
# SESSION 5, Q5: S3 Dual-Layer Encryption
# ============================================================
F = "session5/exam-strategy/q5"

with d(F, "option_a"):
    s3 = S3("S3 Bucket")
    kms = Shield("SSE-S3\n(Single layer)")
    s3 >> Edge(label="One encryption layer\nNot dual-layer!", color="#c62828", style="bold") >> kms

with d(F, "option_b"):
    app = EC2("Application")
    kms = Shield("Client-side\nKMS encrypt")
    s3 = S3("S3 Bucket")
    app >> Edge(label="Encrypt before upload\nCan't enforce at bucket", color="#c62828", style="bold") >> kms >> s3

with d(F, "option_c"):
    s3 = S3("S3 Bucket")
    with Cluster("DSSE-KMS (Dual Layer)", graph_attr=C_OK):
        l1 = Shield("Layer 1\nKMS Key")
        l2 = Shield("Layer 2\nKMS Key")
    s3 >> Edge(label="Two independent\nencryption layers ✓", color="#2e7d32", style="bold") >> l1 >> l2

with d(F, "option_d"):
    s3 = S3("S3 Bucket")
    with Cluster("Invalid Config", graph_attr=C_NO):
        k1 = Shield("SSE-KMS")
        k2 = Shield("SSE-S3")
    s3 >> Edge(label="Cannot enable both\nsimultaneously!", color="#c62828", style="bold") >> k1
    s3 >> Edge(color="#c62828", style="dashed") >> k2

print("Session 5 Q5 done")
print("\n✅ All diagrams generated!")
