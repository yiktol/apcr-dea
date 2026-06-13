# Session 1 Architecture Diagrams

End-to-end data pipeline covering the core concepts from Session 1: **AWS & Data Fundamentals**.

## Architecture Overview

The pipeline shows data flowing through all 5 V's of Big Data using AWS services covered in the session.

```
Data Sources (Variety) → Ingestion (Velocity) → Processing (Velocity) → Storage (Volume) → Analytics (Value)
                                                                              ↓
                                                                     Governance (Veracity)
```

## Diagrams

| # | Diagram | Covers | PPT Slides |
|---|---------|--------|------------|
| 1 | `01_overall_pipeline.png` | Full end-to-end pipeline | All sections |
| 2 | `02_ingestion_layer.png` | Kinesis Data Streams with sharding | 46–47 (Velocity) |
| 3 | `03_processing_layer.png` | Lambda event-driven transform | 48 (Lambda/Velocity) |
| 4 | `04_storage_layer.png` | S3 Data Lake + Lake Formation + Glue | 38–40 (Volume) |
| 5 | `05_analytics_layer.png` | Athena, Redshift, OpenSearch queries | 41–44 (Value/Variety) |
| 6 | `06_governance_layer.png` | Macie PII detection + EventBridge | Exam Q3 (Veracity) |
| 7 | `07_networking_layer.png` | VPC private subnets + NAT + endpoints | 25–27, Exam Q5 |

## 5 V's Mapping

- **Volume** → S3 Data Lake, Lake Formation, Redshift (store any amount)
- **Variety** → Multiple source types (IoT, apps, databases) into structured/semi-structured
- **Velocity** → Kinesis Data Streams + Lambda for real-time ingestion and processing
- **Veracity** → Amazon Macie + EventBridge for automated PII detection and masking
- **Value** → Athena/Redshift/OpenSearch for business analytics and insights

## Regenerate

```bash
cd session1/diagrams
python3 create_diagrams.py
```

Requires: `pip install diagrams` and `graphviz` installed.
