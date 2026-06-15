# Session 2 — CDK Infrastructure

AWS CDK stack for the ETL Pipeline Orchestration demo.

## Architecture

```
Producers (Streamlit UI)
    │
    ▼
Kinesis Data Streams (2 shards)  ←  resharding demo
    │
    ├──► Lambda Consumer → S3 raw/
    │
    ▼
Data Firehose → S3 firehose/     ←  side-by-side comparison
    │
    ▼
Step Functions (Standard Workflow)
    ├── Validate (Lambda)
    ├── Transform (Glue ETL)
    └── Notify (Lambda + SNS)
    │
    ▼
S3 Data Lake: raw/ → curated/ (Parquet)
    │
    ▼
Athena + Glue Catalog  ←  SQL optimization demo
```

## Structure

```
cdk/
├── app.py                     # CDK app entry point
├── cdk.json
├── requirements.txt
├── stacks/
│   └── pipeline_stack.py      # Main stack (all resources)
├── lambda_fns/
│   ├── consumer/index.py      # Kinesis → S3 raw zone
│   ├── validator/index.py     # Validates data before ETL
│   └── notifier/index.py      # SNS notification on completion
├── glue_scripts/
│   └── transform_etl.py       # Glue ETL: JSON → Parquet (partitioned, deduped)
└── stepfunctions/
    └── etl_workflow.json       # Step Functions ASL definition
```

## Deploy

```bash
cd cdk
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cdk bootstrap   # Only once per account/region
cdk deploy
```

## Post-Deploy

```bash
cd ../scripts
python produce_records.py --count 100
```

## Teardown

```bash
cd cdk
cdk destroy --all
```

## Frontend

The Streamlit app lives in `session2/` (one level up):

```bash
cd ..
./setup.sh
# or
python -m streamlit run Home.py --server.port 8087
```
