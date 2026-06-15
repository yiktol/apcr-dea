# Session 3 — CDK Infrastructure

AWS CDK stack for the Data Store Management & Lifecycle demo.

## Architecture

```
S3 Data Lake (with Lifecycle rules)
├── raw/          JSON (Standard)
├── curated/      Parquet (→ Standard-IA after 30d)
├── archive/      (→ Glacier IR after 90d → Expire 365d)
└── versioned/    (Versioning enabled)

AWS Glue
├── Crawler → auto-discover schema
├── Data Catalog (databases + tables)
└── ETL Job (JSON → Parquet)

DynamoDB
├── Table with TTL enabled
└── Streams → Lambda (CDC events)

Athena
├── Query raw JSON → measure scan
├── Query curated Parquet → compare
└── Partition pruning demo
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
python produce_data.py --count 200
```

## Teardown

```bash
cd cdk
cdk destroy --all
```

## Frontend

```bash
cd ../../   # back to session3/
./setup.sh
# App runs on port 8088
```
