# Session 1 — CDK Infrastructure

AWS CDK stack for the 5 V's of Big Data pipeline.

## Structure

```
cdk/
├── app.py                   # CDK app entry point
├── cdk.json
├── requirements.txt
├── stacks/
│   └── pipeline_stack.py    # Main stack (all resources)
├── lambda_fns/
│   ├── transform/index.py   # Kinesis → S3 transformer
│   └── masker/index.py      # PII masking function
└── glue_scripts/
    └── raw_to_curated.py    # Glue ETL: JSON → Parquet
```

## Deploy

```bash
cd cdk
pip install -r requirements.txt
cdk bootstrap   # Only once per account/region
cdk deploy
```

## Post-Deploy

```bash
cd ../scripts
python produce_records.py --count 100 --source mixed
python setup_athena.py
```

## Teardown

```bash
cd cdk
cdk destroy --all
```

## Frontend

The web application lives in `session1/` (one level up):

```bash
cd ..
python -m streamlit run Home.py
```
