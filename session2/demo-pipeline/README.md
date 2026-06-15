# Session 2 — CDK Infrastructure

AWS CDK stack for the ETL Pipeline Orchestration demo.

## Architecture

```
IoT Device Simulator
    │
    ▼ (MQTT publish)
IoT Core → IoT Rule
    │
    ▼ (route to stream)
Kinesis Data Streams (2 shards)
    │
    ▼ (delivery stream)
Data Firehose → S3 raw/ (JSON, partitioned by date)
    │
    ├── Glue Crawler → Data Catalog (schema + partitions)
    │
    ▼ (Step Functions orchestration)
    Validate (Lambda) → Glue ETL → Notify (Lambda → SNS)
    │
    ▼
S3 curated/ (Parquet, Snappy, partitioned)
    │
    ▼
Athena (SQL queries, cost comparison)
```

## Resources Created

| Service | Resource | Purpose |
|---------|----------|---------|
| Kinesis | dea-s2-ingestion-stream | 2-shard provisioned stream |
| Firehose | dea-s2-firehose | KDS → S3 raw/ delivery |
| IoT Core | dea_s2_to_kinesis rule | IoT → Kinesis routing |
| S3 | Data Lake Bucket | raw/ and curated/ zones |
| Glue | dea-s2-datalake-crawler | Schema discovery |
| Glue | dea_s2_pipeline database | Data Catalog |
| Glue | dea-s2-transform-etl | JSON → Parquet ETL job |
| Step Functions | dea-s2-etl-orchestrator | Validate → ETL → Notify |
| Lambda | dea-s2-validator | Validates raw data |
| Lambda | dea-s2-notifier | SNS notifications |
| SNS | dea-s2-pipeline-notifications | Pipeline alerts |
| Athena | dea-s2-pipeline workgroup | Query execution |

## Deploy

```bash
cd cdk
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cdk bootstrap   # Only once per account/region
cdk deploy
```

## Teardown

```bash
cdk destroy --all
```

## IoT Device Simulator

The pipeline integrates with the [AWS IoT Device Simulator](https://aws.amazon.com/solutions/implementations/iot-device-simulator/) 
for generating realistic vehicle telemetry. Deploy it separately:

```bash
export REGION=ap-southeast-1
export USER_EMAIL=your-email@example.com

aws cloudformation create-stack \
  --region $REGION \
  --stack-name iot-device-simulator \
  --template-url https://solutions-reference.s3.amazonaws.com/iot-device-simulator/latest/iot-device-simulator.template \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --parameters ParameterKey=UserEmail,ParameterValue=$USER_EMAIL
```

After deployment (~10 min), get the console URL:
```bash
aws cloudformation describe-stacks \
  --region $REGION \
  --stack-name iot-device-simulator \
  --query "Stacks[0].Outputs[?OutputKey=='ConsoleURL'].OutputValue" \
  --output text
```

Create a "Connected Vehicle" device type with topic `iot/simulator/vehicle`. 
The CDK stack's IoT Rule (`dea_s2_to_kinesis`) automatically routes these messages into the pipeline.

## Frontend

```bash
cd ../../   # back to session2/
./setup.sh  # runs on port 8087
```
