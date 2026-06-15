# AWS Partner Certification Readiness — Data Engineer Associate

Interactive demo applications and infrastructure for the APCR DEA Content Review sessions.

## Sessions

| Session | Port | Topic | Directory |
|---------|------|-------|-----------|
| 1 | 8086 | 5 V's of Big Data Pipeline | `session1/` |
| 2 | 8087 | ETL Pipeline Orchestration | `session2/` |
| 3 | 8088 | Data Store Management | `session3/` |
| 4 | 8089 | Data Operations & Support | `session4/` |
| 5 | 8090 | Data Security & Governance | `session5/` |

## Architecture

Each session follows the same pattern:
- **Streamlit app** — interactive demo pages with live AWS integration
- **CDK infrastructure** — deployable AWS resources in `demo-pipeline/cdk/`
- **Diagrams** — architecture PNGs generated with the `diagrams` library

## Quick Start

### Deploy Infrastructure (Session 2 example)

```bash
cd session2/demo-pipeline/cdk
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cdk bootstrap
cdk deploy
```

### Run Streamlit App

```bash
cd session2
./setup.sh
# Opens on http://localhost:8087
```

### Deploy IoT Device Simulator (for Session 2)

```bash
aws cloudformation create-stack \
  --region ap-southeast-1 \
  --stack-name iot-device-simulator \
  --template-url https://solutions-reference.s3.amazonaws.com/iot-device-simulator/latest/iot-device-simulator.template \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --parameters ParameterKey=UserEmail,ParameterValue=your-email@example.com
```

## Project Structure

```
apcr-dea/
├── session1/              # 5 V's of Big Data
│   ├── Home.py
│   ├── pages/             # Streamlit pages
│   ├── diagrams/          # Architecture PNGs
│   └── demo-pipeline/cdk/ # CDK infrastructure
├── session2/              # ETL Pipeline Orchestration
│   ├── Home.py
│   ├── pages/             # 01_Ingest → 06_Visualize
│   ├── diagrams/          # Architecture PNGs
│   └── demo-pipeline/cdk/ # CDK infrastructure
├── session3/              # Data Store Management
├── session4/              # Data Operations
├── session5/              # Security & Governance
├── infrastructure/        # Shared CloudFormation (VPC, EC2, etc.)
├── iot-device-simulator/  # AWS IoT Device Simulator (source)
├── ppt/                   # PowerPoint content review decks
├── web/                   # Landing page + architecture images
└── aws-icons/             # AWS architecture icon sets
```

## Teardown

```bash
# Per-session CDK stacks
cd session2/demo-pipeline/cdk && cdk destroy --all

# IoT Simulator
aws cloudformation delete-stack --stack-name iot-device-simulator --region ap-southeast-1

# Shared infrastructure
aws cloudformation delete-stack --stack-name <stack-name> --region ap-southeast-1
```
