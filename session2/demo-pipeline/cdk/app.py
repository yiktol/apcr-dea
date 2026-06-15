#!/usr/bin/env python3
"""CDK app entry point for Session 2 — ETL Pipeline Orchestration."""
import aws_cdk as cdk
from stacks.pipeline_stack import EtlPipelineStack

app = cdk.App()

EtlPipelineStack(
    app,
    "DeaSession2Pipeline",
    description="DEA Session 2 - ETL Pipeline Orchestration Demo",
)

app.synth()
