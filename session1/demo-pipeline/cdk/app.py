#!/usr/bin/env python3
"""CDK App entry point for the 5 V's of Big Data Pipeline."""
import aws_cdk as cdk
from stacks.pipeline_stack import DataPipelineStack

app = cdk.App()

DataPipelineStack(
    app,
    "DataEngineerPipeline",
    description="Session 1: 5 V's of Big Data Pipeline",
    env=cdk.Environment(
        account=cdk.Aws.ACCOUNT_ID,
        region=cdk.Aws.REGION,
    ),
)

app.synth()
