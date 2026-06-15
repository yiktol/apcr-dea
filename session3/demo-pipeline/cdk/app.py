#!/usr/bin/env python3
"""CDK app entry point for Session 3 — Data Store Management & Lifecycle."""
import aws_cdk as cdk
from stacks.datastore_stack import DataStoreStack

app = cdk.App()

DataStoreStack(
    app,
    "DeaSession3DataStore",
    description="DEA Session 3 - Data Store Management & Lifecycle Demo",
    env=cdk.Environment(
        account=cdk.Aws.ACCOUNT_ID,
        region="ap-southeast-1",
    ),
)

app.synth()
