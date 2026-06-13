#!/usr/bin/env python3
"""
Post-deployment setup script.
Creates MSCK REPAIR TABLE to register partitions in Athena after data lands.

Usage:
    python setup_athena.py
"""
import boto3
import time


def get_config():
    cfn = boto3.client("cloudformation")
    response = cfn.describe_stacks(StackName="DataEngineerPipeline")
    raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
    # Normalize keys — strip "Output" prefix if present
    config = {}
    for k, v in raw.items():
        normalized = k.replace("Output", "", 1) if k.startswith("Output") else k
        config[normalized] = v
    return config


def run_query(athena, query, database, workgroup):
    """Run an Athena query and wait for completion."""
    print(f"  Running: {query[:80]}...")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print(f"  ✅ Success")
            return True
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
            print(f"  ❌ {state}: {reason}")
            return False
        time.sleep(1)


def main():
    config = get_config()
    database = config["GlueDatabase"]
    workgroup = config["AthenaWorkgroup"]

    athena = boto3.client("athena")

    print(f"Database: {database}")
    print(f"Workgroup: {workgroup}")
    print()

    # Repair tables to load partitions
    print("Repairing tables (loading partitions)...")
    run_query(athena, f"MSCK REPAIR TABLE {database}.events_raw", database, workgroup)
    run_query(athena, f"MSCK REPAIR TABLE {database}.events_curated", database, workgroup)

    # Verify with a count query
    print("\nVerifying data...")
    run_query(athena, f"SELECT COUNT(*) FROM {database}.events_raw", database, workgroup)

    print("\n✅ Setup complete! You can now query data in Athena.")


if __name__ == "__main__":
    main()
