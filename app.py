#!/usr/bin/env python3

import aws_cdk as cdk

from cdk_workshop.cdk_workshop_stack import StorageStack
from cdk_workshop.cdk_workshop_stack import EtlStack
from cdk_workshop.cdk_workshop_stack import OrchestrationStack



app = cdk.App()
storage = StorageStack(app, "StorageStack")
etl = EtlStack(app, "EtlStack",
    bronze_bucket=storage.bronze_bucket,
    silver_bucket=storage.silver_bucket,
    gold_bucket=storage.gold_bucket,
)
OrchestrationStack(app, "OrchestrationStack",
    bronze_to_silver_job_name=etl.bronze_to_silver_job_name,
    validate_silver_job_name=etl.validate_silver_job_name,
    silver_to_gold_job_name=etl.silver_to_gold_job_name,
)

app.synth()
