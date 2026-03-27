#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.storage_stack import StorageStack
from stacks.replicator_stack import ReplicatorStack
from stacks.cleaner_stack import CleanerStack

app = cdk.App()

storage_stack = StorageStack(app, "StorageStack")

replicator_stack = ReplicatorStack(
    app, "ReplicatorStack",
    src_bucket=storage_stack.src_bucket,
    dst_bucket=storage_stack.dst_bucket,
    table=storage_stack.table,
)
replicator_stack.add_dependency(storage_stack)

cleaner_stack = CleanerStack(
    app, "CleanerStack",
    dst_bucket=storage_stack.dst_bucket,
    table=storage_stack.table,
)
cleaner_stack.add_dependency(storage_stack)

app.synth()