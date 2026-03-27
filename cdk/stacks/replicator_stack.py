from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


class ReplicatorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        src_bucket: s3.IBucket,
        dst_bucket: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.replicator_fn = _lambda.Function(
            self, "ReplicatorFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_asset("../lambdas/replicator"),
            timeout=Duration.seconds(60),
            environment={
                "DST_BUCKET": dst_bucket.bucket_name,
                "SRC_BUCKET": src_bucket.bucket_name,
                "TABLE_NAME": table.table_name,
            },
        )

        src_bucket.grant_read(self.replicator_fn)
        dst_bucket.grant_read_write(self.replicator_fn)
        table.grant_read_write_data(self.replicator_fn)

        # EventBridge rule: S3 events from source bucket -> Replicator
        rule = events.Rule(
            self, "SrcBucketEventRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created", "Object Deleted"],
                detail={
                    "bucket": {"name": [src_bucket.bucket_name]},
                },
            ),
        )
        rule.add_target(targets.LambdaFunction(self.replicator_fn))