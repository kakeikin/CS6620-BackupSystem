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


class CleanerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        dst_bucket: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.cleaner_fn = _lambda.Function(
            self, "CleanerFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_asset("../lambdas/cleaner"),
            timeout=Duration.seconds(60),
            environment={
                "DST_BUCKET": dst_bucket.bucket_name,
                "TABLE_NAME": table.table_name,
            },
        )

        dst_bucket.grant_read_write(self.cleaner_fn)
        table.grant_read_write_data(self.cleaner_fn)

        # Scheduled rule: every 1 minute
        schedule_rule = events.Rule(
            self, "CleanerScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        schedule_rule.add_target(targets.LambdaFunction(self.cleaner_fn))