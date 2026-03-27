import os
import json
import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
dynamodb_resource = boto3.resource("dynamodb")

DST_BUCKET = os.environ["DST_BUCKET"]
TABLE_NAME = os.environ["TABLE_NAME"]

table = dynamodb_resource.Table(TABLE_NAME)


def handler(event, context):
    print("Cleaner invoked")

    now = int(time.time())
    cutoff = now - 10  # disowned for longer than 10 seconds

    # Query GSI — no scan needed
    response = table.query(
        IndexName="DisownedIndex",
        KeyConditionExpression=(
            Key("disowned").eq("true") & Key("disownedAt").lte(cutoff)
        ),
    )
    items = response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
        response = table.query(
            IndexName="DisownedIndex",
            KeyConditionExpression=(
                Key("disowned").eq("true") & Key("disownedAt").lte(cutoff)
            ),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))

    print(f"Found {len(items)} disowned copies to clean")

    for item in items:
        copy_key = item["copyKey"]
        original_key = item["originalKey"]

        print(f"Processing disowned copy: {original_key} -> {copy_key}")

        # Step 1: delete copy from S3
        try:
            s3_client.delete_object(Bucket=DST_BUCKET, Key=copy_key)
            print(f"Deleted from S3: {copy_key}")

        except ClientError as e:
            print(f"ClientError deleting {copy_key} from S3: {e}")
            # Keep DynamoDB record so Cleaner can retry next time
            continue

        except Exception as e:
            print(f"Unexpected error deleting {copy_key} from S3: {e}")
            # Keep DynamoDB record so Cleaner can retry next time
            continue

        # Step 2: delete mapping from DynamoDB
        try:
            table.delete_item(
                Key={
                    "originalKey": original_key,
                    "copyKey": copy_key,
                }
            )
            print(f"Deleted from DynamoDB: {original_key} -> {copy_key}")

        except ClientError as e:
            print(f"ClientError deleting DynamoDB record for {copy_key}: {e}")

        except Exception as e:
            print(f"Unexpected error deleting DynamoDB record for {copy_key}: {e}")

    print("Cleaner finished")