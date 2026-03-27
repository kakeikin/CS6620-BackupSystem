import os
import json
import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
dynamodb_resource = boto3.resource("dynamodb")

DST_BUCKET = os.environ["DST_BUCKET"]
SRC_BUCKET = os.environ["SRC_BUCKET"]
TABLE_NAME = os.environ["TABLE_NAME"]

table = dynamodb_resource.Table(TABLE_NAME)


def handler(event, context):
    print("Received event:", json.dumps(event))

    detail = event.get("detail", {})
    bucket_name = detail.get("bucket", {}).get("name", "")
    object_key = detail.get("object", {}).get("key", "")
    detail_type = event.get("detail-type", "")

    print(f"detail_type={detail_type}, bucket={bucket_name}, key={object_key}")

    if not object_key:
        print("No object key found, skipping.")
        return

    if bucket_name != SRC_BUCKET:
        print(f"Event is not from source bucket {SRC_BUCKET}, skipping.")
        return

    if "Object Created" in detail_type:
        handle_put(object_key)
    elif "Object Deleted" in detail_type:
        handle_delete(object_key)
    else:
        print(f"Unhandled detail-type: {detail_type}")


def query_all_copies(original_key):
    response = table.query(
        KeyConditionExpression=Key("originalKey").eq(original_key),
    )
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("originalKey").eq(original_key),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))

    return items


def handle_put(original_key):
    """Copy object to dst bucket. Keep at most 3 active copies."""
    timestamp = int(time.time() * 1000)
    copy_key = f"{original_key}_{timestamp}"

    # Copy object to destination bucket
    s3_client.copy_object(
        CopySource={"Bucket": SRC_BUCKET, "Key": original_key},
        Bucket=DST_BUCKET,
        Key=copy_key,
    )
    print(f"Copied {original_key} -> {DST_BUCKET}/{copy_key}")

    now = int(time.time())

    # Record new copy in DynamoDB
    table.put_item(
        Item={
            "originalKey": original_key,
            "copyKey": copy_key,
            "createdAt": now,
        }
    )

    # Query all copies for this original object
    items = query_all_copies(original_key)

    # Keep only active copies
    active_items = [item for item in items if item.get("disowned") != "true"]
    active_items.sort(key=lambda x: x.get("createdAt", 0))

    # Delete oldest copies if active copies exceed 3
    if len(active_items) > 3:
        items_to_delete = active_items[: len(active_items) - 3]

        for item in items_to_delete:
            old_copy_key = item["copyKey"]

            try:
                s3_client.delete_object(Bucket=DST_BUCKET, Key=old_copy_key)
                print(f"Deleted oldest copy from S3: {old_copy_key}")
            except ClientError as e:
                print(f"ClientError deleting oldest copy {old_copy_key}: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error deleting oldest copy {old_copy_key}: {e}")
                continue

            try:
                table.delete_item(
                    Key={
                        "originalKey": original_key,
                        "copyKey": old_copy_key,
                    }
                )
                print(f"Deleted oldest copy record from DynamoDB: {old_copy_key}")
            except ClientError as e:
                print(f"ClientError deleting DynamoDB record for {old_copy_key}: {e}")
            except Exception as e:
                print(f"Unexpected error deleting DynamoDB record for {old_copy_key}: {e}")


def handle_delete(original_key):
    """Mark all copies as disowned. Do NOT delete actual copies."""
    items = query_all_copies(original_key)
    now = int(time.time())

    for item in items:
        if item.get("disowned") == "true":
            continue

        try:
            table.update_item(
                Key={
                    "originalKey": item["originalKey"],
                    "copyKey": item["copyKey"],
                },
                UpdateExpression="SET disowned = :d, disownedAt = :da",
                ExpressionAttributeValues={
                    ":d": "true",
                    ":da": now,
                },
            )
            print(f"Marked disowned: {item['copyKey']}")
        except ClientError as e:
            print(f"ClientError updating {item['copyKey']} as disowned: {e}")
        except Exception as e:
            print(f"Unexpected error updating {item['copyKey']} as disowned: {e}")