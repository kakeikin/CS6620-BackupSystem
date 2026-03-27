import os
import json
import time
import boto3
from boto3.dynamodb.conditions import Key

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

    if "Object Created" in detail_type:
        handle_put(object_key)
    elif "Object Deleted" in detail_type:
        handle_delete(object_key)
    else:
        print(f"Unhandled detail-type: {detail_type}")


def handle_put(original_key):
    """Copy object to dst bucket. Keep at most 3 copies, delete oldest if exceeded."""
    timestamp = int(time.time() * 1000)
    copy_key = f"{original_key}/{timestamp}"

    # Copy object
    s3_client.copy_object(
        CopySource={"Bucket": SRC_BUCKET, "Key": original_key},
        Bucket=DST_BUCKET,
        Key=copy_key,
    )
    print(f"Copied {original_key} -> {DST_BUCKET}/{copy_key}")

    # Record in DynamoDB
    now = int(time.time())
    table.put_item(
        Item={
            "originalKey": original_key,
            "copyKey": copy_key,
            "createdAt": now,
        }
    )

    # Query all copies for this original key
    response = table.query(
        KeyConditionExpression=Key("originalKey").eq(original_key),
    )
    items = response.get("Items", [])

    # Filter only active (non-disowned) copies
    active_items = [item for item in items if "disowned" not in item]
    active_items.sort(key=lambda x: x.get("createdAt", 0))

    # If more than 3, delete the oldest
    if len(active_items) > 3:
        items_to_delete = active_items[: len(active_items) - 3]
        for item in items_to_delete:
            old_copy_key = item["copyKey"]
            s3_client.delete_object(Bucket=DST_BUCKET, Key=old_copy_key)
            table.delete_item(
                Key={
                    "originalKey": original_key,
                    "copyKey": old_copy_key,
                }
            )
            print(f"Deleted oldest copy: {old_copy_key}")


def handle_delete(original_key):
    """Mark all copies as disowned. Do NOT delete actual copies."""
    response = table.query(
        KeyConditionExpression=Key("originalKey").eq(original_key),
    )
    items = response.get("Items", [])
    now = int(time.time())

    for item in items:
        if item.get("disowned") == "true":
            continue
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