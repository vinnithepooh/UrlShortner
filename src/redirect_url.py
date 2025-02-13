import json
import os
import time
import boto3
import logging

dynamodb = boto3.client("dynamodb")
sqs = boto3.client("sqs")

TABLE_NAME = os.environ["DYNAMODB_TABLE"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        short_id = event["pathParameters"]["short_id"]
        logger.info(f"Received short_id: {short_id}")

        # Fetch from DynamoDB
        response = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={"short_id": {"S": short_id}}
        )
        
        logger.info(f"DynamoDB response: {response}")

        item = response.get("Item")
        if not item:
            logger.error(f"No item found in DynamoDB for short_id: {short_id}")
            return {"statusCode": 404, "body": json.dumps({"error": "Short URL not found"})}

        # Check expiry
        expiry_timestamp = int(item["expiry_timestamp"]["N"])
        if time.time() > expiry_timestamp:
            logger.info(f"URL has expired: {short_id}")
            return {"statusCode": 410, "body": json.dumps({"error": "URL has expired"})}

        original_url = item["original_url"]["S"]
        logger.info(f"Retrieved original_url: {original_url}")

        # Log analytics event
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                "timestamp": int(time.time()),
                "short_id": short_id,
                "user_agent": event["headers"].get("User-Agent", "unknown"),
                "ip_address": event["requestContext"]["identity"]["sourceIp"]
            })
        )

        # Redirect
        return {
            "statusCode": 302,
            "headers": {"Location": original_url},
            "body": ""
        }

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
