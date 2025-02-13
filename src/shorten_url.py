import json
import os
import time
import hashlib
import boto3
import base64

dynamodb = boto3.client("dynamodb")

TABLE_NAME = os.environ["DYNAMODB_TABLE"]

def generate_short_code(user_id, original_url):
    """Generate a short code based on user_id and URL hash."""
    hash_value = hashlib.sha256(f"{user_id}-{original_url}".encode()).digest()
    return base64.urlsafe_b64encode(hash_value)[:8].decode('utf-8')

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])
        original_url = body.get("url")
        expiry_minutes = int(body.get("expiry_minutes", 1440))  # Default 24 hours
        
        # Extract user_id from request context (assuming IAM auth)
        user_id = "anonymous"
        if event.get("requestContext") and event["requestContext"].get("identity"):
            user_arn = event["requestContext"]["identity"]["userArn"]                   #.split("/")[-1]
            if user_arn:
                user_id = user_arn.split("/")[-1]

        if not original_url:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing URL"})}

        short_id = generate_short_code(user_id, original_url)
        expiry_timestamp = int(time.time()) + (expiry_minutes * 60)

        # Store in DynamoDB
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                "short_id": {"S": short_id},
                "original_url": {"S": original_url},
                "expiry_timestamp": {"N": str(expiry_timestamp)},
                "created_at": {"N": str(int(time.time()))},
            }
        )

        return {
            "statusCode": 201,
            "body": json.dumps({"short_url": f"https://example.com/{short_id}"})
        }

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
