import json
import os
import boto3

dynamodb = boto3.client("dynamodb")
TABLE_NAME = os.environ["DYNAMODB_TABLE"]

def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            message = json.loads(record["body"])
            short_id = message["short_id"]
            timestamp = str(message["timestamp"])
            user_agent = message["user_agent"]
            ip_address = message["ip_address"]

            # Update DynamoDB Analytics Table
            dynamodb.update_item(
                TableName=TABLE_NAME,
                Key={"short_id": {"S": short_id}},
                UpdateExpression="ADD click_count :incr SET last_accessed = :timestamp",
                ExpressionAttributeValues={
                    ":incr": {"N": "1"},
                    ":timestamp": {"N": timestamp}
                }
            )

        return {"statusCode": 200, "body": "Analytics processed"}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
