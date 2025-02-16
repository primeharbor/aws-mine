#!/usr/bin/env python3

import json
import boto3
import gzip
import base64
from botocore.exceptions import BotoCoreError, ClientError
import os

# Environment variables
NOTIFICATION_TOPIC_ARN = os.getenv('NOTIFICATION_TOPIC_ARN')
MINE_TABLE_NAME = os.getenv('MINE_TABLE_NAME')

# AWS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
table = dynamodb.Table(MINE_TABLE_NAME)

def handle_tripped_mine(access_key_id: str, event_time: str):
    try:
        # Get item from DynamoDB
        response = table.get_item(
            Key={'accessKeyId': access_key_id},
            ConsistentRead=True
        )

        item = response.get('Item')
        if item and item.get('tripped'):
            print("Mine has already been tripped")
            return

        # Publish SNS notification
        sns.publish(
            TopicArn=NOTIFICATION_TOPIC_ARN,
            Message=(
                f'Mine with access key ID {access_key_id} and description '
                f'"{item.get("description", "N/A")}" has been tripped.'
            ),
            Subject='aws-mine: Mine tripped',
            MessageAttributes={
                'MessageGroupId': {
                    'DataType': 'String',
                    'StringValue': 'aws-mine'
                }
            }
        )

        # Update DynamoDB item
        table.update_item(
            Key={'accessKeyId': access_key_id},
            UpdateExpression="SET tripped = :tripped, trippedAt = :trippedAt",
            ExpressionAttributeValues={
                ':tripped': True,
                ':trippedAt': event_time
            }
        )
    except (BotoCoreError, ClientError) as e:
        print(f"Error handling tripped mine: {e}")

def lambda_handler(event, context):
    try:
        # Decode and decompress payload
        payload = base64.b64decode(event['awslogs']['data'])
        decompressed = gzip.decompress(payload)
        result = json.loads(decompressed)

        for log_event in result['logEvents']:
            message = json.loads(log_event['message'])
            interesting_fields = {
                'accessKeyId': message['userIdentity']['accessKeyId'],
                'eventTime': message['eventTime'],
                'eventName': message['eventName'],
                'eventSource': message['eventSource'],
                'awsRegion': message['awsRegion'],
                'userAgent': message['userAgent'],
                'sourceIPAddress': message['sourceIPAddress'],
            }
            print("Tripped mine, interesting fields:", interesting_fields)
            handle_tripped_mine(
                interesting_fields['accessKeyId'],
                interesting_fields['eventTime']
            )
    except Exception as e:
        print(f"Error processing event: {e}")
