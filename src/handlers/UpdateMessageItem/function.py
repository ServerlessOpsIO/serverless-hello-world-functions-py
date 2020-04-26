'''Update item in DDB'''
import json
import logging
import os

import boto3


# This path reflects the packaged path and not repo path to the common
# package for this service.
import common   # pylint: disable=unused-import

DDB_TABLE_NAME = os.environ.get('DDB_TABLE_NAME')
ddb_res = boto3.resource('dynamodb')
ddb_table = ddb_res.Table(DDB_TABLE_NAME)


def _update_item(message_id: str, attrs: dict) -> dict:
    '''Update item in DDB'''

    attribute_updates = {}
    for key in attrs.keys():
        attribute_updates[key] = {'Action': 'PUT', 'Value': attrs.get(key)}

    r = ddb_table.update_item(
        Key={
            'pk': message_id,
            'sk': 'v0'
        },
        AttributeUpdates=attribute_updates
    )
    return r


def handler(event, context):
    '''Function entry'''
    message_id = event['pathParameters']['messageId']
    attr = json.loads(event.get('body'))

    r = _update_item(message_id, attr)
    resp = {
        "statusCode": 200,
        "body": json.dumps({'success': True})
    }

    return resp

