'''Get item from DDB'''
import json
import logging
import os

import boto3

# This path reflects the packaged path and not repo path to the common
# package for this service.
from common import DecimalEncoder

DDB_TABLE_NAME = os.environ.get('DDB_TABLE_NAME')
ddb_res = boto3.resource('dynamodb')
ddb_table = ddb_res.Table(DDB_TABLE_NAME)


def _retrieve_item(message_id: str) -> dict:
    '''Get item in DDB'''
    r = ddb_table.get_item(
        Key={
            'pk': message_id,
            'sk': 'v0'
        }
    )
    item = r.get('Item', {})
    item['message_id'] = item.pop('pk')

    return item


def handler(event, context):
    '''Function entry'''
    message_id = event['pathParameters']['messageId']

    item = _retrieve_item(message_id)

    resp = {
        "statusCode": 200,
        "body": json.dumps(item, cls=DecimalEncoder)
    }

    return resp


