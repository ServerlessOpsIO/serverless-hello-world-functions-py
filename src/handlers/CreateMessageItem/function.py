'''Put item in DDB'''
import json
import logging
import os

from datetime import datetime
from uuid import uuid4
import boto3


# This path reflects the packaged path and not repo path to the common
# package for this service.
import common   # pylint: disable=unused-import

DDB_TABLE_NAME = os.environ.get('DDB_TABLE_NAME')
ddb_res = boto3.resource('dynamodb')
ddb_table = ddb_res.Table(DDB_TABLE_NAME)


def _create_item(item: dict) -> dict:
    '''Transform item to put into DDB'''
    dt = datetime.utcnow()
    item['pk'] = str(uuid4())
    item['sk'] = 'v0'
    item['timestamp'] = int(dt.timestamp())

    ddb_table.put_item(
        Item=item
    )

    return {'message_id': item['pk']}


def handler(event, context):
    '''Function entry'''
    message = json.loads(event.get('body'))

    message_id = _create_item(message)

    body = {
        'success': True,
        'message': message_id
    }
    resp = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return resp

