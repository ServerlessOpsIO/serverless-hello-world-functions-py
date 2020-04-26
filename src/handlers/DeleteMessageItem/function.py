'''Put item in DDB'''
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


def _delete_item(message_id: str) -> dict:
    '''Delete item in DDB'''
    r = ddb_table.delete_item(
        Key={
            'pk': message_id,
            'sk': 'v0'
        },
    )
    return r


def handler(event, context):
    '''Function entry'''
    message_id = event['pathParameters']['messageId']

    r = _delete_item(message_id)
    resp = {
        "statusCode": 200,
        "body": json.dumps({'success': True})
    }

    return resp

