import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr

s3_cient = boto3.client('s3')
dynamo_db = boto3.resource('dynamodb')
table = dynamo_db.Table('filedetails')  # DynamoDB table name


def lambda_handler(event, context):

    for i in event["Records"]:
        action = i["eventName"]
        eventtime = i["eventTime"]
        bucket_name = i["s3"]["bucket"]["name"]
        objects = i["s3"]["object"]["key"]
        etag = i["s3"]["object"]["eTag"]
        
    try:
        table.put_item(
            Item ={
                "etag": str(etag),
                "file_key": objects,
                "lastmodified": eventtime
            })
        print('Data added to Dynamo DB')
    except Exception as err:
        print (">>>>>>>>"+str(err))
    return {
        'statusCode': 200,
        'body': json.dumps('Ho gaya!')
    }
    
