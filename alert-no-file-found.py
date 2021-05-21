import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr

client = boto3.client('ses')
dynamo_db = boto3.resource('dynamodb')
table = dynamo_db.Table('filedetails')  # DynamoDB table name

def lambda_handler(event, context):
    try:
        now = datetime.datetime.now()
        twentyfour_hours_ago = now - datetime.timedelta(hours=24)
        now = now.strftime('%FT%T')
        twentyfour_hours_ago = twentyfour_hours_ago.strftime('%FT%T')

        response = table.scan(
            FilterExpression = Key('lastmodified').between(twentyfour_hours_ago,now)
            )
        print(response)
        no_of_files = response["Count"]
        files = response["Items"]
        print(no_of_files)
        if no_of_files == 0:
            print('No file found')
            subject = "NO FILES IN THE BUCKET SINCE 24 HOURS"
            body = """
                    <br>
                    This email is to notify you that no file is beind ingested in the bucket since 24 hours
                    """
            message = {"Subject": {"Data": subject}, "Body": {"Html": {"Data": body}}}

            response_ses = client.send_email(Source = "manoj.more@uplight.com", Destination = {"ToAddresses": ["manoj.more@uplight.com"]}, Message = message)
        else:
            print('file found' + str(files))
        
    except Exception as err:
        print("Error fetching data"+str(err))

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('How was that!!')
    }