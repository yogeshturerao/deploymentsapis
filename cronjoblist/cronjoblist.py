import json
import boto3
from pprint import pprint
def lambda_handler(event, context):
    statelist=[]
    dynamodb= boto3.resource('dynamodb')
    table = dynamodb.Table('CronJobListMaster')
    response = table.scan() 
    print(response)
    Items = response['Items'] 
    #print(Items)
    for index, _ in enumerate(Items):
        del Items[index]['FileName']
        del Items[index]['AliasName']
    print(Items)
    return {
        'statusCode': 200,
        'body': Items
    }