import json
import boto3
from pprint import pprint
def lambda_handler(event, context):
    statelist=[]
    dynamodb= boto3.resource('dynamodb')
    table = dynamodb.Table('StateList')
    response = table.scan() 
    print(response)
    Items = response['Items'] 
    print(Items)
    for state in Items:
        templist=[]
        statecode = state['StateCode'] 
        statename = state['StateName'] 
        templist.append(statecode)
        templist.append(statename)
        statelist.append(templist)
    #pprint(x)
    return {
        'statusCode': 200,
        'body': {"StateList": Items}
    }