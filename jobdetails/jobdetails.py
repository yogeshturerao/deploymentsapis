import json
import boto3

def lambda_handler(event, context):
    id=[]
    jobids=[]
    responses = []
    client = boto3.client('dynamodb')
    dynamodb= boto3.resource('dynamodb')
    table = dynamodb.Table('JobDetails')
    response = table.scan() 
    Items = response['Items'] 
    for i in Items:
        jobid = i['JobId']
        id.append(jobid)
    id = set(id)
    for i in id:
        id = str(i)
        jobids.append(id) 
    for i in jobids:
        response = client.query(
            TableName='JobDetails',
            KeyConditionExpression= "JobId = :jobid",
            ExpressionAttributeValues= {
                ":jobid": { "N": i }
                } 
            ) 
        responses.append(response)
        var1=[]
        var2=[]
        var3=[]
        for i in responses:
            items = i['Items']
            for i in items:
                runstatus= i['RunStatus']
                runstatus= runstatus['S']
                task= i['Task']
                task= task['S']
                stagename= i['StageName']
                stagename= stagename['S']
                starttimestamp= i['StartTimeStamp']
                starttimestamp= starttimestamp['N']
                endtimestamp= i['EndTimeStamp']
                endtimestamp=endtimestamp['S']
                tempvar2= {"starttimestamp": starttimestamp, "endtimestamp": endtimestamp, "stagename": stagename, "task": task, "runstatus": runstatus }
                var2.append(tempvar2)
    
    return { 
        'statusCode': 200,
        'job_id': jobid,
        'Job_details': var2
    }
