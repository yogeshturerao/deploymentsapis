import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    print(event,'event')
    client = boto3.client('dynamodb')
    dynamodb = boto3.resource('dynamodb')
    jobidlisttable = dynamodb.Table('JobIdList')
    jobinputtable = dynamodb.Table('JobInput')
    jobscounttable = dynamodb.Table('JobsCount')
    customerdetailestable = dynamodb.Table('CustomerDetails')

    body = event['Body']
    approvalstatus= body.get('approval_status') or None
    lastjobid = body.get('last_job_id') or None
    JobIdsList_with_approvalstatus = []
    count_limit_var = int('50')
    
    if approvalstatus in ['Pending', 'Rejected', 'Approved']:
        print(approvalstatus)
        if lastjobid is None:
            response = jobidlisttable.query(KeyConditionExpression= Key('Type').eq('Job'),ScanIndexForward=False)
            Items = response['Items']
            for item in Items:
                var = item['JobId']
                response2 = jobinputtable.get_item(Key={'JobId': var })
                item=response2['Item']
                customerid= item['CustomerDetailsId']
                responseA = customerdetailestable.get_item(Key={'CustomerDetailsId': customerid })
                responseA = responseA['Item']
                print(responseA,'responseA')
                firstname= responseA['FirstName']
                lastname= responseA['LastName']
                fullname = firstname+" "+lastname
                item.update({'CustomerName': fullname})
                #customerid= item['CustomerDetailsId']
                #print(customerid,'edhsbbhdwbhb')
                if item['ApprovalStatus'] == approvalstatus:
                    JobIdsList_with_approvalstatus.append(item)
                    if len(JobIdsList_with_approvalstatus) == count_limit_var:
                        break
            print(JobIdsList_with_approvalstatus)

            if response.get('LastEvaluatedKey'):
                while 'LastEvaluatedKey' in response:
                    key = response['LastEvaluatedKey']
                    response = jobidlisttable.query(KeyConditionExpression= Key('Type').eq('Job'),ScanIndexForward=False,ExclusiveStartKey=key)
                    #response = table.scan(ExclusiveStartKey=key)
                    Items = response['Items']
                    for item in Items:
                        var = item['JobId'] 
                        response = jobinputtable.get_item(Key={'JobId': var })
                        item=response['Item']
                        if item['ApprovalStatus'] == approvalstatus:
                            JobIdsList_with_approvalstatus.append(item)
                            if len(JobIdsList_with_approvalstatus) == count_limit_var:
                                break
        else:
            response3 = jobidlisttable.query(KeyConditionExpression= Key('Type').eq('Job'),ScanIndexForward=False)
            Items = response3['Items']
            for item in Items:
                var = item['JobId']
                if int(var) < int(lastjobid):
                    response4 = jobinputtable.get_item(Key={'JobId': var })
                    item=response4['Item']
                    customerid= item['CustomerDetailsId']
                    responseB = customerdetailestable.get_item(Key={'CustomerDetailsId': customerid })
                    responseB = responseB['Item']
                    print(responseB,'responseB')
                    firstname= responseB['FirstName']
                    lastname= responseB['LastName']
                    fullname = firstname+" "+lastname
                    item.update({'CustomerName': fullname})
                    if item['ApprovalStatus'] == approvalstatus:
                        print(True)
                        JobIdsList_with_approvalstatus.append(item)
                        if len(JobIdsList_with_approvalstatus) == count_limit_var:
                            break
            if response3.get('LastEvaluatedKey'):
                while 'LastEvaluatedKey' in response3:
                    key = response3['LastEvaluatedKey']
                    response3 = jobidlisttable.query(KeyConditionExpression= Key('Type').eq('Job'),ScanIndexForward=False,ExclusiveStartKey=key)
                    #response = table.scan(ExclusiveStartKey=key)
                    Items = response3['Items']
                    for item in Items:
                        var = item['JobId']
                        response3 = jobinputtable.get_item(Key={'JobId': var })
                        item=response3['Item']
                        if item['ApprovalStatus'] == approvalstatus:
                            JobIdsList_with_approvalstatus.append(item)
                            if len(JobIdsList_with_approvalstatus) == count_limit_var:
                                break
    
    elif approvalstatus is None or approvalstatus == 'All':
        if lastjobid is None:
            print(True)
            response5 = jobidlisttable.query(KeyConditionExpression= Key('Type').eq('Job'),ScanIndexForward=False)
            #response = table.scan(Limit = 50)
            Items = response5['Items']
            for item in Items:
                var = item['JobId']
                response6 = jobinputtable.get_item(Key={'JobId': var })
                #print(response6)
                
                item2 = response6['Item']
                customerid= item2['CustomerDetailsId']
                responseX = customerdetailestable.get_item(Key={'CustomerDetailsId': customerid })
                responseX = responseX['Item']
                print(responseX,'responseX')
                firstname= responseX['FirstName']
                lastname= responseX['LastName']
                fullname = firstname+" "+lastname
                item2.update({'CustomerName': fullname})
                JobIdsList_with_approvalstatus.append(item2) 
                if len(JobIdsList_with_approvalstatus) == count_limit_var:
                    break
            print(JobIdsList_with_approvalstatus,'hii')
            #print(name)
        else:
            response7 = jobidlisttable.query(KeyConditionExpression= Key('Type').eq('Job'),ScanIndexForward=False)
            #response = table.scan()
            Items = response7['Items']
            for item in Items:
                var = item['JobId']
                if int(var) < int(lastjobid):
                    response8 = jobinputtable.get_item(Key={'JobId': var })
                    item=response8['Item']
                    customerid= item['CustomerDetailsId']
                    responseY = customerdetailestable.get_item(Key={'CustomerDetailsId': customerid })
                    responseY = responseY['Item']
                    print(responseY,'responseY')
                    firstname= responseY['FirstName']
                    lastname= responseY['LastName']
                    fullname = firstname+" "+lastname
                    item.update({'CustomerName': fullname})
                    JobIdsList_with_approvalstatus.append(item)
                    if len(JobIdsList_with_approvalstatus) == count_limit_var:
                        break
            #print(JobIdsList_with_approvalstatus)
    response9 = jobscounttable.scan()
    Items= response9['Items']
    #print(Items)
    for i in Items:
        if i['Id'] == int(1):
            all= i['TotalCount']
        if i['Id'] == int(2):
            approved= i['TotalCount']
        if i['Id'] == int(3):
            pending= i['TotalCount']
        if i['Id'] == int(4):
            rejected= i['TotalCount']
    #approvalstatus = {"All": all, "Approved": approved, "Rejected": rejected, "Pending": pending}
    Approvalstatus = approvalstatus
    if approvalstatus == None or approvalstatus == 'All':
        approvalstatus = all
    elif approvalstatus == 'Approved':
        approvalstatus = approved
    elif approvalstatus == 'Pending':
        approvalstatus = pending
    elif approvalstatus == 'Rejected':
        approvalstatus = rejected
        
    for index, _ in enumerate(JobIdsList_with_approvalstatus):
        del JobIdsList_with_approvalstatus[index]['CustomerDetailsId']
        del JobIdsList_with_approvalstatus[index]['CronListId']
        del JobIdsList_with_approvalstatus[index]['Id']
        del JobIdsList_with_approvalstatus[index]['EntityType']
        del JobIdsList_with_approvalstatus[index]['Hosting']
        del JobIdsList_with_approvalstatus[index]['TrackingId']
    #print(JobIdsList_with_approvalstatus,'hello')

    return {
        'statusCode': 200,
        'body':{
        'approval_status': Approvalstatus,
        'last_job_id': lastjobid,
        'total_count': approvalstatus,
        'job_list': JobIdsList_with_approvalstatus
        }
    }    