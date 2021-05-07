import json
import os
import datetime
import uuid
import boto3
from boto3.dynamodb.conditions import Key, Attr 
def lambda_handler(event, context):
    print(event)

    def CUSTOMER(): 
        dynamodb= boto3.resource('dynamodb')
        table = dynamodb.Table('JobsCount')
        response = table.scan() 
        Items = response['Items']
        print(Items)
        for i in Items:
            if i['Id'] == int(1):
                versionnumber_all = i['VersionNumber']
            if i['Id'] == int(3):
                versionnumber_pending = i['VersionNumber']
        response = client.transact_write_items(
        TransactItems= [
            {
                'Put': {
                    'TableName': 'JobInput',
                    'Item': {
                        'JobId': { 'N': str(jobid) },
                        'EntityType': { 'S': entitytype },
                        'Hosting': { 'S': hosting },
                        'Initial': { 'S': initials },
                        'CompanyCode': { 'S': companycode },
                        'InvoiceStartDate': { 'S': invoicedate },
                        'ApprovalStatus': { 'S': approvalstatus },
                        'EndTimestamp': { 'S': endtimestamp },
                        'ApprovedBy': { 'S': approvedby },
                        'CreatedBy': { 'S': createdby },
                        'TrackingId': { 'S': trackingid },
                        'CronListId': { 'S': cronlistid },
                        'CustomerDetailsId': { 'S': customerdetailid },
                        'Id': { 'S': id }
                            }  
                        }
                    },
            {
                'Put': {
                    'TableName': 'CustomerDetails',
                    'Item': {
                        'CustomerDetailsId': { 'S': customerdetailid },
                        'JobId': { 'N': str(jobid) },
                        'FirstName': { 'S': firstname },
                        'LastName': { 'S': lastnme },
                        'TimeZone': { 'S': timezone },
                        'Address': { 'S': address },
                        'City': { 'S': city },
                        'ZipCode': { 'N': zipcode },
                        'BillingAddress': { 'S': billingaddress },
                        'BillingCity': { 'S': billingcity },
                        'BillingState': { 'S': billingstate },
                        'BillingZipCode': { 'N': billinzipcode },
                        'BillingEmail': { 'S': billingemail },
                        'Phone': { 'N': phone },
                        'StartDayOfWeek': { 'S': startdayofweek }
                        
                        }  
                    }
                },
            {
                'Put': {
                    'TableName': 'UserDetails',
                    'Item': {
                        'Id': { 'S': id },
                        'JobId': { 'N': str(jobid) },
                        'UserDetails': { 'S': userdetails }
                            }  
                        }
                    },
            {
                'Put': {
                    'TableName': 'JobIdList',
                    'Item': {
                        'Type': { 'S': 'Job' },
                        'JobId': { 'N': str(jobid) }
                            }  
                        }
                    },
            {
                'Update': {
                    'TableName': 'JobsCount',
                    'Key': {
                        'Id': { 'N': '1' }
                        },
                        'ConditionExpression': 'VersionNumber = :versionnumber_all',
                        'UpdateExpression': 'SET #TotalCount = #TotalCount + :inc , #VersionNumber = #VersionNumber + :inc',
                        'ExpressionAttributeValues': {
                            ":inc": {"N": "1"},
                            ":versionnumber_all": {"N": str(versionnumber_all)}
                        }, 
                        'ExpressionAttributeNames': {
                            "#TotalCount": "TotalCount",
                            "#VersionNumber": "VersionNumber"
                        }
                    }   
                },
            {
                'Update': {
                    'TableName': 'JobsCount',
                    'Key': {
                        'Id': { 'N': '3' }
                        },
                        'ConditionExpression': 'VersionNumber = :versionnumber_pending',
                        'UpdateExpression': 'SET #TotalCount = #TotalCount + :inc , #VersionNumber = #VersionNumber + :inc',
                        'ExpressionAttributeValues': {
                            ":inc": {"N": "1"},
                            ":versionnumber_pending": {"N": str(versionnumber_pending)}
                        },
                        'ExpressionAttributeNames': {
                            "#TotalCount": "TotalCount",
                            "#VersionNumber": "VersionNumber"
                        }
                    }   
                }
                ]
                )

        for items in cronlist:
            customertypeforcron = items['CustomerType']
            filecommandalias = items['FileCommandAlias']
            command = items['Command']
            deployment = items['Deployment']
            filename = filecommandalias[: filecommandalias.index('_')]
            response = client.transact_write_items(
            TransactItems= [
                {
                    'Put': {
                        'TableName': 'CustomerCronJobList',
                        'Item': {
                            'Id': { 'S': id },
                            'FileCommandAlias': { 'S': filecommandalias },
                            'Deployment': { 'S': deployment },
                            'CustomerType': { 'S': customertypeforcron },
                            'FileName': { 'S': filename },
                            'Command': { 'S': command },
                            'JobId': { 'N': str(jobid) }
                                }  
                            }
                        }
                    ]
                )
        
        
        
        
    def DEVELOPER():
        dynamodb= boto3.resource('dynamodb')
        table = dynamodb.Table('JobsCount')
        response = table.scan() 
        Items = response['Items']
        print(Items)
        for i in Items:
            if i['Id'] == int(1):
                versionnumber_all = i['VersionNumber']
            if i['Id'] == int(3):
                versionnumber_pending = i['VersionNumber']
        
        response = client.transact_write_items(
        TransactItems= [
            {
                'Put': {
                    'TableName': 'JobInput',
                    'Item': {
                        'JobId': { 'N': str(jobid) },
                        'EntityType': { 'S': entitytype },
                        'Hosting': { 'S': hosting },
                        'Initial': { 'S': initials },
                        'CompanyCode': { 'S': companycode },
                        'InvoiceStartDate': { 'S': invoicedate },
                        'ApprovalStatus': { 'S': approvalstatus },
                        'EndTimestamp': { 'S': endtimestamp },
                        'ApprovedBy': { 'S': approvedby },
                        'CreatedBy': { 'S': createdby },
                        'TrackingId': { 'S': trackingid },
                        'CronListId': { 'S': cronlistid },
                        'CustomerDetailsId': { 'S': customerdetailid },
                        'Id': { 'S': id }
                            }  
                        }
                    },
            {
                'Put': {
                    'TableName': 'CustomerDetails',
                    'Item': {
                        'CustomerDetailsId': { 'S': customerdetailid },
                        'JobId': { 'N': str(jobid) },
                        'FirstName': { 'S': firstname },
                        'LastName': { 'S': lastnme },
                        'BillingEmail': { 'S': billingemail }
                        
                        }  
                    }
                },
            
            {
                'Put': {
                    'TableName': 'UserDetails',
                    'Item': {
                        'Id': { 'S': id },
                        'JobId': { 'N': str(jobid) },
                        'UserDetails': { 'S': userdetails }
                            }  
                        }
                    },
            {
                'Put': {
                    'TableName': 'JobIdList',
                    'Item': {
                        'Type': { 'S': 'Job' },
                        'JobId': { 'N': str(jobid) }
                            }  
                        }
                    },
            {
                'Update': {
                    'TableName': 'JobsCount',
                    'Key': {
                        'Id': { 'N': '1' }
                        },
                        'ConditionExpression': 'VersionNumber = :versionnumber_all',
                        'UpdateExpression': 'SET #TotalCount = #TotalCount + :inc , #VersionNumber = #VersionNumber + :inc',
                        'ExpressionAttributeValues': {
                            ":inc": {"N": "1"},
                            ":versionnumber_all": {"N": str(versionnumber_all)}
                        }, 
                        'ExpressionAttributeNames': {
                            "#TotalCount": "TotalCount",
                            "#VersionNumber": "VersionNumber"
                        }
                    }   
                },
            {
                'Update': {
                    'TableName': 'JobsCount',
                    'Key': {
                        'Id': { 'N': '3' }
                        },
                        'ConditionExpression': 'VersionNumber = :versionnumber_pending',
                        'UpdateExpression': 'SET #TotalCount = #TotalCount + :inc , #VersionNumber = #VersionNumber + :inc',
                        'ExpressionAttributeValues': {
                            ":inc": {"N": "1"},
                            ":versionnumber_pending": {"N": str(versionnumber_pending)}
                        },
                        'ExpressionAttributeNames': {
                            "#TotalCount": "TotalCount",
                            "#VersionNumber": "VersionNumber"
                        }
                    }   
                }
                ]
                )

    client = boto3.client('dynamodb')
    
    jobid = round(datetime.datetime.utcnow().timestamp() * 1000)
    customerdetailid = uuid.uuid4().hex
    id = uuid.uuid4().hex
    cronlistid = uuid.uuid4().hex
    
    forminput = event['FormInput']
    customerdetails = forminput['CustomerDetails']
    entitytype = forminput['EntityType']
    hosting = forminput['Hosting']
    initials = forminput['Initial'] 
    companycode = ""
    invoicedate = forminput['InvoiceStartDate']
    #approvalstatus = forminput['ApprovalStatus']
    approvalstatus = 'Pending'
    #endtimestamp = forminput['EndTimestamp']
    endtimestamp = ""
    #approvedby = forminput['ApprovedBy']
    approvedby = ""
    createdby = forminput['CreatedBy']
    #trackingid = forminput['TrackingId']
    trackingid = ""
    cronlist = forminput['CronList']
    #print(cronlist)
    
    firstname = customerdetails['FirstName']
    lastnme = customerdetails['LastName']
    timezone = customerdetails['TimeZone']
    address = customerdetails['Address']
    city = customerdetails['City']
    state = customerdetails['State']
    zipcode = customerdetails['ZipCode']
    billingaddress = customerdetails['BillingAddress']
    billingcity = customerdetails['BillingCity']
    billingstate = customerdetails['BillingState']
    billinzipcode = customerdetails['BillingZipCode']
    billingemail = customerdetails['BillingEmail']
    phone = customerdetails['Phone']
    startdayofweek = customerdetails['StartDayOfWeek']
    
    userdetails = forminput['UserDetails']
    userdetails= json.dumps(userdetails)
    
    if entitytype == 'CUSTOMER': 
        CUSTOMER()
    elif entitytype == 'DEVELOPER':
        userdetails = forminput['UserDetails']
        for i in userdetails:
            print(i)
            firstname = i['FirstName']
            lastnme = i['LastName']
            billingemail = i['Email']

            break
        userdetails= json.dumps(userdetails)
        DEVELOPER()
            
            
    return {
        'statusCode': 200,
        'body': { 
            "JobId": jobid,
            "ApprovalStatus": approvalstatus
        }
    }
