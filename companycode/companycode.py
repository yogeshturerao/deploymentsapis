import json
import os
import boto3
import random
from boto3.dynamodb.conditions import Key, Attr
def lambda_handler(event, context): 

    def CARRIER():
        list_2a = []
        list_2b = []
        list_3b = []
        response = table.scan()  
        response= response['Items']
        for i in response:
            j = i['M_Id']
            list_2a.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j > int('20000') and j <= int('40000'):
                list_2b.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j <= int('20000'):
                list_3b.append(j)
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1020000))
        Items = result['Items']
        for i in Items:
            versionnumber_icm = i['VersionNumber'] 
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1040000))
        Items = result['Items']
        for i in Items:
            versionnumber_carrier = i['VersionNumber'] 
        if len(list_2a) == int('2'):
            try:
                print('ok')
                for i in response:
                    m_id2 = int(i['M_Id'])
                    id2 = i['Id']
                    if m_id2 == int('1040000'):
                        id2 = int(id2)+1 
                        id2 = int(id2)
                        if id2 > int(40000):
                            print('CARRIER range crossed its limit')
                            break
                        id2 = str(id2)
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id2 },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1040000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_carrier',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': {
                                            ":inc": {"N": "1"},
                                            ":versionnumber_carrier": {"N": str(versionnumber_carrier)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": id2}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                ]
                            )
                        print(response1)
            except Exception as e:
                print("An exception occurred",str(e))
                    
        elif len(list_2b) != int('0'):
            for i in list_2b:
                result = table.query(KeyConditionExpression= Key('M_Id').eq(i))
                Items = result['Items']
                for j in Items:
                    versionnumber_delete = j['VersionNumber']
                result2 = table2.query(KeyConditionExpression= Key('Id').eq(i))
                Items2 = result2['Items'] 
                Items2 = Items2[0]
                if Items2.get('JobId') is None:
                    try: 
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Update': {
                                        'TableName': 'CompanyCode',
                                        'Key': {
                                            'Id': { 'N': str(i) }
                                            },
                
                                        'UpdateExpression': 'SET #JobId = :jobid',
                                        'ExpressionAttributeValues': { 
                                            ":jobid": {"N": str(jobid)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#JobId": "JobId"
                                        }
                                        }
                                    },
                                {
                                    'Delete': {
                                        'TableName': 'CompanyCodeModeration',
                                            'Key': {
                                                'M_Id': { 'N': str(i) } 
                                                },
                                            'ConditionExpression': 'VersionNumber = :versionnumber_delete',
                                            'ExpressionAttributeValues': {
                                            ":versionnumber_delete": {"N": str(versionnumber_delete)}
                                            }
                                            }
                                        },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": i}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                    ]
                                ) 
                        print('Free id is attached successfully with new jobid' +str(jobid)+ ', Free id is deleted from moderation table')
                        return
            
                    except Exception as e:
                        print("An exception occurred",str(e)) 
                else: 
                    tablejobid = str(j['JobId'])
                    print('Another jobid '+tablejobid+' is attached in comapanycode table, and free id is also exist in moderaton table')
                    return
            try:
                for i in response:
                    m_id2 = int(i['M_Id'])
                    id2 = i['Id']
                    if m_id2 == int('1040000'):
                        id2 = int(id2)+1 
                        id2 = int(id2)
                        if id2 > int(40000):
                            print('CARRIER range crossed its limit')
                            break
                        id2 = str(id2)
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id2 },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1040000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_carrier',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': {
                                            ":inc": {"N": "1"},
                                            ":versionnumber_carrier": {"N": str(versionnumber_carrier)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": id2}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                ]
                            )
            except Exception as e:
                print("An exception occurred",str(e))
                    
        elif list_3b != int(0):
            try:
                for i in response:
                    m_id2 = int(i['M_Id'])
                    id2 = i['Id']
                    if m_id2 == int('1040000'):
                        id2 = int(id2)+1
                        if id2 > int(40000):
                            print('CARRIER range crossed its limit')
                            break
                        id2 = str(id2)
                        response = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id2 },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1040000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_carrier',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': {
                                            ":inc": {"N": "1"},
                                            ":versionnumber_carrier": {"N": str(versionnumber_carrier)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": id2}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                    ]
                                )
            except Exception as e:
                print("An exception occurred",str(e))
    
    
    
    def DEVELOPER():
        list_2a = []
        list_2b = []
        list_3b = []
        response = table.scan()  
        response= response['Items']
        for i in response:
            j = i['M_Id']
            list_2a.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j > int('20000') and j <= int('40000'):
                list_2b.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j <= int('20000'):
                list_3b.append(j)
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1020000))
        Items = result['Items']
        for i in Items:
            versionnumber_icm = i['VersionNumber'] 
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1040000))
        Items = result['Items']
        for i in Items:
            versionnumber_carrier = i['VersionNumber'] 
        if len(list_2a) == int('2'):
            try:
                print('ok')
                for i in response:
                    m_id2 = int(i['M_Id'])
                    id2 = i['Id']
                    if m_id2 == int('1040000'):
                        id2 = int(id2)+1 
                        id2 = int(id2)
                        if id2 > int(40000):
                            print('CARRIER range crossed its limit')
                            break
                        id2 = str(id2)
                        
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id2 },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1040000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_carrier',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': {
                                            ":inc": {"N": "1"},
                                            ":versionnumber_carrier": {"N": str(versionnumber_carrier)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": id2}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                ]
                            )
                        print(response1)
            except Exception as e:
                print("An exception occurred",str(e))
                    
        elif len(list_2b) != int('0'):
            for i in list_2b:
                result = table.query(KeyConditionExpression= Key('M_Id').eq(i))
                Items = result['Items']
                for j in Items:
                    versionnumber_delete = j['VersionNumber']
                result2 = table2.query(KeyConditionExpression= Key('Id').eq(i))
                Items2 = result2['Items'] 
                Items2 = Items2[0]
                if Items2.get('JobId') is None:
                    try: 
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Update': {
                                        'TableName': 'CompanyCode',
                                        'Key': {
                                            'Id': { 'N': str(i) }
                                            },
                
                                        'UpdateExpression': 'SET #JobId = :jobid',
                                        'ExpressionAttributeValues': { 
                                            ":jobid": {"N": str(jobid)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#JobId": "JobId"
                                        }
                                        }
                                    },
                                {
                                    'Delete': {
                                        'TableName': 'CompanyCodeModeration',
                                            'Key': {
                                                'M_Id': { 'N': str(i) } 
                                                },
                                            'ConditionExpression': 'VersionNumber = :versionnumber_delete',
                                            'ExpressionAttributeValues': {
                                            ":versionnumber_delete": {"N": str(versionnumber_delete)}
                                            }
                                            }
                                        },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": str(i)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                    ]
                                ) 
                        print('Free id is attached successfully with new jobid' +str(jobid)+ ', Free id is deleted from moderation table')
                        return
            
                    except Exception as e:
                        print("An exception occurred",str(e)) 
                else: 
                    tablejobid = str(j['JobId'])
                    print('Another jobid '+tablejobid+' is attached in comapanycode table, and free id is also exist in moderaton table')
                    return
            try:
                for i in response:
                    m_id2 = int(i['M_Id'])
                    id2 = i['Id']
                    if m_id2 == int('1040000'):
                        id2 = int(id2)+1 
                        id2 = int(id2)
                        if id2 > int(40000):
                            print('CARRIER range crossed its limit')
                            break
                        id2 = str(id2)
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id2 },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1040000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_carrier',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': {
                                            ":inc": {"N": "1"},
                                            ":versionnumber_carrier": {"N": str(versionnumber_carrier)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": id2}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                ]
                            )
            except Exception as e:
                print("An exception occurred",str(e))
                    
        elif list_3b != int(0):
            try:
                for i in response:
                    m_id2 = int(i['M_Id'])
                    id2 = i['Id']
                    if m_id2 == int('1040000'):
                        id2 = int(id2)+1
                        if id2 > int(40000):
                            print('CARRIER range crossed its limit')
                            break
                        id2 = str(id2)
                        response = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id2 },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1040000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_carrier',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': {
                                            ":inc": {"N": "1"},
                                            ":versionnumber_carrier": {"N": str(versionnumber_carrier)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                {
                                    'Update': {
                                        'TableName': 'JobInput',
                                        'Key': {
                                            'JobId': { 'N': str(jobid) }
                                            
                                            },
                                        'UpdateExpression': 'SET #CompanyCode = :comapanycode',
                                        'ExpressionAttributeValues': {
                                            ":comapanycode": {"S": id2}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#CompanyCode": "CompanyCode"
                                        }
                                        }   
                                    }
                                    ]
                                )
            except Exception as e:
                print("An exception occurred",str(e))

    dynamodb= boto3.resource('dynamodb')
    table = dynamodb.Table('CompanyCodeModeration')
    table2 = dynamodb.Table('CompanyCode')
    client = boto3.client('dynamodb')
    Event= event['Records']
    print(event,'event') 
    for event in Event:
        #customertype = event['dynamodb']['NewImage']['CustomerType']['S']
        entitytype = event['dynamodb']['NewImage']['EntityType']['S']
        jobid = event['dynamodb']['NewImage']['JobId']['N']
        eventname = event['eventName']
        if eventname == 'INSERT':
            if entitytype == 'CUSTOMER':
                CARRIER()
            elif entitytype == 'DEVELOPER':
                DEVELOPER() 

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }