import json
import os
import boto3
import random

from boto3.dynamodb.conditions import Key, Attr
def lambda_handler(event, context): 

    def ICM(event):
        jobid = event['JobId']
        list_a = []
        list_b = []
        list_c = []
        response = table.scan()  
        response= response['Items']
        for i in response:
            j = i['M_Id']
            list_a.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j <= int('20000'):
                list_b.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j > int('20000') and j < int('40000'):
                list_c.append(j)
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1020000))
        Items = result['Items']
        for i in Items:
            versionnumber_icm = i['VersionNumber'] 
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1040000))
        Items = result['Items']
        for i in Items:
            versionnumber_carrier = i['VersionNumber'] 
        if len(list_a) == int('2'):
            try:
                for i in response:
                    m_id = int(i['M_Id'])
                    #versionnumber = i['VersionNumber']
                    id = i['Id']
                    if m_id == int('1020000'):
                        id = int(id)+1
                        if id > int(20000):
                            print('ICM range crossed its limit')
                            break
                        id = str(id)
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1020000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_icm',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': { 
                                            ":inc": {"N": "1"},
                                            ":versionnumber_icm": {"N": str(versionnumber_icm)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    }
                                ]
                            )
            except Exception as e:
                print("An exception occurred",str(e))
                    
        elif len(list_b) != int('0'):
            print('ok')
            random_num = random.choice(list_b)
            #for i in list_b:
            i=random_num
            result = table.query(KeyConditionExpression= Key('M_Id').eq(i))
            Items = result['Items']
            for j in Items:
                versionnumber_delete = j['VersionNumber']
            result2 = table2.query(KeyConditionExpression= Key('Id').eq(i))
            Items2 = result2['Items']
            print(Items,'result2')
            for j in Items2:
                if j.get('JobId') is None:
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
        elif list_c != int(0):
            try:
                for i in response:
                    m_id = int(i['M_Id'])
                    id3 = i['Id']
                    if m_id == int('1020000'):
                        id3 = int(id3)+1
                        if id3 > int(20000):
                            print('ICM range crossed its limit')
                            break
                        id = str(id3) 
                        response1 = client.transact_write_items(
                            TransactItems= [
                                {
                                    'Put': {
                                        'TableName': 'CompanyCode',
                                        'Item': {
                                            'Id': { 'N': id },
                                            'JobId': { 'N': jobid }
                                            }  
                                        }
                                    },
                                {
                                    'Update': {
                                        'TableName': 'CompanyCodeModeration',
                                        'Key': {
                                            'M_Id': { 'N': '1020000' }
                                            
                                            },
                                        'ConditionExpression': 'VersionNumber = :versionnumber_icm',
                                        'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                        'ExpressionAttributeValues': { 
                                            ":inc": {"N": "1"},
                                            ":versionnumber_icm": {"N": str(versionnumber_icm)}
                                        },
                                        'ExpressionAttributeNames': {
                                            "#Id": "Id",
                                            "#VersionNumber": "VersionNumber"
                                        }
                                        }   
                                    },
                                ]
                            )
            except Exception as e:
                print("An exception occurred",str(e))
                            
    def CARRIER():
        print(jobid,'jobid')
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

        #Code for writing the main table with unique id and jobid
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
                        print('ok')
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
                print(result2)
                Items2 = result2['Items'] 
                print(Items2,'hi')
                Items2 = Items2[0]
                print(Items2)
                #print(Items2.get('JobId'))
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
                                            ":comapanycode": {"S": id2}
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
                        print(id2,'id2') 
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
        list_3a = []
        list_3b = []
        list_3c = []
        response = table.scan()  
        response= response['Items']
        for i in response:
            j = i['M_Id']
            list_3a.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j <= int('20000'):
                list_3b.append(j)
        for i in response:
            j = int(i['M_Id'])
            if j > int('20000') and j <= int('40000'):
                list_3c.append(j)
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1020000))
        Items = result['Items']
        for i in Items:
            versionnumber_icm = i['VersionNumber'] 
        result = table.query(KeyConditionExpression= Key('M_Id').eq(1040000))
        Items = result['Items']
        for i in Items:
            versionnumber_carrier = i['VersionNumber'] 

        if len(list_3a) == int('2'):
            try:
                print('ok')
                for i in response:
                    m_id = int(i['M_Id'])
                    versionnumber = i['VersionNumber']
                    id = i['Id']
                    if m_id == int('1020000'):
                        id = int(id)+1
                        if id <= int('20000'):
                            id = str(id) 
                            print('ok')
                            response1 = client.transact_write_items(
                                TransactItems= [
                                    {
                                        'Put': {
                                            'TableName': 'CompanyCode',
                                            'Item': {
                                                'Id': { 'N': id },
                                                'JobId': { 'N': jobid }
                                                }  
                                            }
                                        },
                                    {
                                        'Update': {
                                            'TableName': 'CompanyCodeModeration',
                                            'Key': {
                                                'M_Id': { 'N': '1020000' }
                                            
                                                },
                                            'ConditionExpression': 'VersionNumber = :versionnumber_icm',
                                            'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                            'ExpressionAttributeValues': {
                                                ":inc": {"N": "1"},
                                                ":versionnumber_icm": {"N": str(versionnumber_icm)}
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
                                                ":comapanycode": {"S": id}
                                            },
                                            'ExpressionAttributeNames': {
                                                "#CompanyCode": "CompanyCode"
                                            }
                                            }   
                                        }
                                    ]
                                )
                        elif id > int('20000') and id <= int('40000'):
                            id = str(id)
                            response1 = client.transact_write_items(
                                TransactItems= [
                                    {
                                        'Put': {
                                            'TableName': 'CompanyCode',
                                            'Item': {
                                                'Id': { 'N': id },
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
                                                ":comapanycode": {"S": id}
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
        elif len(list_3b) != int('0'):
            for i in list_3b:
                result = table.query(KeyConditionExpression= Key('M_Id').eq(i))
                Items = result['Items']
                for j in Items:
                    versionnumber_delete = j['VersionNumber']
                result2 = table2.query(KeyConditionExpression= Key('Id').eq(i))
                Items2 = result2['Items']
                print(Items,'result2')
                Items2 = Items2[0]
                print(Items2)
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
                                            ":comapanycode": {"S": id}
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
                    m_id = int(i['M_Id'])
                    versionnumber = i['VersionNumber']
                    id = i['Id']
                    if m_id == int('1020000'):
                        id = int(id)+1
                        if id <= int('20000'):
                            id = str(id)
                            response1 = client.transact_write_items(
                                TransactItems= [
                                    {
                                        'Put': {
                                            'TableName': 'CompanyCode',
                                            'Item': {
                                                'Id': { 'N': id },
                                                'JobId': { 'N': jobid }
                                                }  
                                            }
                                        },
                                    {
                                        'Update': {
                                            'TableName': 'CompanyCodeModeration',
                                            'Key': {
                                                'M_Id': { 'N': '1020000' }
                                            
                                                },
                                            'ConditionExpression': 'VersionNumber = :versionnumber_icm',
                                            'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                            'ExpressionAttributeValues': {
                                                ":inc": {"N": "1"},
                                                ":versionnumber_icm": {"N": str(versionnumber_icm)}
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
                                                ":comapanycode": {"S": id}
                                            },
                                            'ExpressionAttributeNames': {
                                                "#CompanyCode": "CompanyCode"
                                            }
                                            }   
                                        }
                                    ]
                                )
                        elif id > int('20000') and id <= int('40000'):
                            id = str(id)
                            response1 = client.transact_write_items(
                                TransactItems= [
                                    {
                                        'Put': {
                                            'TableName': 'CompanyCode',
                                            'Item': {
                                                'Id': { 'N': id },
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
                                                ":comapanycode": {"S": id}
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
            
        elif len(list_3c) != int('0'):
            for i in list_3c: 
                result = table.query(KeyConditionExpression= Key('M_Id').eq(i))
                Items = result['Items']
                for j in Items:
                    versionnumber_delete = j['VersionNumber']
                result2 = table2.query(KeyConditionExpression= Key('Id').eq(i))
                Items2 = result2['Items']
                print(Items,'result2')
                Items2 = Items2[0]
                print(Items2)
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
                                                ":comapanycode": {"S": id}
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
                    m_id = int(i['M_Id'])
                    versionnumber = i['VersionNumber']
                    id = i['Id']
                    if m_id == int('1020000'):
                        id = int(id)+1
                        if id <= int('20000'):
                            id = str(id)
                            response1 = client.transact_write_items(
                                TransactItems= [
                                    {
                                        'Put': {
                                            'TableName': 'CompanyCode',
                                            'Item': {
                                                'Id': { 'N': id },
                                                'JobId': { 'N': jobid }
                                                }  
                                            }
                                        },
                                    {
                                        'Update': {
                                            'TableName': 'CompanyCodeModeration',
                                            'Key': {
                                                'M_Id': { 'N': '1020000' }
                                            
                                                },
                                            'ConditionExpression': 'VersionNumber = :versionnumber_icm',
                                            'UpdateExpression': 'SET #Id = #Id + :inc , #VersionNumber = #VersionNumber + :inc',
                                            'ExpressionAttributeValues': {
                                                ":inc": {"N": "1"},
                                                ":versionnumber_icm": {"N": str(versionnumber_icm)}
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
                                                ":comapanycode": {"S": id}
                                            },
                                            'ExpressionAttributeNames': {
                                                "#CompanyCode": "CompanyCode"
                                            }
                                            }   
                                        }
                                    ]
                                )
                        elif id > int('20000') and id <= int('40000'):
                            id = str(id)
                            response1 = client.transact_write_items(
                                TransactItems= [
                                    {
                                        'Put': {
                                            'TableName': 'CompanyCode',
                                            'Item': {
                                                'Id': { 'N': id },
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
                                                ":comapanycode": {"S": id}
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
        customertype = event['dynamodb']['NewImage']['CustomerType']['S']
        entitytype = event['dynamodb']['NewImage']['EntityType']['S']
        jobid = event['dynamodb']['NewImage']['JobId']['N']
        eventname = event['eventName']
        if eventname == 'INSERT':
            if customertype == 'CARRIER':
                CARRIER()
            elif entitytype == 'DEVELOPER':
                DEVELOPER() 


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }