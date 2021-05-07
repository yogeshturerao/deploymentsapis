import json
import pytz
import datetime

def lambda_handler(event, context):
    tempvar2=[]
    timezones = pytz.common_timezones
    #timezones = pytz.all_timezones
    for timezone in timezones:
        tempvar = timezone[0:2]
        if tempvar == 'US':
            tempvar2.append(timezone)
    return {
        'statusCode': 200,
        'body': {"TimeZones": tempvar2}
    }
