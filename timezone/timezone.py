import json
import pytz
import datetime

def lambda_handler(event, context):

    timezones = pytz.common_timezones
    return {
        'statusCode': 200,
        'body': {"TimeZones": timezones}
    }
