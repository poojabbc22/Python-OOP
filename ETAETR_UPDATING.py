import json
import requests
import boto3
from datetime import datetime
import pytz
import math


def lambda_handler(event, context):
    auth_username = ''
    auth_password = ''
    dynamodb = boto3.resource('dynamodb')
    ticket_table = dynamodb.Table('xxxxxx')

    query = "SELECT * FROM xxxxx WHERE attribute_exists(XXXXX) AND XXXXX = 'open'"
    response = ticket_table.meta.client.execute_statement(Statement=query)

    ticket_items = response['Items']

    ticket_numbers_no_etr_eta = []  
    ticket_numbers_etaupdated = [] 
    ticket_numbers_etrupdated = []

    for ticket_item in ticket_items:
        XXXXX = ticket_item['XXXXX']
        status = ticket_item.get('XXXXX')

        url = ""
        response = requests.get(url, auth=(auth_username, auth_password))
        response_text = response.text
        response_json = json.loads(response_text)

        if response_json:
            for item in response_json:
                timetracking_category_id = item['xxxxx']
                total_seconds = item['total']

                if timetracking_category_id == n:
                    panama_timezone = pytz.timezone('America/Panama')
                    panama_time = datetime.now(panama_timezone)
                    eta_timestamp = panama_time.strftime("%Y-%m-%d %H:%M:%S")

                    current_eta = ticket_item.get('xxxxxx')
                    current_eta_seconds = convert_hours_to_seconds(current_eta) if current_eta else None

                    rounded_eta_hours = convert_seconds_to_rounded_hours(total_seconds)

                    if current_eta_seconds != total_seconds and rounded_eta_hours != current_eta:
                        ticket_item['xxxxx'] = rounded_XXXXX_hours
                        ticket_item['xxx'] = str(XXXXX_timestamp)
                        ticket_table.update_item(
                            Key={
                                'xxxx': ticket_item['xxxx'],
                                'xxxx': ticket_item['xxxx']
                            },
                            UpdateExpression='SET XXXXX = :val1, XXXXX = :val2',
                            ExpressionAttributeValues={
                                ':val1': ticket_item['yyyyy'],
                                ':val2': ticket_item['yyyyyy']
                            }
                        )
                        ticket_numbers_XXXXXupdated.append(XXXXX)
                    else:
                        print(f"No update xxxxx for {XXXXX}")

                elif timetracking_category_id == 5:
                    panama_timezone = pytz.timezone('America/Panama')
                    panama_time = datetime.now(panama_timezone)
                    etr_timestamp = panama_time.strftime("%Y-%m-%d %H:%M:%S")

                    current_etr = ticket_item.get('xxxxxx')
                    current_etr_seconds = convert_hours_to_seconds(current_etr) if current_etr else None

                    rounded_etr_hours = convert_seconds_to_rounded_hours(total_seconds)

                    if current_etr_seconds != total_seconds and rounded_etr_hours != current_etr:
                        ticket_item['xxxx'] = rounded_etr_hours
                        ticket_item['XXXXX'] = str(etr_timestamp)
                        ticket_table.update_item(
                            Key={
                                'XXXXX': ticket_item['xxxxxx'],
                                'XXXXX': ticket_item['xxxx']
                            },
                            UpdateExpression='SET XXXXX = :val1, XXXXX = :val2',
                            ExpressionAttributeValues={
                                ':val1': ticket_item['xxx'],
                                ':val2': ticket_item['Last_Updated']
                            }
                        )
                        ticket_numbers_etrupdated.append(XXXXX)
                    else:
                        print(f"No update XXXXX for {XXXXX}")
        else:
            ticket_numbers_no_etr_XXXXX.append(XXXXX)

    if ticket_numbers_no_etr_XXXXX:
        for XXXXX in ticket_numbers_no_etr_XXXXX:
            print(f"No XXXXX XXXXX values for XXXXX: {xxxxxx}")

    for XXXXX in ticket_numbers_XXXXXupdated or ticket_numbers_etrupdated:
        ticket_item = get_ticket_item(ticket_items, XXXXX)
        send_ticket_dXXXXXils(ticket_item, 'XXXXX_ETR_update')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def convert_seconds_to_rounded_hours(seconds):
    hours = seconds / 3600
    rounded_hours = math.ceil(hours)
    formatted_hours = '{}h'.format(rounded_hours)
    return formatted_hours

def convert_seconds_to_hours(seconds):
    hours = seconds / 3600
    formatted_hours = '{}h'.format(math.ceil(hours))
    return formatted_hours

def convert_hours_to_seconds(hours):
    return int(float(hours.replace('h', '')) * 3600)

def get_ticket_item(ticket_items, XXXXX):
    for ticket_item in ticket_items:
        if ticket_item['xxxxx'] == XXXXX:
            return ticket_item
    return None

def send_ticket_dXXXXXils(ticket_item, update_type):
    XXXXX = ticket_item.get('xxxxx')
    dynamodb = boto3.resource('dynamodb')
    liberate_table = dynamodb.Table('xxxxxx')
    dynamodb_client = boto3.client('dynamodb')

    query = 'SELECT * FROM "yyyyyy" WHERE XXXXX = {}'.format(f"'{XXXXX}'")
    response = dynamodb_client.execute_statement(Statement=query, ConsistentRead=True)
    

    if 'Items' in response and response['Items']:
        ticket_dXXXXXils = {
            'XXXXX_ETR_update': {
                'XXXXX': ticket_item.get('xxxx'),
                'XXXXX': ticket_item.get('xxx'),
                'xxx': ticket_item.get('xxx'),
                'xxx': ticket_item.get('xxx')
            }
        }

        endpoint_url = ""
        headers = {'Content-Type': 'text/plain','Authorization':''}
        response_rpa = requests.post(endpoint_url, headers=headers, json=data)
        
        if response_rpa.status_code == 200:
            print(f"{update_type} has been sent  for XXXXX: {ticket_item['xxxxxx']}")
        else:
            print(f"Failed to send {update_type} for XXXXX: {ticket_item['xxxxxxxxx']}")
    else:
         print("{xxxx} is not present in xxxx")

    return {"statusCode": 200, "body": "Success"}
