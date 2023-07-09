import json
import subprocess
import sys
import requests
from boto3.dynamodb.conditions import Key
import boto3
from boto3.dynamodb.conditions import Attr
#installing reddis 
subprocess.call('pip install redis -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')
import redis
def get_account_num(ANI):
    redis_conn = None
    redis_endpoint = ""
    redis_port = xxxx
    redis_auth = None
    try:
        #connecting the reddis cache
        redis_conn = redis.StrictRedis(host=redis_endpoint, port=redis_port, db=0)
        response = redis_conn.get(xxx)
    except Exception as ex:
        return {'statusCode': 500, 'message': 'Invalid Request'}
    finally:
        del redis_conn
    return response

def lambda_handler(event, context):
    request_body = event['body-json']
    bussiness_unit = request_body['xxxx']
    ID = request_body['xxxx']
    ANI = request_body['xxxx']

  # checking the buidness unit.
    if bussiness_unit != 'xxx':
        return {'statusCode': 400, 'message': 'Invalid Request'}

    redis_conn = None
    redis_endpoint = ""
    redis_port = xxxx
    redis_auth = None
    try:
        redis_conn = redis.StrictRedis(host=redis_endpoint, port=redis_port, db=0)
        response = redis_conn.get(ID)
        if response is None:
            ID = get_account_num(ANI)
            if ID is not None:
                response = redis_conn.get(ID)
    except Exception as ex:
        return {'statusCode': 500, 'message': 'Invalid Request'}
    finally:
        del redis_conn

    if response is None:
        return {'statusCode': 400, 'message': 'Invalid Request'}
 #taking the fields from the response json
    response_json = json.loads(response)
    nr_short_node = response_json.get('xxxxx')
    nr_tel_center = response_json.get('xxxx')

    dynamodb = boto3.resource('dynamodb')
    node_db_table = dynamodb.Table('xxxx')
    node_db_response = node_db_table.scan(
        FilterExpression=(
            Attr('xxx').contains(short_node) | Attr('xxxx').contains(tel_center))
    )
    print("checked if any of the columns matching the Nodeid in db")
    node_db_items = node_db_response.get('Items', [])

    if len(node_db_items) == 0:
        print("yyyyyy or xxxxxx value not matching with xxxxx db")
        return {'statusCode': 400, 'message': 'Invalid Request'}
    #checking outage flag
    outage_flag = node_db_items[0].get('xxxxx')
    print("outage_flag", outage_flag)

    if outage_flag == 'Y':
        account_number = response_json.get('xxxx')
        phone_number = response_json.get('xxxx')
        Node_db_id = node_db_items[0]['xxxx']
        print("Account Number", account_number)
        ticket_db_table = dynamodb.Table('xxxxx')
        ticket_db_response = ticket_db_table.query(
            KeyConditionExpression=Key('xxxx').eq(Node_db_id)
        )
       #checking the response in ticket dynamodb
        print("Outage is Y so checked for details in ticket_db")
        ticket_db_items = ticket_db_response.get('Items', [])

        if len(ticket_db_items) == 0:
            print("No ticket details matching either tel_center or short_node")
        else:
            for ticket_details in ticket_db_items:
                ticket_status = ticket_details.get('xxxxxxx')
                if ticket_status == 'open':
                    ETR = ticket_details.get('yyyy')
                    if not ETR:  
                        ETR = '6h'
                    #sending sqs to trigger another lambda.
                    sqs_message = {
                        'Account_Number': account_number,
                        'Phone_Number': phone_number,
                        'Node_id': Node_db_id,
                   
                    }
                 
                    sqs = boto3.client('sqs')
                    queue_url = ''
                    sqs.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(sqs_message, default=str)
                    )
                    print(f"SQS has been sent for Node id: {Node_db_id}")
                    #sending response to IVR
                    return {
                        'statusCode': 200,
                        'message': {
                            'ID': ID,
                            'response': response_json
                        },
                        'outage_flag': outage_flag,
                        'ETR': ETR
                    }
               
    else:
        print(f"No SQS has been sent for {node_db_items[0]['xxxx']} which has no outage")

    return {
        'statusCode': 200,
        'message': {
            'ID': ID,
            'response': response_json
        },
        'outage_flag': outage_flag
    }
