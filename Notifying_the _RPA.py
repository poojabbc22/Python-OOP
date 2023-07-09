import json
import boto3
import subprocess
import sys
from decimal import Decimal
 #if you don't have request package, you can use this subprocess to import request into ur code
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')

import requests

def fetch_ticket_details(ticket_db_table, node_id):
#querying the dynamodb for the relevent nodeid and fetching its data from the db
    response = ticket_db_table.query(
        KeyConditionExpression="Node_id = :node_id",
        ExpressionAttributeValues={":node_id": node_id},
        ProjectionExpression="Ticket_Creation_Date, ETA, ETR, Technology, Ticket_Number, Ticket_Status"
    )
    items = response.get('Items', [])
    if items:
        return items[0]
    return None

def lambda_handler(event, context):
    print("event", event)
   #we get the nodeid details from SQS
    sqs_body = event['Records'][0]['body']
    sqs_message = json.loads(sqs_body)
    node_id = sqs_message.get('Node id')
    
    dynamodb = boto3.resource('dynamodb')
    ticket_db_table = dynamodb.Table('xxxxx')

    ticket_details = fetch_ticket_details(ticket_db_table, node_id)
    #after fetch the details from the db ,using endpoint, we are sending the data
    if ticket_details:
        combined_details = {**sqs_message, **ticket_details}  
        endpoint_url = "xxxx"
        headers = {'Content-Type': 'text/plain'}
        response = requests.post(endpoint_url, headers=headers, json=combined_details)
        
        if response.status_code == 200:
            print(f"Message sent to RPA endpoint for Node_id: {node_id}")
        else:
            print(f"Failed to send message to RPA endpoint for Node_id: {node_id}")
    else:
        print(f"No ticket details found for Node_id: {node_id}")
    
    return {
        'statusCode': 200,
       
    }
