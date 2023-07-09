import json

import boto3
import subprocess
import sys
#installing request module
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')
import requests

#querying the db for ticket details. Using ".query " method is efficient than ".scan"
def fetch_ticket_details(ticket_db_table, node_id):
    response = ticket_db_table.query(
        KeyConditionExpression="xxxx = :xxxxx",
        ExpressionAttributeValues={":node_id": node_id},
        ProjectionExpression="Ticket_Creation_Date, ETA, ETR, Technology, Ticket_Number, Ticket_Status"
    )
    items = response.get('Items', [])
    if items:
        return items[0]
    return None

def lambda_handler(event, context):
    print("event", event)
    #sqs trigger
    
    if 'Records' in event:
     
        sqs_body = event['Records'][0]['body']
        sqs_message = json.loads(sqs_body)
        node_id = sqs_message.get('xxxxx')
        
        dynamodb = boto3.resource('dynamodb')
        ticket_db_table = dynamodb.Table('xxxxxx')
    
        ticket_details = fetch_ticket_details(ticket_db_table, node_id)
        if ticket_details:
            ticket_status = ticket_details.get('Ticket_Status')
            if ticket_status == 'open':
                combined_details = {**sqs_message, **ticket_details, "consent": "N"}  
                create_ticket = {"create_ticket": combined_details}
               
                endpoint_url = ""
                headers = {'Content-Type': 'text/plain','Authorization':''}
                response = requests.post(endpoint_url, headers=headers, json=create_ticket)
                #sending the ticket details to RPA
                if response.status_code == 200:
                    print(f"Message sent to RPA endpoint for Node_id: {node_id}")
                else:
                    print(f"Failed to send message to RPA endpoint for Node_id: {node_id}")
            else:
                print(f"Ticket for Node_id: {node_id} is not open. Skipping RPA message.")
        else:
            print(f"No ticket details found for Node_id: {node_id}. Skipping RPA message.")
         #api trigger   
    elif 'body-json' in event:
    #consent info we receive from api gateway
     
        api_response = event['body-json']
        print("API response:", api_response)

        consent = api_response.get('consent')
        ANI = api_response.get('ANI')
        consent_info = {
            'AAA': ANI,
            'consent': consent
        }
        rpa_message = {"with_Consent": consent_info}
        print("Sending message to RPA endpoint:", rpa_message)
        #sending consent info to RPA
        endpoint_url = ""
        headers = {'Content-Type': 'text/plain','Authorization':' '}
        response = requests.post(endpoint_url, headers=headers, json=rpa_message)

        if response.status_code == 200:
            print("Message sent successfully to RPA endpoint")
        else:
            print("Failed to send message to RPA endpoint:", response.text)
    else:
        print("Invalid trigger source")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid trigger source')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
