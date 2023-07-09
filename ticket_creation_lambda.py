import boto3
from decimal import Decimal
import pandas as pd
import os
import datetime

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    ticket_db_table = dynamodb.Table('xxxx')
    node_db_table = dynamodb.Table('xxxxx')
    sns_client = boto3.client('sns')
    s3_client = boto3.client('s3')

    api_response = event['body-json']

    valid_fields = ["Impact", "Service ID", "OpCo", "creationDate", "status", "Number of subscribers", "Service impacted", "Severity", "id","category","ETA","ETR"]
#Loss of servive means outgae
    if api_response.get("Impact") == "Loss of Service":
        service_id = api_response.get("Service-ID")
        ticket = ticket_db_table.get_item(Key={"Node_id": service_id}).get("Item")

        if ticket:
            ticket_status = ticket.get("Ticket_Status")
            if ticket_status != api_response.get("status"):
                ticket_db_table.update_item(
                    Key={"Node_id": service_id},
                    UpdateExpression="SET #ts = :status",
                    ExpressionAttributeNames={"#ts": "Ticket_Status"},
                    ExpressionAttributeValues={":status": api_response.get("status")}
                )
                new_outage_flag = "Y" if api_response.get("status") == "OPEN" else "N"
                if new_outage_flag != ticket.get("outage_flag"):
                    node_db_table.update_item(
                        Key={"Node_id": service_id},
                        UpdateExpression="SET outage_flag = :flag",
                        ExpressionAttributeValues={":flag": new_outage_flag}
                    )
                print(f"Updated {service_id} in both ticket_db and node_db")
                if api_response.get("status") == "CLOSED":
                    liberate_db_table = dynamodb.Table('late_liberate')
                    response = liberate_db_table.scan(FilterExpression="Node_id = :service_id", ExpressionAttributeValues={":service_id": service_id})
                    print(f"checking liberate db for service_id ")
                    print("response",response)
                    if response.get("Items") and len(response.get("Items")) > 0:
                        ticket = ticket_db_table.get_item(Key={"Node_id": service_id}).get("Item")
                        ticket = {k: int(v) if isinstance(v, Decimal) else v for k, v in ticket.items()}
                        ticket_details = {k: v for k, v in ticket.items()}
                        ticket_details["Node_id"] = service_id
                        ticket_details["Ticket_Status"] = api_response.get("status")
                        ticket_details["Ticket_creation_Date"] = api_response.get("creationDate")
                        ticket_details["Impacted_Service"] = api_response.get("Service impacted")
                        ticket_details["Ticket_Number"] = api_response.get("id")
                        sns_subject = f"Updated ticket for {service_id} to closed"
                        sns_message = f"{ticket_details}"
                        sns_client.publish(
                            TopicArn='xxxx',
                            Subject=sns_subject,
                            Message=sns_message
                        )
                        print(f"SNS sent for updated ticket {service_id}")
                        df = pd.DataFrame(ticket_details, index=[0])
                        csv_data = df.to_csv(index=False)
                        filename = f"updated_ticket_{service_id}.csv"
                        current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
                        folder_path = f"OUTAGE/{current_date}"
                        bucket_name = 'xxxxxx'
                        s3_key = f"{folder_path}/{filename}"
                        s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=s3_key)
                        print(f" {s3_key} uploaded to S3 bucket")
                    else:
                        print(f"No service ID {service_id} in liberate_db")
            else:
                print(f"No changes for {service_id} status")
        else:
            ticket_data = {field: api_response.get(field) for field in valid_fields if field in api_response}
            ticket_data["Node_id"] = service_id
            ticket_data["Ticket_Status"] = api_response.get("status")
            ticket_data["Ticket_creation_Date"] = api_response.get("creationDate")
            ticket_data["Impacted_Service"] = api_response.get("Service impacted")
            ticket_data["Ticket_Number"]=api_response.get("id")
                  
            ticket_data.pop("status", None)
            ticket_data.pop("creationDate", None)
            ticket_data.pop("Service impacted", None)
            ticket_data.pop("Service ID",None)
            ticket_data.pop("id",None)
    
            node_db_table.update_item(
                Key={"Node_id": service_id},
                UpdateExpression="SET outage_flag = :flag",
                ExpressionAttributeValues={":flag": "Y" if api_response.get("status") == "OPEN" else "N"}
            )

            ticket_db_table.put_item(Item=ticket_data)
    
            print(f"Added {service_id} to ticket_db and node_db")
            df = pd.DataFrame(ticket_data, index=[0])
            csv_data = df.to_csv(index=False)

            
            filename = f"new_ticket_{service_id}.csv"

          
            current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
            folder_path = f"OUTAGE/{current_date}"
            bucket_name = 'xxxxx'
            s3_key = f"{folder_path}/{filename}"
            s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=s3_key)

            print(f" {s3_key} uploaded to S3 bucket")
    
