import boto3
from boto3.dynamodb.conditions import Attr
def lambda_handler(event, context):
# we get the outage ticket details from the api endpoint.

    api_response = event['body-json']

 #aprt from these fields, will not allow any other field 
    valid_fields = ['Liberate_Ticket_ID', 'Liberate_Ticket_Status', 'Liberate_Creation_Timestamp',
                    'Liberate_Closure_Date', 'Account_Number', 'Node_id']
    primary_key = 'Liberate_Ticket_ID'
#if anymissing fields are there, it will be printing in the cconsole saying that this field is missing

    missing_fields = [field for field in valid_fields if field != 'Liberate_Closure_Date' and field not in api_response]
    if missing_fields:
        print("Missing fields:", missing_fields)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('xxxxx')
#checking the ticketid from the event and from the db are equal, so if it is equal then, will ccheck its status, if not will add all the fields in the db
    response = table.scan(FilterExpression=Attr(primary_key).eq(api_response.get(primary_key)))
    items = response.get('Items')

    if items:
        db_item = items[0]
        db_ticket_status = db_item.get('Liberate_Ticket_Status')
   
        api_ticket_status = api_response.get('Liberate_Ticket_Status')
        if api_ticket_status != db_ticket_status:
#udpating the ticket status if there is a change in the ticket status
            table.update_item(
                Key={primary_key: db_item.get(primary_key)},
                UpdateExpression='SET Liberate_Ticket_Status = :status',
                ExpressionAttributeValues={':status': api_ticket_status}
            )
            print("Ticket status updated for Liberate_Ticket_ID:", db_item.get(primary_key))
        else:
            print("No changes for Liberate_Ticket_ID:", db_item.get(primary_key))
    else:
   #if not then adding new ticket into the db
        new_ticket_data = {field: api_response.get(field) for field in valid_fields}
        new_ticket_data['Liberate_Ticket_Status'] = 'open'
        table.put_item(Item=new_ticket_data)
        print("New ticket stored in the database:", new_ticket_data)

    return {
        'statusCode': 200,
        'body': 'Success'
    }
