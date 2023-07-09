import boto3
import awswrangler as wr
import pandas as pd

NODE_TABLE_NAME = "xxxxx"
TICKET_TABLE_NAME = "xxxxx"

dynamodb_client = boto3.client("dynamodb")
*******************client gave us the list of area codes that are in Panama, so we had put that into s3 and loaded into dynamodb ***********************************
def lambda_handler(event, context):
    s3_path = "xxxxxxxxx"
    #reading the csv provided by them and checking the required fields
    df = wr.s3.read_csv(s3_path + "xxxxxxx.csv
    df_hfc = wr.s3.read_csv("xxxxxxxxxxxx")
    ci_ids = set(df_hfc["CI_ID"].dropna().astype(str))
    df_shortnode = wr.s3.read_csv("s3://xxxxxxxxxxxxxxxxx.csv")
    nr_short_nodes = set(df_shortnode["nr_short_node"].dropna().astype(str))
    matching_hfcnodes = df_hfc[df_hfc["CI_ID"].str.contains('|'.join(nr_short_nodes), na=False)]["CI_ID"]
    ftth_ids_csv = set(df[(df["nr_tel_center"].str.startswith("PAN")) & (df["nr_tel_center"].str.len() >= 16) & df["nr_tel_center"].notnull() & (df["nr_tel_center"] != "")]["nr_tel_center"])

    node_table = dynamodb_client
    query = f"SELECT Node_id FROM {NODE_TABLE_NAME}"
    response = node_table.execute_statement(Statement=query, ConsistentRead=True)
    existing_nodes = set([item["Node_id"]["S"] for item in response["Items"]])
#this dynamodb is to maintain the area code and its OUTAGE status alone
    new_ftthnodes = ftth_ids_csv - existing_nodes
    print("new ftth", len(new_ftthnodes))
    new_hfcnodes = list(set(matching_hfcnodes) - existing_nodes)
    print("new hfc", len(new_hfcnodes))

#checking every csv in order to check if there are any new HFC type of nodes 

    if not new_ftthnodes:
        print("No new FTTH node IDs added ")
    else:
        for node_id in new_ftthnodes:
            node_table.put_item(
                TableName=NODE_TABLE_NAME,
                Item={"Node_id": {"S": node_id}, "outage_flag": {"S": "N"}}
            )
            print(f"{node_id} ftth added")
 #checking every csv in order to check if there are any new HFC type of nodes 
    if not new_hfcnodes:
        print("No new HFC node IDs added ")
    else:
        for node_id in new_hfcnodes:
            node_table.put_item(
                TableName=NODE_TABLE_NAME,
                Item={"Node_id": {"S": node_id}, "outage_flag": {"S": "N"}}
            )
            print(f"{node_id} hfc added")

    ticket_table = dynamodb_client
    query = f"SELECT Node_id, Ticket_Status FROM {TICKET_TABLE_NAME}"
    response = ticket_table.execute_statement(Statement=query, ConsistentRead=True)
    tickets = response["Items"]
# for each node checking the dynamodb which I had to store the tickets from the OTS system, to check the status of that node and update into this dynamodb
    for ticket in tickets:
        ticket_node_id = ticket["Node_id"]["S"]
        ticket_status = ticket["Ticket_Status"]["S"]

        if ticket_node_id in existing_nodes:
            node_response = node_table.get_item(
                TableName=NODE_TABLE_NAME,
                Key={"Node_id": {"S": ticket_node_id}}
            )
            current_outage_flag = node_response["Item"]["outage_flag"]["S"]

            if ticket_status == "closed":
                if current_outage_flag == "N":
                    print(f"Outageflag already N for {ticket_node_id}")
                else:
#updating the status of the nodes in the dyanmodb
                    node_table.update_item(
                        TableName=NODE_TABLE_NAME,
                        Key={"Node_id": {"S": ticket_node_id}},
                        UpdateExpression="SET outage_flag = :flag",
                        ExpressionAttributeValues={":flag": {"S": "N"}}
                    )
                    print(f"outage_flag {ticket_node_id}  to N ")
            elif ticket_status == "open":
                if current_outage_flag == "Y":
                    print(f"Outageflag already Y for {ticket_node_id}")
                else:
                    node_table.update_item(
                        TableName=NODE_TABLE_NAME,
                        Key={"Node_id": {"S": ticket_node_id}},
                        UpdateExpression="SET outage_flag = :flag",
                        ExpressionAttributeValues={":flag": {"S": "Y"}}
                    )
                    print(f"outage_flag  {ticket_node_id} to Y")
        else:
            print(f"{ticket_node_id} not in Node_db")

    return {"statusCode": 200, "body": "Success"}
