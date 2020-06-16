import boto3
import mysql.connector
import re
import os

client = boto3.client('s3')

# MySQL credentials
cnx = mysql.connector.connect(
    host=os.environ['MSQL_HOST'],
    user=os.environ['MSQL_USER'],
    password=os.environ['MSQL_PASS'],
    database=os.environ['MYSQL_DB']
)
cursor = cnx.cursor()

cursor.execute("SELECT * FROM ss2_migration_latest_0603.SS1_shipment_coa_mapping")

db_output = cursor.fetchall()
cnx.close()

# Over 1,000 objects, requires pagination
paginator = client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='shipment-batch-coa-live')

keys = []
for page in pages:
    for bucket_object in page['Contents']:
        vibe = bucket_object['Key']
        keys.append(vibe)

for key in keys:
    key_slice = re.split('/', key)
    for output in db_output:
        if int(key_slice[0]) in output:
            cp_obj = client.copy_object(
                Bucket='ss2-us-live',  # Destination bucket
                CopySource=f'shipment-batch-coa-live/{key}',
                Key=f'{output[4]}',
            )
            print(cp_obj)
