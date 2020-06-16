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

cursor.execute("SELECT m.seller_id, concat(sku, '/',file_name_orig) AS sku1, "
               "concat(chempax_sku, '/', file_name_orig) AS sku2 "
               "FROM ss2_migration_latest_0603.products p INNER JOIN master_products m "
               "ON m.master_product_id = p.master_product_id "
               "INNER JOIN documents d "
               "ON d.seller_id = m.seller_id")

sql_docs = cursor.fetchall()
cnx.close()
print('Query ran!')
# query end --

# Over 1,000 objects, requires pagination
paginator = client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='qcdocs-live')

keys = []
for page in pages:
    for bucket_object in page['Contents']:
        vibe = bucket_object['Key']
        keys.append(vibe)


for key in keys:
    for docs in sql_docs:
        if key in docs:
            key_split = re.split('/', key)
            jobs = client.copy_object(
                Bucket='ss2-us-public-live',  # Destination bucket
                CopySource=f'qcdocs-live/{key}',
                Key=f'seller-document/{docs[0]}/{key_split[1]}',
            )
            print(jobs)
