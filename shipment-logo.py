import boto3
import mysql.connector
import os

client = boto3.client('s3')

# MySQL credentials

cnx = mysql.connector.connect(
    host=os.environ['MSQL_HOST'],
    user=os.environ['MSQL_USER'],
    password=os.environ['MSQL_PASS'],
    database=os.environ['MYSQL_DB']
)

# MySQL query to grab the logos for the shipment orders

cursor = cnx.cursor()
cursor.execute("SELECT concat(Vendor_ID, '_logo.jpg') AS vendor_id_logo, concat(upper(Company_Code), '/logo.jpg') "
               "As Company_ID_logo, is_factory, vendor_internal_id, concat(upper(Company_Code), '/logo.png') "
               "AS Company_ID_logo_2, concat(upper(Company_Code), '/logo.JPG') as Company_ID_logo_3 "
               "FROM ss2_migration_latest_0603.SS1_company_code_linkages2;")

linkage = cursor.fetchall()
cnx.close()

# Use AWS API to list objects in buckets and store results into a list
list_of_objects = client.list_objects_v2(
    Bucket='company-logo-live'
)

keys = []
for logos in list_of_objects['Contents']:
    keys.append(logos['Key'])

# Condition to determine the seller type(FD / Non-FD), then find a match and copy to new bucket
for key in keys:
    for link in linkage:
        if key in link:
            if link[2] == 1 and link[3] is not None:

                response = client.copy_object(
                    Bucket='ss2-us-public-live',
                    CopySource=f'company-logo-live/{key}',
                    Key=f'factory-logo/{link[0]}',
                )
                response2 = client.copy_object(
                    Bucket='ss2-us-public-live',
                    CopySource=f'company-logo-live/{key}',
                    Key=f'seller-logo/{link[0]}',
                )
                print(response, response2)
                continue

            elif link[2] == 1:

                response = client.copy_object(
                    Bucket='ss2-us-public-live',
                    CopySource=f'company-logo-live/{key}',
                    Key=f'factory-logo/{link[0]}',
                )
                print(response)
                continue

            elif link[2] == 0:

                response = client.copy_object(
                    Bucket='ss2-us-public-live',  # Destination bucket
                    CopySource=f'company-logo-live/{key}',
                    Key=f'seller-logo/{link[0]}',
                    )
                print(response)
                continue
