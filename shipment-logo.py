import boto3
import mysql.connector
import re
import query
client = boto3.client('s3')

# Grabbing keys from list_objects
cnx = mysql.connector.connect(
    host='ss2-test-database.cl9pljacubb2.us-west-2.rds.amazonaws.com',
    user='ss2_application',
    password='se3cur1ty',
    database='ss2_migration_latest'
)

cursor = cnx.cursor()
cursor.execute("SELECT concat(Vendor_ID, '_logo.jpg') AS vendor_id_logo, concat(Company_Code, '/logo.jpg')"
               " As Company_ID_logo FROM ss2_migration_latest.SS1_company_code_linkages;")

linkage = cursor.fetchall()
cnx.close()
# query end --


list_of_objects = client.list_objects_v2(
    Bucket='company-logo-live'
)

# Storing Keys into a list
keys = []
for logos in list_of_objects['Contents']:
    keys.append(logos['Key'])

for key in keys:
    for link in linkage:
        if key in link:
            response = client.copy_object(
                Bucket='abacus-test-2',  # Destination bucket
                CopySource=f'company-logo-live/{key}',
                Key=f'seller-document/{link[0]}',
                )
            print(response)


