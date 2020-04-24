import boto3
import mysql.connector
import re
import query
client = boto3.client('s3')


cnx = mysql.connector.connect(
    host='ss2-test-database.cl9pljacubb2.us-west-2.rds.amazonaws.com',
    user='ss2_application',
    password='se3cur1ty',
    database='ss2_migration_latest'
)

cursor = cnx.cursor()
cursor.execute("SELECT Vendor_ID, Company_Code FROM ss2_migration_latest.SS1_company_code_linkages;")

linkage = cursor.fetchall()
cnx.close()
# query end --

list_of_objects = client.list_objects_v2(
    Bucket='company-logo-live'
)
# Grabbing keys from list_objects

for row in linkage:
    print(row[1])
    if row[1] in list_of_objects:
        print('yup')
    else:
        print('Nope')
        # match = re.

keys = []
for logos in list_of_objects['Contents']:
    keys.append(logos['Key'])

for logo in keys:
    pass


def get_logos():
    response = client.copy_object(
        Bucket='company-logo-live',
        CopySource='company-logo-live/',
        Key='abacus-test-2/',
    )

