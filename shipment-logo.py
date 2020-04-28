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
cursor.execute("SELECT concat(Vendor_ID, '_logo.jpg') AS vendor_id_logo, concat(upper(Company_Code), '/logo.jpg') "
               "As Company_ID_logo, is_factory, vendor_internal_id, concat(upper(Company_Code), '/logo.png') "
               "AS Company_ID_logo_2, concat(upper(Company_Code), '/logo.JPG') as Company_ID_logo_3 "
               "FROM ss2_migration_latest.SS1_company_code_linkages2;")

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
            if link[2] == 1 and link[3] is not None:

                response = client.copy_object(
                    Bucket='abacus-test-2',
                    CopySource=f'company-logo-live/{key}',
                    Key=f'ss2-us-public-tst/factory-logo/{link[0]}',
                )
                response2 = client.copy_object(
                    Bucket='abacus-test-2',
                    CopySource=f'company-logo-live/{key}',
                    Key=f'ss2-us-public-tst/seller-logo/{link[0]}',
                )
                print(response, response2)
                continue

            elif link[2] == 1:

                response = client.copy_object(
                    Bucket='abacus-test-2',
                    CopySource=f'company-logo-live/{key}',
                    Key=f'ss2-us-public-tst/factory-logo/{link[0]}',
                )
                print(response)
                continue

            elif link[2] == 0:

                response = client.copy_object(
                    Bucket='abacus-test-2',  # Destination bucket
                    CopySource=f'company-logo-live/{key}',
                    Key=f'ss2-us-public-tst/seller-logo/{link[0]}',
                    )
                print(response)
                continue
