import boto3
import mysql.connector
import re
import query as call
client = boto3.client('s3')


def get_doc_list():
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket='qcdocs-live')
    obj_list = []
    for page in pages:
        for bucket_object in page['Contents']:
            vibe = bucket_object['Key']
            obj_list.append(vibe)
    return obj_list


bucket_objects = get_doc_list()
#
# for docs in bucket_objects:
#     jobs = client.copy_object(
#         Bucket='abacus-test-2',  # Destination bucket
#         CopySource=f'qcdocs-tst/{docs}',
#         Key=f'seller-document/id/{docs}',
#     )
#     print(jobs)
#

# for obj in bucket_objects:   # Loop through objects in bucket one time
#     match = re.search(, str)
#     print(match)

slicer = slice(-9)
