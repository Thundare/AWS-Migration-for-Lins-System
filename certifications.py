import boto3
import re
import fnmatch

client = boto3.client('s3')

# Over 1,000 objects, requires pagination
paginator = client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='company-info-live')

keys = []
for page in pages:
    for bucket_object in page['Contents']:
        vibe = bucket_object['Key']
        keys.append(vibe)

for key in keys:
    if fnmatch.fnmatch(key, '*/certificates/*/*'):
        split_key = re.split(r'/', key)
        file_name = (split_key[2] + '/' + split_key[3])
        print(file_name)
        jobs = client.copy_object(
            Bucket='ss2-us-public-live',  # Destination bucket
            CopySource=f'company-info-live/{key}',
            Key=f'seller-document/{file_name}',
        )
        print(jobs)
