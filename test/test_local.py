import os
import boto3
from botocore.config import Config
import fiftyone as fo
import tempfile

# Connect to MinIO with explicit signature version
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  
    aws_access_key_id='minio_access_key',   
    aws_secret_access_key='minio_secret_key',  
    config=Config(signature_version='s3v4')  
)

bucket_name = 'coco-val-2017'
prefix = 'val2017/'

# Create a temporary directory to store downloaded files
temp_dir = tempfile.mkdtemp()

# Create a paginator to handle large lists of objects
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

local_files = []

# Iterate through each page of objects
for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            file_key = obj['Key']
            local_path = os.path.join(temp_dir, os.path.basename(file_key))
            s3.download_file(bucket_name, file_key, local_path)
            local_files.append(local_path)


# Create a FiftyOne dataset and add the downloaded images
dataset = fo.Dataset()
dataset.add_images(local_files)

# Display a few samples to confirm
for sample in dataset.take(5):
    print(sample)
fo.pprint(dataset.stats(include_media=True))
# Launch the FiftyOne app to view the dataset
session = fo.launch_app(dataset)
session.wait()

# Cleanup: you might want to delete the temporary directory afterwards
# import shutil
# shutil.rmtree(temp_dir)
