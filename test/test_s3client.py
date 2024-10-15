import boto3
from botocore.exceptions import ClientError

def check_s3_image_fetch(s3_client, bucket_name, key):
    """Checks if the S3 client can fetch an image from the specified bucket and key.

    Args:
        s3_client (boto3.client): The S3 client object.
        bucket_name (str): The name of the S3 bucket.
        key (str): The key of the image object within the bucket.

    Returns:
        bool: True if the image can be fetched, False otherwise.
    """

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        print(response)
        create_custom_s3_response(response)
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' does not exist.")
        elif error_code == 'NoSuchKey':
            print(f"Image '{key}' does not exist in bucket '{bucket_name}'.")
        else:
            print(f"Error fetching image: {e}")
        return False

# Example usage:
s3 = boto3.client(
    's3',
    endpoint_url="http://localhost:9000",
    aws_access_key_id ="minio_access_key",
    aws_secret_access_key = "minio_secret_key",
    region_name="us-east-1"
)

def create_custom_s3_response(s3_response):
    # Extract relevant headers from the S3 response
    content_length = s3_response['ContentLength']
    content_type = s3_response['ContentType']
    date = s3_response['ResponseMetadata']['HTTPHeaders']['date']
    last_modified = s3_response['LastModified'].strftime('%a, %d %b %Y %H:%M:%S GMT')
    etag = s3_response['ETag']
    streaming_body = s3_response['Body']
    content = streaming_body.read()
    with open('output_file.jpg', 'wb') as f:
        f.write(content)

    # Customize headers as needed
    headers = {
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': 'https://localhost:9000',
        'Content-Length': str(content_length),
        'Content-Type': content_type,
        'Date': date,
        'ETag': etag,
        'Last-Modified': last_modified,
        'Server': 'hypercorn-h11',  # Custom server name
        'Vary': 'Origin',
        'X-Colab-Notebook-Cache-Control': 'no-cache'
    }

    print(headers)
    return headers


bucket_name = "coco-val-2017"
key = "val2017/000000000139.jpg"

if check_s3_image_fetch(s3, bucket_name, key):
    print("Image fetched successfully.")
else:
    print("Image could not be fetched.")