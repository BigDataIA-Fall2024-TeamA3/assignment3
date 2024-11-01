import boto3
from botocore.exceptions import ClientError
from config import fastapi_config

def get_s3_client():
    s3 = boto3.client(
        's3',
        aws_access_key_id=fastapi_config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=fastapi_config.AWS_SECRET_ACCESS_KEY,
        region_name=fastapi_config.AWS_REGION
    )
    return s3

def list_buckets():
    s3 = get_s3_client()
    response = s3.list_buckets()
    return response

def list_objects():
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=fastapi_config.S3_BUCKET_NAME)
    return response

def upload_file(file_name, bucket_name=fastapi_config.S3_BUCKET_NAME):
    s3 = get_s3_client()
    try:
        response = s3.upload_file(file_name, bucket_name, file_name)
        
    except ClientError as e:
        return e
    return response

def download_file(file_name, bucket_name):
    s3 = get_s3_client()
    try:
        response = s3.download_file(bucket_name, file_name, file_name)
    except ClientError as e:
        return e
    return response

def check_connection():
    s3 = get_s3_client()
    try:
        response = s3.list_buckets()
    except ClientError as e:
        return e
    return response

def get_document_details(document_name: str):
    s3_client = get_s3_client()
    try:
        response = s3_client.head_object(Bucket=fastapi_config.S3_BUCKET_NAME, Key=document_name)
        # Extract relevant details and return a JSON serializable object
        details = {
            "ContentLength": response["ContentLength"],
            "ContentType": response["ContentType"],
            "LastModified": response["LastModified"].isoformat(),
            "ETag": response["ETag"]
        }
        return details
    except ClientError as e:
        raise e


# Document_utils_response_s3

def list_s3_documents():
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=fastapi_config.S3_BUCKET_NAME)
        documents = [
            {
                "key": obj["Key"],
                "last_modified": obj["LastModified"].isoformat(),
                "size": obj["Size"],
                "url": f"https://{fastapi_config.S3_BUCKET_NAME}.s3.amazonaws.com/{obj['Key']}"
            }
            for obj in response.get('Contents', [])
        ]
        return documents
    except Exception as e:
        print(f"Error listing S3 documents: {e}")
        return []    
