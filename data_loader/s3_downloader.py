import boto3
import os
from datetime import datetime

def download_file_from_s3(bucket, key):
    local_path = os.path.join("/tmp/data_loader", os.path.basename(key))
    os.makedirs("/tmp/data_loader", exist_ok=True)
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, local_path)
    return local_path

def move_s3_file_to_archive(bucket, source_key, archive_prefix="archive/"):
    s3 = boto3.client('s3')
    filename = source_key.split("/")[-1]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_key = f"{archive_prefix}{filename.rsplit('.', 1)[0]}_{timestamp}.{filename.rsplit('.', 1)[-1]}"
    copy_source = {'Bucket': bucket, 'Key': source_key}
    s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=archive_key)
    s3.delete_object(Bucket=bucket, Key=source_key)
    return archive_key
