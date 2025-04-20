# scripts/s3_uploader.py

import os
import boto3

def upload_folder_to_s3(local_folder, bucket_name, s3_prefix="raw", profile_name="GenAI_Permission-688567268018"):
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            if file.endswith(".csv"):
                local_path = os.path.join(root, file)
                s3_key = os.path.join(s3_prefix, file)

                print(f"⬆️ Uploading {file} to s3://{bucket_name}/{s3_key}")
                s3.upload_file(local_path, bucket_name, s3_key)

    print("✅ All files uploaded to S3.")
