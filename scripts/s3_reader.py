# scripts/s3_reader.py

import boto3
import pandas as pd
from io import StringIO

def read_csv_from_s3(bucket, key, profile_name="GenAI_Permission-688567268018"):
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read().decode('utf-8')
    
    return pd.read_csv(StringIO(body))
