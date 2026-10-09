import pandas as pd 
import numpy as np
from dotenv import load_dotenv
import os, io, boto3, ast
from io import StringIO

load_dotenv()

session = boto3.Session(
aws_access_key_id=os.getenv("aws_access_key_id"),
aws_secret_access_key=os.getenv("aws_secret_access_key"),
aws_session_token=os.getenv("aws_session_token"),
region_name="us-east-1",
)

s3_client = session.client("s3")
bucket_name = "microhub-project-bucket"
file_key = "01-raw-bronze/data.csv"
response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

df_collect = pd.read_csv(io.BytesIO(response["Body"].read()), sep=";", low_memory=False)

df_noNa = df_collect.dropna()

df_collect["cpu_percent"] = df_collect["cpu_percent"].fillna(df_noNa["cpu_percent"].mean())
df_collect["cpu_frequency"] = df_collect["cpu_frequency"].fillna(df_noNa["cpu_frequency"].mean())
df_collect["ram_percent"] = df_collect["ram_percent"].fillna(df_noNa["ram_percent"].mean())
df_collect["swap_memory_percent"] = df_collect["swap_memory_percent"].fillna(df_noNa["swap_memory_percent"].mean())
df_collect["upload_speed"] = df_collect["upload_speed"].fillna(round(df_noNa["upload_speed"].mean(), 0))
df_collect["download_speed"] = df_collect["download_speed"].fillna(df_noNa["download_speed"].mean())
df_collect["disk"] = df_collect["disk"].fillna(df_noNa["disk"].mean())

df_collect["cpu_frequency"] = round((df_collect["cpu_frequency"] / 1000), 2)

df_collect["disk"] = round((df_collect["disk"] / (1024 ** 3)), 2)

df_collect["processes_qtd"] = df_collect["processes_qtd"].apply(ast.literal_eval)

df_collect["processes_qtd"] = df_collect["processes_qtd"].str.len()

nome_arquivo = df_collect.loc[0, "user"]
data_nome = df_collect.loc[0, "timestamp"]

df_collect = df_collect.drop(columns=["user"])

csv_buffer = StringIO()
df_collect.to_csv(csv_buffer, index=False)

file_name = f"02-trusted-silver/{nome_arquivo}-{data_nome}.csv"

s3_client.put_object(
    Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue()
)