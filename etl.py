import pandas as pd 
from dotenv import load_dotenv
import os, io, boto3, ast, mysql.connector, json
from io import StringIO

load_dotenv()

#========================================================================================================================================================================
#CONFIG
#========================================================================================================================================================================
session = boto3.Session(
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
    aws_session_token=os.getenv("aws_session_token"),
    region_name="us-east-1",
)
conexao = mysql.connector.connect(
    host=os.getenv("host"),
    user=os.getenv("user"),
    password=os.getenv("password"),
    database=os.getenv("database"),
)

cursor = conexao.cursor()
s3_client = session.client("s3")
bucket_name = "microhub-project-bucket"

#==========================================================================================================================================================================
#BRONZE2SILVER
#==========================================================================================================================================================================

file_key1 = "01-raw-bronze/data.csv"
response = s3_client.get_object(Bucket=bucket_name, Key=file_key1)

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

df_collect["timestamp"] = df_collect["timestamp"].apply(pd.to_datetime, errors="coerce")

nome_arquivo = df_collect.loc[0, "user"]

timestamp_arquivo = df_collect.loc[0, "timestamp"]

csv_buffer = StringIO()
df_collect.to_csv(csv_buffer, index=False)

file_name = f"02-trusted-silver/{nome_arquivo}_{timestamp_arquivo}.csv"

file_nameAtt = f"02-trusted-silver/{nome_arquivo}.csv"

s3_client.put_object(
    Bucket=bucket_name, Key=file_nameAtt, Body=csv_buffer.getvalue()
)

s3_client.put_object(
    Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue()
)

#==========================================================================================================================================================================
#SILVER2GOLD
#==========================================================================================================================================================================

cursor.execute(
    "SELECT e.id, e.nome_fantasia FROM empresa e "
    "JOIN microsservico mc ON e.id = mc.empresa_id "
    "JOIN instancia i ON i.microsservico_id = mc.id "
    f"WHERE i.endereco_mac = '{nome_arquivo}'"
)

empresa = cursor.fetchone()

empresaId, nomeEmpresa = empresa

cursor.execute(
    "SELECT i.endereco_mac FROM instancia i "
    "JOIN microsservico mc ON mc.id = i.microsservico_id "
    f"WHERE mc.empresa_id = {empresaId}"
)
macsLista = []

macs = cursor.fetchall()

for mac in macs:
    macsLista.append(mac[0])

instancias = {}       
componentesPorInstancia = {}

for mac in macsLista:
    cursor.execute(
        "SELECT c.nome FROM instancia_tem_componente ic "
        "JOIN componente c ON c.id = ic.componente_id "
        "JOIN instancia i ON i.id = ic.instancia_id "
        f"WHERE i.endereco_mac = '{mac}'"
    )

    componentesLista = []

    componentes = cursor.fetchall()

    for comp in componentes:
        componentesLista.append(comp[0])

    file_key2 = f"02-trusted-silver/{mac}.csv"

    try:
        resposta = s3_client.get_object(Bucket=bucket_name, Key=file_key2)
    except s3_client.exceptions.NoSuchKey:
        continue

    df = pd.read_csv(io.BytesIO(resposta["Body"].read()), sep=",", low_memory=False)

    manterDf = []
    for comp in componentesLista:
        if comp in df:
            manterDf.append(comp)

    valoresFinais = df[manterDf].apply(pd.to_numeric, errors="coerce").max()

    dataFinal = df.loc[0, "timestamp"]

    instanciaArquivo = {}

    for componente, valor in valoresFinais.items():
        instanciaArquivo[componente] = round(float(valor), 2)
        componentesPorInstancia.setdefault(componente, []).append(float(valor))

    instanciaArquivo["timestamp"] = dataFinal

    instancias[mac] = instanciaArquivo

mediasGerais = {}
for componente, valores in componentesPorInstancia.items():
    mediasGerais[componente] = round(sum(valores) / len(valores), 2)

jsonFinal = {
    "empresa": nomeEmpresa,
    "instancias": instancias,
    "medias": mediasGerais,
}

nomeGold = nomeEmpresa.lower().replace(" ", "_")
file_key3 = f"03-client-gold/{nomeGold}.json"

s3_client.put_object(
    Bucket=bucket_name,
    Key=file_key3,
    Body=json.dumps(jsonFinal, ensure_ascii=False, indent=4).encode("utf-8"),
    ContentType="application/json",
)

cursor.close()
conexao.close()