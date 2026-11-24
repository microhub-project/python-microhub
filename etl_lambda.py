#========================================================================================================================================================================
# Arquivo de como a etl deve estar dentro da lambda
#========================================================================================================================================================================

import pandas as pd
import os, io, boto3, ast, pymysql, json, base64
from io import StringIO

#========================================================================================================================================================================
#CONFIG
#========================================================================================================================================================================
s3_client = boto3.client("s3")
bucket_name = os.getenv("bucket")

conexao = None
cursor = None

def macToString(mac_address):
    return mac_address.upper().replace(':', '_')

def tupleToString(mac_address):
    mac_address = ' '.join(mac_address)
    return mac_address.upper().replace(':', '_')

def stringToMac(mac_address):
    return mac_address.upper().replace('_', ':')

# ==========================================================================================================================================================================
# HISTÓRICO
# ==========================================================================================================================================================================

def getHistoricalFile(nome_empresa):
    file_key = f"03-client-gold/{nome_empresa}_historical.json"
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        conteudo = response["Body"].read().decode("utf-8")
        return json.loads(conteudo)
    except:
        return None
    
historico_por_empresa = {}

def createHistoricalFile(nome_empresa, dados_empresa):
    historico_por_empresa = {
        "empresa": nome_empresa,
        "instancias": []
    }
    
    for instancia in dados_empresa["instancias"]:
        cursor.execute(
            "SELECT t.nome, t.cpu, t.ram, t.custo_computacao "
            "FROM tipo t JOIN instancia i ON t.uuid = i.tipo_uuid "
            "WHERE i.endereco_mac = %s;",
            (instancia["mac_address"],)
        )
        tipo = cursor.fetchone()
        
        nome, cpu, ram, custo_computacao = tipo
        
        leitura = instancia.copy()
        mac_address = leitura.pop("mac_address")
        
        leitura["tipo"] = {
            "nome": str(nome),
            "cpu": int(cpu),
            "ram": float(ram),
            "custo_computacao": float(custo_computacao)
        }
        
        nova_instancia = {
            "mac_address": mac_address,
            "leituras": [leitura]
        }
        
        historico_por_empresa["instancias"].append(nova_instancia)
        
    nome_arquivo = nome_empresa.lower().replace(" ", "_")
        
    file_key = f"03-client-gold/{nome_arquivo}_historical.json"
        
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(historico_por_empresa, ensure_ascii=False, indent=4).encode("utf-8"),
        ContentType="application/json",
    )
    
def addToHistoricalFile(nome_empresa, dados_empresa, arquivo):
    file_key = f"03-client-gold/{nome_empresa}_historical.json"
    
    instancias_existentes = {
        item["mac_address"]: item for item in arquivo["instancias"]
    }
    
    for instancia in dados_empresa["instancias"]:
        cursor.execute(
            "SELECT t.nome, t.cpu, t.ram, t.custo_computacao "
            "FROM tipo t JOIN instancia i ON t.uuid = i.tipo_uuid "
            "WHERE i.endereco_mac = %s;",
            (instancia["mac_address"],)
        )
        tipo = cursor.fetchone()
        
        nome, cpu, ram, custo_computacao = tipo
        
        leitura = instancia.copy()
        mac_address = leitura.pop("mac_address")
        
        leitura["tipo"] = {
            "nome": str(nome),
            "cpu": int(cpu),
            "ram": float(ram),
            "custo_computacao": float(custo_computacao)
        }
        
        if mac_address in instancias_existentes:
            instancias_existentes[mac_address]["leituras"].append(leitura)
        else:
            arquivo["instancias"].append({
                "mac_address": mac_address,
                "leituras": [leitura]
            })
        
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(arquivo, ensure_ascii=False, indent=4).encode("utf-8"),
        ContentType="application/json",
    )

#==========================================================================================================================================================================
#BRONZE2SILVER
#==========================================================================================================================================================================

def b2s(mac_address):
    file_key1 = f"01-raw-bronze/{mac_address}.csv"
    
    try:
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
        
        nome_arquivo = macToString(nome_arquivo)

        csv_buffer = StringIO()
        df_collect.to_csv(csv_buffer, index=False)

        file_name = f"02-trusted-silver/{nome_arquivo}.csv"

        s3_client.put_object(
            Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue()
        )
    except:
        print("Não foram encontradas leituras da instância de endereço MAC ", mac_address)

#==========================================================================================================================================================================
#SILVER2GOLD
#==========================================================================================================================================================================

dados_por_empresa = {}

def s2g(mac_address):
    cursor.execute(
        "SELECT BIN_TO_UUID(e.uuid), e.nome_fantasia "
        "FROM empresa e "
        "JOIN microsservico mc ON mc.empresa_uuid = e.uuid "
        "JOIN instancia i ON i.microsservico_uuid = mc.uuid "
        f"WHERE i.endereco_mac = '{mac_address}';"
    )

    empresa = cursor.fetchone()

    empresaId, nomeEmpresa = empresa
    
    if nomeEmpresa not in dados_por_empresa:
        dados_por_empresa[nomeEmpresa] = {
            "instancias": [],
            "componentesPorInstancia": {},
        }

    cursor.execute(
        "SELECT c.nome_tecnico "
        "FROM instancia_tem_componente ic "
        "JOIN componente c ON c.uuid = ic.componente_uuid "
        "JOIN instancia i ON i.uuid = ic.instancia_uuid "
        f"WHERE i.endereco_mac = '{mac_address}'"
    )

    componentesLista = []

    componentes = cursor.fetchall()

    for componente in componentes:
        componentesLista.append(componente[0])

    nomeArquivo = macToString(mac_address)
    file_key2 = f"02-trusted-silver/{nomeArquivo}.csv"
    
    try:
        resposta = s3_client.get_object(Bucket=bucket_name, Key=file_key2)
    except Exception as e:
        print(f"Arquivo não encontrado para a instância {mac_address} no bucket silver: {e}")
        return

    df = pd.read_csv(io.BytesIO(resposta["Body"].read()), sep=",", low_memory=False)

    manterDf = []
    for componente in componentesLista:
        if componente in df:
            manterDf.append(componente)

    valoresFinais = df[manterDf].apply(pd.to_numeric, errors="coerce").max()

    dataFinal = df.loc[0, "timestamp"]

    instanciaArquivo = {"mac_address": mac_address}
    comp_empresa = dados_por_empresa[nomeEmpresa]["componentesPorInstancia"]

    for componente, valor in valoresFinais.items():
        val_float = round(float(valor), 2)
        instanciaArquivo[componente] = val_float
        comp_empresa.setdefault(componente, []).append(val_float)

    instanciaArquivo["timestamp"] = dataFinal

    dados_por_empresa[nomeEmpresa]["instancias"].append(instanciaArquivo)
    
def uploadToGold():
    for nomeEmpresa, dados in dados_por_empresa.items():
        mediasGerais = {}
        for componente, valores in dados["componentesPorInstancia"].items():
            mediasGerais[componente] = round(sum(valores) / len(valores), 2)

        jsonFinal = {
            "empresa": nomeEmpresa,
            "instancias": dados["instancias"],
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
        
        response = getHistoricalFile(nomeGold)
        
        if response:
            addToHistoricalFile(nomeGold, dados, response)
        else:
            createHistoricalFile(nomeEmpresa, dados)
    
# ==========================================================================================================================================================================
# EXECUÇÃO
# ==========================================================================================================================================================================

def run_etl():
    global conexao, cursor

    # A Lambda pode reaproveitar o container entre execuções, então zera o acumulado
    dados_por_empresa.clear()

    conexao = pymysql.connect(
        host=os.getenv("host"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        database=os.getenv("database"),
        connect_timeout=10,
    )
    cursor = conexao.cursor()

    try:
        cursor.execute(
            f"SELECT endereco_mac FROM instancia i;"
        )

        macs = cursor.fetchall()

        for mac in macs:
            mac = tupleToString(mac)
            b2s(mac)
            
            mac = stringToMac(mac)
            s2g(mac)
            
        uploadToGold()
    finally:
        cursor.close()
        conexao.close()

#==========================================================================================================================================================================
# HANDLER DA LAMBDA
#==========================================================================================================================================================================

def lambda_handler(event, context):
    try:
        faltando = [v for v in ("bucket", "host", "user", "password", "database") if not os.getenv(v)]
        if faltando:
            raise Exception(f"Variáveis de ambiente não configuradas na Lambda: {', '.join(faltando)}")

        query_params = event.get('queryStringParameters') or {}
        file_name = os.path.basename(query_params.get('filename', 'arquivo_padrao.csv'))

        body = event.get('body', '') or ''
        if event.get('isBase64Encoded', False):
            file_content = base64.b64decode(body)
        else:
            file_content = body.encode('utf-8')

        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"01-raw-bronze/{file_name}",
            Body=file_content,
            ContentType='text/csv'
        )

        run_etl()

        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Arquivo {file_name} recebido e ETL executada com sucesso!'})
        }

    except Exception as e:
        print("Erro:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }