<img src="https://drive.google.com/thumbnail?id=1F9ZayTh5WS-dyqXuZ1eFlD7_pl5naQwv&sz=w1000" alt="Logo MicroHub" width="500">

# Python MicroHub

## Sobre o projeto

O Python MicroHub faz parte do projeto [MicroHub](https://github.com/microhub-project). Seus dois principais componentes são:

- Data collector
- ETL

### Data collector

O data collector (`data-collector.py`) é responsável pela coleta dos dados dos componentes das instâncias, bem como o envio do arquivo `.csv` de dados coletados para o bucket **bronze** na **AWS S3**.

### ETL

A ETL (`etl.py`) é responsável pelo tratamento e envio dos dados para os buckets **silver** e **gold**, seguindo a arquitetura de **medalhão** no **data lake**.

Processos da ETL:

1. BRONZE2SILVER:
    - Substitui os dados ausentes por 0;
    - Converte os dados coletados para unidades de medida de uso mais comum.

2. SILVER2GOLD:
    - Consulta o banco de dados e filtra as instâncias por empresa;
    - Consulta o banco de dados e filtra cada um dos componentes monitorados em cada instância;
    - Cria o arquivo de relatório em `.json` de cada empresa cadastrada no sistema, contendo as últimas leituras de cada instância e a média geral das leituras de todas as instâncias.
3. Histórico:
    - Cria o arquivo de histórico da empresa, caso ele ainda não exista;
    - Insere as últimas leituras de cada instância no arquivo histórico, além do tipo atual da instância.

## Como utilizar

Após clonar o repositório, siga os passos abaixo para utilizar o sistema:

1. Crie um **virtual environment** (`.venv`) Python na sua IDE utilizando a versão `3.14`;
2. Execute o comando abaixo para instalar todas as dependências do projeto;
    ```
    pip install -r requirements.txt
    ```
3. Utilize o arquivo `.env.example` como base para criar **um novo arquivo** `.env` e preencher as variáveis de ambiente;
4. Caso não possua o banco de dados populado, acesse o [diretório database](https://github.com/microhub-project/web-application/tree/main/src/database) do sistema web e execute os arquivos `db.sql` e `data.sql`;
5. Caso necessite de dados de teste, o diretório `/data` possui arquivos de exemplo para cada uma das camadas do data lake.
6. Ative o ambiente Python executando o comando abaixo;
    ```
    source /{CAMINHO_DO_PROJETO}/.venv/bin/activate
    ```
7. Execute o script que desejar executando o comando abaixo.
    ```
    /{CAMINHO_DO_PROJETO}/.venv/bin/python /{CAMINHO_DO_PROJETO}/{ARQUIVO}.py
    ```

## Equipe MicroHub

- [Amanda Cardoso Reys](https://github.com/amandareys)
- [Andy Lin Araujo](https://github.com/Andylinaraujo)
- [Gabriel Matias da Palma](https://github.com/GabrielMatiasDaPalma)
- [Juan Oliveira Rodrigues](https://github.com/Juan-Oliveira-Rodrigues)
- [Lucas Raphael Garcia Quinália](https://github.com/lucasquinalia)
- [Yuri Aguilera Proença](https://github.com/aguilera-yuri)