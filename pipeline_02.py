import os
import gdown
import duckdb
from datetime import datetime
import psycopg2


def conectar_banco():
    """Conecta ao banco de dados DuckDB; cria o banco se não existir."""
    return duckdb.connect(database='historico_arquivos.duckdb', read_only=False)

def inicializar_tabela(con):
    """Cria a tabela se ela não existir."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    """)

def registrar_arquivo(con, nome_arquivo):
    """Registra um novo arquivo no banco de dados com o horário atual."""
    con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
        VALUES (?, ?)
    """, (nome_arquivo, datetime.now()))

def arquivos_processados(con):
    """Retorna um set com os nomes de todos os arquivos já processados."""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

def baixar_pasta_google_drive(url_pasta, diretorio_local):
    """Baixa a pasta do Google Drive para o diretório local especificado."""
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    """Lista todos os arquivos CSV no diretório especificado."""
    return [os.path.join(diretorio, arquivo) for arquivo in os.listdir(diretorio) if arquivo.endswith('.csv')]

def ler_csv(caminho_do_arquivo):
    """Lê um arquivo CSV para um DataFrame do DuckDB."""
    return duckdb.read_csv(caminho_do_arquivo)

def processar_arquivos_novos(diretorio, con):
    """Processa arquivos CSV novos que não estão registrados no banco de dados."""
    inicializar_tabela(con)
    arquivos_ja_processados = arquivos_processados(con)
    arquivos_csv_atuais = listar_arquivos_csv(diretorio)
    dataframes_novos = []

    for arquivo_csv in arquivos_csv_atuais:
        nome_arquivo = os.path.basename(arquivo_csv)
        if nome_arquivo not in arquivos_ja_processados:
            df = ler_csv(arquivo_csv)
            dataframes_novos.append(df)
            registrar_arquivo(con, nome_arquivo)
            print(f"DataFrame criado para novo arquivo {nome_arquivo}:")
            # Aqui você pode adicionar lógica adicional para processar o DataFrame df, se necessário

    return dataframes_novos

def calcular_valor_total(df):
    """Calcula o valor total de cada venda e adiciona uma nova coluna 'valor_total'."""
    df['valor_total'] = df['Quantidade'] * df['Preço_Unitário']
    return df

def transformation(dataframes_novos):
    """Realiza a transformação nos dataframes novos."""
    # Realiza a transformação em cada dataframe
    for df in dataframes_novos:
        df = calcular_valor_total(df)
    return dataframes_novos