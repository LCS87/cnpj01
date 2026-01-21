import numpy as np
import re
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import mysql.connector
from tqdm import tqdm
import pandas as pd
import logging
import zipfile
import csv
import time
from datetime import datetime
import shutil
import concurrent.futures
import random
from concurrent.futures import ThreadPoolExecutor
import json
from io import StringIO



anoMes = '2025-08'
nomeTabela = f'empresas_{anoMes.replace("-", "")}_ATLZ'
DBsenha = 'kkkkkkkkk'
BDdataBase = 'cnpj_brasil'


base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
download_dir = "arquivo receita"


os.makedirs(download_dir, exist_ok=True)


BASE_DIR = "dividir"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": f"{DBsenha}",
    "database": f"{BDdataBase}"
}
CHUNKSIZE = 100_000
BATCH_SIZE = 10_000 


FOLDER_TO_TABLE = {
    "10 PARTES EMPRESAS": "empresas",
    "10 PARTES ESTABELECIMENTOS": "estabelecimentos",
    "10 PARTES SOCIOS": "socios"
}

def get_latest_release_url(base_url):
    """Encontra a URL do release mais recente ou específico conforme anoMes"""
    try:
     
        if anoMes:
            data_atual = datetime.now().strftime("%Y%m")
            if anoMes > data_atual:
                raise ValueError(f"Data {anoMes} é futura. Não é possível baixar dados de meses que ainda não existem.")
        
        response = requests.get(base_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.find_all('a') if a['href'].startswith('20') and a['href'].endswith('/')]
        
        if not links:
            raise ValueError("Nenhum link de release encontrado no site da Receita Federal")
            
    
        if anoMes:
            target_link = f"{anoMes}/"
            matching_links = [link for link in links if link == target_link]
            
            if not matching_links:
                available_dates = ", ".join([link.replace('/', '') for link in links[:5]]) 
                raise ValueError(f"Nenhum link encontrado para {anoMes}. Datas disponíveis: {available_dates}{'...' if len(links) > 5 else ''}")
                
            return urljoin(base_url + '/', matching_links[0])
        else:
     
            links.sort(reverse=True)
            latest_release = links[0]
            return urljoin(base_url + '/', latest_release)
        
    except Exception as e:
        print(f"\n❌ Erro ao encontrar o release: {e}")
        print("Verifique se:")
        print("- A data está no formato YYYY-MM (ex: '2024-05')")
        print("- A data não é futura")
        print("- O site da Receita Federal está acessível")
        return None

def get_file_links(release_url):
    """Obtém todos os links de arquivos ZIP no release"""
    try:
        response = requests.get(release_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        file_links = []
        
        for a in soup.find_all('a'):
            href = a['href']
            if href.endswith('.zip'):
                file_links.append(urljoin(release_url, href))
                
        return file_links
        
    except Exception as e:
        print(f"Erro ao obter links dos arquivos: {e}")
        return []

def download_file(url, dest_dir):
    """Faz o download de um arquivo e salva no diretório de destino"""
    try:
        filename = os.path.basename(url)
        dest_path = os.path.join(dest_dir, filename)
        
     
        if os.path.exists(dest_path):
            print(f"Arquivo {filename} já existe, pulando...")
            return True
            
        print(f"Baixando {filename}...")
        
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        print(f"{filename} baixado com sucesso!")
        return True
        
    except Exception as e:
        print(f"Erro ao baixar {url}: {e}")
        return False
    
def mainPegarDados():
    """Faz download apenas dos arquivos essenciais (Empresas, Estabelecimentos e Sócios)"""
    print("Iniciando processo de download dos arquivos essenciais...")
    
   
    latest_release = get_latest_release_url(base_url)
    if not latest_release:
        print("Não foi possível encontrar o release mais recente.")
        return
        
    print(f"Release mais recente encontrado: {latest_release}")
    
 
    file_links = get_file_links(latest_release)
    if not file_links:
        print("Nenhum arquivo encontrado para download.")
        return
        
  
    arquivos_essenciais = {
        'Empresas': [],
        'Estabelecimentos': [],
        'Socios': []
    }
    
    for file_url in file_links:
        filename = os.path.basename(file_url)
        if filename.startswith('Empresas') and filename.endswith('.zip'):
            arquivos_essenciais['Empresas'].append(file_url)
        elif filename.startswith('Estabelecimentos') and filename.endswith('.zip'):
            arquivos_essenciais['Estabelecimentos'].append(file_url)
        elif filename.startswith('Socios') and filename.endswith('.zip'):
            arquivos_essenciais['Socios'].append(file_url)
    

    for tipo, arquivos in arquivos_essenciais.items():
        if not arquivos:
            print(f"⚠️ Nenhum arquivo {tipo} encontrado para download!")
            return
    
    print("\n📁 Arquivos essenciais encontrados:")
    for tipo, arquivos in arquivos_essenciais.items():
        print(f" - {tipo}: {len(arquivos)} arquivos")
    

    os.makedirs(download_dir, exist_ok=True)
    
    
    total_arquivos = sum(len(arquivos) for arquivos in arquivos_essenciais.values())
    success_count = 0
    
    print("\n⏳ Iniciando downloads...")
    
    for tipo, arquivos in arquivos_essenciais.items():
        print(f"\n🔽 Baixando arquivos de {tipo}:")
        for file_url in arquivos:
            filename = os.path.basename(file_url)
            print(f" - {filename}...", end=' ', flush=True)
            
            if download_file(file_url, download_dir):
                success_count += 1
                print("✅")
            else:
                print("❌ (Falha)")
    
  
    print(f"\n📊 Resultado:")
    print(f" - Total de arquivos essenciais: {total_arquivos}")
    print(f" - Baixados com sucesso: {success_count}")
    print(f" - Falhas: {total_arquivos - success_count}")
    
    if success_count == total_arquivos:
        print("\n🎉 Todos os arquivos essenciais foram baixados com sucesso!")
    else:
        print("\n⚠️ Alguns arquivos não foram baixados. Verifique os logs acima.")
    
    print(f"\nArquivos salvos em: {download_dir}") 

def dezipar():
    """Função para descompactar e processar os arquivos baixados da Receita Federal"""
    ########################## Configurações Padrão ##########################
    DEFAULT_CONFIG = {
        'base_url': 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/',
        'csv_sep': ';',
        'csv_dec': ',',
        'csv_quote': '"',
        'csv_enc': 'latin1',
        'export_format': 'csv',
        'dtypes': {
            'empresas': {
                'cnpj_basico': "str",
                'razao_social': "str",
                'natureza_juridica': "str",
                'qualificacao_do_responsavel': "str",
                'capital_social': "str",
                'porte_da_empresa': "str",
                'ente_federativo_resposavel': "str"
            },
            'estabelecimentos': {
                'cnpj_basico': "str",
                'cnpj_ordem': "str",
                'cnpj_dv': "str",
                'identificador_matriz_filial': "str",
                'nome_fantasia': "str",
                'situacao_cadastral': "str",
                'data_situacao_cadastral': "str",
                'motivo_situacao_cadastral': "str",
                'nome_da_cidade_no_exterior': "str",
                'pais': "str",
                'data_de_inicio_da_atividade': "str",
                'cnae_fiscal_principal': "str",
                'cnae_fiscal_secundaria': "str",
                'tipo_de_logradouro': "str",
                'logradouro': "str",
                'numero': "str",
                'complemento': "str",
                'bairro': "str",
                'cep': "str",
                'uf': "str",
                'municipio': "str",
                'ddd1': "str",
                'telefone1': "str",
                'ddd2': "str",
                'telefone2': "str",
                'ddd_do_fax': "str",
                'fax': "str",
                'correio_eletronico': "str",
                'situacao_especial': "str",
                'data_da_situacao_especial': "str"
            },
            'socios': {
                'cnpj_basico': "str",
                'identificador_de_socio': "str",
                'nome_do_socio': "str",
                'cnpj_ou_cpf_do_socio': "str",
                'qualificacao_do_socio': "str",
                'data_de_entrada_sociedade': "str",
                'pais': "str",
                'representante_legal': "str",
                'nome_do_representante': "str",
                'qualificacao_do_representante_legal': "str",
                'faixa_etaria': "str"
            }
        }
    }

    ########################## Inicialização ##########################
    data_incoming_foldername = "arquivo receita"
    data_outgoing_foldername = "arquivo receita deszipados"
    log_foldername = 'logs'
    log_filename = 'cnpj_merger.log'
    
    path_script_dir = os.path.dirname(os.path.abspath(__file__))
    path_incoming = os.path.join(path_script_dir, data_incoming_foldername)
    path_outgoing = os.path.join(path_script_dir, data_outgoing_foldername)
    path_log = os.path.join(path_script_dir, log_foldername, log_filename)
    
    os.makedirs(path_outgoing, exist_ok=True)
    os.makedirs(os.path.dirname(path_log), exist_ok=True)

    logging.basicConfig(filename=path_log, level=logging.INFO,
                       format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    logging.info('Starting script')
    print('Starting script')

    config = DEFAULT_CONFIG
    csv_sep = config['csv_sep']
    csv_dec = config['csv_dec']
    csv_quote = config['csv_quote']
    csv_enc = config['csv_enc']
    export_format = config['export_format']
    dtypes = config['dtypes']

    ########################## Funções Internas ##########################
    def process_estabelecimentos(file_params):
        """Processamento especial para arquivos de estabelecimentos (grandes)"""
        expected_columns = list(dtypes['estabelecimentos'].keys())
        output_file = os.path.join(path_outgoing, 'estabelecimentos.csv')
        
       
        if os.path.exists(output_file):
            os.remove(output_file)
        
      
        with open(output_file, 'w', encoding='utf-8') as f_out:
          
            f_out.write(csv_sep.join(expected_columns) + '\n')
            
          
            for zip_file_path, zip_filename, _ in file_params:
                print(f'Processing: {zip_filename}')
                
                try:
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                     
                        zip_file_list = zip_ref.namelist()
                        if len(zip_file_list) != 1:
                            print(f'Warning: ZIP file {zip_filename} contains {len(zip_file_list)} files. Using first one.')
                        
                   
                        with zip_ref.open(zip_file_list[0]) as csvfile:
                        
                            for line_bytes in csvfile:
                                try:
                                    
                                    line = line_bytes.decode(csv_enc).strip()
                                    
                                   
                                    parts = line.split(csv_sep)
                                    
                                   
                                    if len(parts) > len(expected_columns):
                                        parts = parts[:len(expected_columns)]
                                    elif len(parts) < len(expected_columns):
                                        parts.extend([''] * (len(expected_columns) - len(parts)))
                                    
                               
                                    f_out.write(csv_sep.join(parts) + '\n')
                                except Exception as e:
                                    print(f"Error processing line: {e}")
                                    continue
                except Exception as e:
                    print(f"Error processing {zip_filename}: {e}")
                    continue
        
        print(f"✅ Arquivo de estabelecimentos gerado: {output_file}")
        return True

    def process_normal_files(file_params, prefix):
        """Processamento normal para arquivos menores (empresas e sócios)"""
        expected_columns = list(dtypes[prefix].keys())
        output_file = os.path.join(path_outgoing, f"{prefix}.{export_format}")
        dfs = []
      
        for zip_file_path, zip_filename, _ in file_params:
            print(f'Processing: {zip_filename}')
            
            try:
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    
                    zip_file_list = zip_ref.namelist()
                    if len(zip_file_list) != 1:
                        print(f'Warning: ZIP file {zip_filename} contains {len(zip_file_list)} files. Using first one.')
                    
                    
                    with zip_ref.open(zip_file_list[0]) as csvfile:
                       
                        df = pd.read_csv(
                            csvfile,
                            sep=csv_sep,
                            decimal=csv_dec,
                            quotechar=csv_quote,
                            dtype=str,
                            encoding=csv_enc,
                            header=None,
                            names=expected_columns
                        )
                        dfs.append(df)
            except Exception as e:
                print(f"Error processing {zip_filename}: {e}")
                continue
        
    
        if dfs:
            try:
                df_merged = pd.concat(dfs, ignore_index=True)
                
            
                if export_format == 'csv':
                    df_merged.to_csv(output_file, index=False, sep=csv_sep, encoding='utf-8')
                else:
                    raise ValueError("Formato de exportação não suportado")
                
                print(f"✅ Arquivo {prefix} gerado: {output_file}")
                print(f"📊 Total de registros: {len(df_merged)}")
                return True
            except Exception as e:
                print(f"Error merging dataframes for {prefix}: {e}")
                return False
        else:
            print(f"⚠️ Nenhum dado válido encontrado para {prefix}")
            return False

    ########################## Processamento Principal ##########################
    try:
        
        file_params = {prefix: [] for prefix in dtypes.keys()}
        
        for root, _, files in os.walk(path_incoming):
            for filename in files:
                if filename.endswith(".zip"):
                    zip_file_path = os.path.join(root, filename)
                    file_with_no_ext = filename.split('.')[0].lower()
                    
                    for prefix in dtypes.keys():
                        if file_with_no_ext.startswith(prefix):
                            file_params[prefix].append((zip_file_path, filename, file_with_no_ext))

       
        for prefix, params in file_params.items():
            if params:  
                print(f"\nProcessando arquivos de {prefix}...")
                
                if prefix == 'estabelecimentos':
                   
                    success = process_estabelecimentos(params)
                else:
                   
                    success = process_normal_files(params, prefix)
                
                if not success:
                    print(f"⚠️ Falha ao processar arquivos de {prefix}")

        print("\n✅ Processo de descompactação concluído com sucesso!")
        return True
        
    except Exception as e:
        logging.error(f"Erro fatal no processo de descompactação: {e}")
        print(f"❌ Erro fatal: {e}")
        return False

def dividir_csv(arquivo_csv, partes=10):
    
    pastas = {
        'EMPRESA': '10 PARTES EMPRESAS',
        'EMPRESAS': '10 PARTES EMPRESAS',
        'ESTABELECIMENTO': '10 PARTES ESTABELECIMENTOS',
        'ESTABELECIMENTOS': '10 PARTES ESTABELECIMENTOS',
        'SOCIO': '10 PARTES SOCIOS',
        'SOCIOS': '10 PARTES SOCIOS'
    }

    if not os.path.exists(arquivo_csv):
        print(f"⚠ Arquivo NÃO encontrado: {arquivo_csv}")
        return


    nome_arquivo = os.path.basename(arquivo_csv).upper()
    pasta_destino = None
    for chave, pasta in pastas.items():
        if chave in nome_arquivo:
            pasta_destino = pasta
            break

    if not pasta_destino:
        print(f"⚠ Nenhuma pasta correspondente encontrada para: {nome_arquivo}")
        return

 
    pasta_base_saida = 'dividir'
    caminho_pasta = os.path.join(pasta_base_saida, pasta_destino)

    os.makedirs(caminho_pasta, exist_ok=True)

   
    with open(arquivo_csv, 'r', encoding='latin1') as f:
        total_linhas = sum(1 for _ in f)

    print(f"\n➡ Dividindo '{nome_arquivo}' ({total_linhas} linhas) para a pasta '{pasta_destino}'...")

    linhas_por_parte = total_linhas // partes + 1

    with open(arquivo_csv, 'r', encoding='latin1') as f:
        leitor = csv.reader(f, delimiter=';')
        cabecalho = next(leitor)

        for i in range(partes):
            numero_arquivo = i + 1
            nome_saida = f"parte_{numero_arquivo}.csv"
            caminho_saida = os.path.join(caminho_pasta, nome_saida)

            while os.path.exists(caminho_saida):
                numero_arquivo += 1
                nome_saida = f"parte_{numero_arquivo}.csv"
                caminho_saida = os.path.join(caminho_pasta, nome_saida)

            with open(caminho_saida, 'w', newline='', encoding='latin1') as out:
                escritor = csv.writer(out, delimiter=';')
                escritor.writerow(cabecalho)

                linhas_escritas = 0
                while linhas_escritas < linhas_por_parte:
                    try:
                        linha = next(leitor)
                        escritor.writerow(linha)
                        linhas_escritas += 1
                    except StopIteration:
                        break

            print(f"✅ Criado: {caminho_saida} ({linhas_escritas} linhas)")

    print(f"✔ Arquivo '{nome_arquivo}' concluído!\n")

def processar_arquivos_deszipados():
    """Processa todos os arquivos CSV descompactados"""
    pasta_base = r"arquivo receita deszipados"

    arquivos = [f for f in os.listdir(pasta_base) if f.lower().endswith('.csv')]

    if not arquivos:
        print(f"⚠ Nenhum arquivo CSV encontrado em {pasta_base}")
    else:
        print(f"📂 Encontrados {len(arquivos)} arquivo(s) na pasta '{pasta_base}':")
        for nome in arquivos:
            print(" -", nome)

        for arquivo in arquivos:
            arquivo_completo = os.path.join(pasta_base, arquivo)
            dividir_csv(arquivo_completo)

        print("🎉 Todos os arquivos foram processados!")

def get_db_connection():
    """Estabelece conexão com o banco de dados"""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as err:
        print(f"🚨 Erro ao conectar ao MySQL: {err}")
        return None

def get_table_columns(conn, table_name):
    """Obtém as colunas de uma tabela"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_name}")
            return [col[0] for col in cursor.fetchall()]
    except Exception as e:
        print(f"🚨 Erro ao obter colunas da tabela '{table_name}': {e}")
        return None

def process_folder(folder_path, table_name, conn):
    """Processa todos os arquivos CSV em uma pasta e importa para a tabela correspondente"""
    if not os.path.exists(folder_path):
        print(f"📌 Pasta não encontrada: {folder_path}")
        return 0
    
    file_paths = sorted([
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.startswith("parte_") and f.endswith(".csv")
    ])
    
    if not file_paths:
        print(f"📌 Nenhum arquivo CSV encontrado na pasta: {folder_path}")
        return 0
    
    colunas = get_table_columns(conn, table_name)
    if not colunas:
        return 0
    
    total_imported = 0
    cursor = conn.cursor()
    
    print(f"\n📂 Processando tabela: {table_name.upper()}")
    
    for file_path in tqdm(file_paths, desc="Arquivos", unit="file"):
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Tamanho em MB
            print(f"\n📄 Arquivo: {os.path.basename(file_path)} ({file_size:.2f} MB)")
            
            for chunk in pd.read_csv(
                file_path,
                encoding='utf-8',
                delimiter=';',
                header=None,
                names=colunas,
                low_memory=False,
                skiprows=1,
                chunksize=CHUNKSIZE,
                on_bad_lines='warn'
            ):
             
                chunk = chunk.where(pd.notnull(chunk), None)
                records = [tuple(None if pd.isna(value) else value for value in row) 
                        for _, row in chunk.iterrows()]
                
            
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i+BATCH_SIZE]
                    colunas_str = ', '.join([f'`{col}`' for col in colunas])
                    valores = ', '.join(['%s'] * len(colunas))
                    query = f"INSERT IGNORE INTO {table_name} ({colunas_str}) VALUES ({valores})"
                    
                    try:
                        cursor.executemany(query, batch)
                        conn.commit()
                        total_imported += len(batch)
                    except mysql.connector.Error as err:
                        print(f"⚠️ Erro ao inserir lote: {err}")
                        conn.rollback()
                
                print(f"   ✅ {len(records):,} registros processados | Total: {total_imported:,}")
                
        except Exception as e:
            print(f"🚨 Erro ao processar o arquivo {file_path}: {e}")
            continue
    
    cursor.close()
    return total_imported

# def mainJogarNoBanco():
#     start_time = time.time()
#     conn = get_db_connection()
#     if not conn:
#         return
    
#     total_global = 0
    
#     try:
#         for folder, table in FOLDER_TO_TABLE.items():
#             # Corrigindo o caminho da pasta
#             folder_path = os.path.join(BASE_DIR, folder)
#             if not os.path.exists(folder_path):
#                 print(f"⚠️ Pasta não encontrada: {folder_path}")
#                 continue
                
#             imported = process_folder(folder_path, table, conn)
#             total_global += imported
#             print(f"\n✔️ {table.upper()}: {imported:,} registros importados\n")
            
#     finally:
#         conn.close()
#         elapsed = time.time() - start_time
#         print(f"\n🎉 Importação concluída! Total: {total_global:,} registros")
#         print(f"⏱️ Tempo total: {elapsed:.2f} segundos")
        
def recreate_database_tables():
    """Reinicia o banco de dados, dropando e recriando todas as tabelas"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n🔧 Recriando estrutura do banco de dados...")
        
        
        tables_to_drop = [
            'socios', 'estabelecimentos', 'empresas'
        ]
      
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"✅ Tabela {table} dropada com sucesso")
            except mysql.connector.Error as err:
                print(f"⚠️ Erro ao dropar tabela {table}: {err}")
        
     
        cursor.execute("CREATE DATABASE IF NOT EXISTS cnpj_brasil")
        cursor.execute("USE cnpj_brasil")
        
    
        cursor.execute("""
        CREATE TABLE empresas (
            cnpj_basico VARCHAR(8),
            razao_social VARCHAR(150),
            natureza_juridica INT,
            qualificacao_do_responsavel INT,
            capital_social DECIMAL(20,2),
            porte_da_empresa INT,
            ente_federativo_responsavel VARCHAR(100)
        )""") 
        
        cursor.execute("""
        CREATE TABLE estabelecimentos (
            cnpj_basico VARCHAR(8),
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            identificador_matriz_filial INT,
            nome_fantasia VARCHAR(55),
            situacao_cadastral INT,
            data_situacao_cadastral DATE,
            motivo_situacao_cadastral INT,
            nome_da_cidade_no_exterior VARCHAR(55),
            pais VARCHAR(3),
            data_de_inicio_da_atividade DATE,
            cnae_fiscal_principal VARCHAR(7),
            cnae_fiscal_secundaria TEXT,
            tipo_de_logradouro VARCHAR(20),
            logradouro VARCHAR(60),
            numero VARCHAR(6),
            complemento VARCHAR(156),
            bairro VARCHAR(50),
            cep VARCHAR(8),
            uf VARCHAR(2),
            municipio INT,
            ddd1 VARCHAR(4),
            telefone1 VARCHAR(9),
            ddd2 VARCHAR(4),
            telefone2 VARCHAR(9),
            ddd_do_fax VARCHAR(4),
            fax VARCHAR(9),
            correio_eletronico VARCHAR(115),
            situacao_especial VARCHAR(23),
            data_da_situacao_especial DATE
        )""")
        
        cursor.execute("""
        CREATE TABLE socios (
            cnpj_basico VARCHAR(8),
            identificador_de_socio INT,
            nome_do_socio VARCHAR(150),
            cnpj_ou_cpf_do_socio VARCHAR(14),
            qualificacao_do_socio INT,
            data_de_entrada_sociedade DATE,
            pais VARCHAR(3),
            representante_legal VARCHAR(11),
            nome_do_representante VARCHAR(60),
            qualificacao_do_representante_legal INT,
            faixa_etaria INT
        )""")


        
        conn.commit()
        print("✅ Todas as tabelas foram recriadas com sucesso!")
        return True
        
    except mysql.connector.Error as err:
        print(f"🚨 Erro ao recriar tabelas: {err}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def mainJogarNoBanco():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return
    
    total_global = 0
    
    try:
        for folder, table in FOLDER_TO_TABLE.items():
         
            folder_path = os.path.join(BASE_DIR, folder)
            
          
            if not os.path.exists(folder_path):
                print(f"🚨 ERRO: Pasta não encontrada: {folder_path}")
                print("Verifique se a estrutura de pastas está correta:")
                print(f"Deve existir: {BASE_DIR}/{folder}/ com arquivos parte_1.csv a parte_10.csv")
                continue
         
            csv_files = [f for f in os.listdir(folder_path) if f.startswith('parte_') and f.endswith('.csv')]
            if not csv_files:
                print(f"🚨 ERRO: Nenhum arquivo CSV encontrado em {folder_path}")
                continue
                
            print(f"\n📂 Processando pasta: {folder_path}")
            print(f"📄 Arquivos encontrados: {len(csv_files)}")
            
            imported = process_folder(folder_path, table, conn)
            total_global += imported
            print(f"✔️ {table.upper()}: {imported:,} registros importados")
            
    except Exception as e:
        print(f"🚨 Erro durante a importação: {e}")
    finally:
        conn.close()
        elapsed = time.time() - start_time
        print(f"\n🎉 Importação concluída! Total: {total_global:,} registros")
        print(f"⏱️ Tempo total: {elapsed:.2f} segundos")

def create_and_export_empresas():
    """Cria tabela simplificada apenas com os campos essenciais"""
    conn = get_db_connection()
    if not conn:
        return False

    cursor = conn.cursor()

    try:
        print(f"\n🔧 Criando tabela simplificada {nomeTabela} com dados essenciais...")

       
        cursor.execute("SET SESSION innodb_lock_wait_timeout=120")
        cursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")

     
        cursor.execute(f"DROP TABLE IF EXISTS {nomeTabela}")
        
        create_structure_query = f"""
        CREATE TABLE {nomeTabela} (
            cnpj_completo VARCHAR(14),
            razao_social VARCHAR(150),
            endereco_completo TEXT,
            email VARCHAR(115),
            ano_abertura INT,
            cnae_principal VARCHAR(100),
            natureza_juridica VARCHAR(100),
            telefones TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        cursor.execute(create_structure_query)
        
       
        insert_query = f"""
        INSERT INTO {nomeTabela}
        SELECT
            CONCAT(
                e.cnpj_basico,
                LPAD(est.cnpj_ordem, 4, '0'),
                LPAD(est.cnpj_dv, 2, '0')
            ) AS cnpj_completo,

            e.razao_social,

            CONCAT(
                est.logradouro, ', ',
                est.numero,
                IFNULL(CONCAT(' ', est.complemento), ''),
                IFNULL(CONCAT(' - ', est.bairro), ''),
                IFNULL(CONCAT(' - ', est.uf), '')
            ) AS endereco_completo,

            est.correio_eletronico AS email,

            YEAR(est.data_de_inicio_da_atividade) AS ano_abertura,

            est.cnae_fiscal_principal AS cnae_principal,

            e.natureza_juridica,

            CONCAT(
                IFNULL(CONCAT(est.ddd1, est.telefone1), ''),
                IFNULL(CONCAT(' / ', est.ddd2, est.telefone2), '')
            ) AS telefones
            CONCAT(
                IFNULL(CONCAT(est.tipo_de_logradouro, ' '), ''),
                IFNULL(est.logradouro, ''),
                IFNULL(CONCAT(', ', est.numero), ''),
                IFNULL(CONCAT(' - ', est.complemento), ''),
                IFNULL(CONCAT(' - ', est.bairro), ''),
                IFNULL(CONCAT(' - ', est.uf), ''),
                IFNULL(CONCAT(' - CEP: ', est.cep), '')
            ) AS endereco,
            e.capital_social,
            CASE 
                WHEN e.porte_da_empresa = '1' THEN 'Micro Empresa'
                WHEN e.porte_da_empresa = '3' THEN 'Empresa de Pequeno Porte'
                WHEN e.porte_da_empresa = '5' THEN 'Demais'
                ELSE 'Não Informado'
            END AS porte_da_empresa,
            CASE
                WHEN e.natureza_juridica = '2135' THEN 'MEI'  -- Microempreendedor Individual
                WHEN e.porte_da_empresa = '1' AND e.razao_social LIKE '%LTDA%' THEN 'LTDA'
                WHEN e.porte_da_empresa = '1' THEN 'ME'  -- Microempresa
                WHEN e.porte_da_empresa = '3' AND e.razao_social LIKE '%LTDA%' THEN 'LTDA'
                WHEN e.porte_da_empresa = '3' THEN 'EPP'  -- Empresa de Pequeno Porte
                WHEN e.razao_social LIKE '%LTDA%' THEN 'LTDA'
                ELSE 'Outros'
            END AS tipo_empresa,
            est.nome_fantasia,
            est.data_situacao_cadastral,
            est.data_de_inicio_da_atividade,
            est.tipo_de_logradouro,
            est.correio_eletronico
        FROM empresas e
        INNER JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
        WHERE est.situacao_cadastral = 2
        AND est.uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE')
        AND est.data_de_inicio_da_atividade BETWEEN '1955-01-01' AND '2025-12-31'
        LIMIT 1000000
        """
       
        '''FROM empresas e
        INNER JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
        LEFT JOIN municipios mun ON est.municipio = mun.codigo_municipio
        WHERE est.situacao_cadastral = 2
        AND est.uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE')
        AND est.data_de_inicio_da_atividade BETWEEN '1955-01-01' AND '2025-12-31'
        LIMIT 1000000'''
        offset = 0
        batch_size = 500000
        total_rows = 0
        cursor.execute(insert_query) 
        conn.commit()

        # while True:
        #     batch_query = f"{insert_query} OFFSET {offset}"
        #     cursor.execute(batch_query)
        #     conn.commit()
            
        #     row_count = cursor.rowcount
        #     total_rows += row_count
        #     print(f"  ✅ Inseridos {row_count:,} registros | Total: {total_rows:,}")
            
        #     if row_count < batch_size:
        #         break
                
        #     offset += batch_size
        #     time.sleep(1)  # Pequena pausa entre lotes

        print(f"✅ Tabela {nomeTabela} populada com sucesso! Total: {total_rows:,} registros")

       
        cursor.execute("SET SESSION sql_mode=(SELECT CONCAT(@@sql_mode,',ONLY_FULL_GROUP_BY'))")

 
        print("\n📤 Exportando dados para CSV...")
        export_dir = "export"
        os.makedirs(export_dir, exist_ok=True)
        csv_path = os.path.join(export_dir, f"{nomeTabela}.csv")

        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            f.write("CNPJ;RAZAO_SOCIAL;ENDERECO;EMAIL;ANO_ABERTURA;CNAE_PRINCIPAL;NATUREZA_JURIDICA;TELEFONES\n")

            cursor.execute(f"SELECT COUNT(*) FROM {nomeTabela}")
            total_export = cursor.fetchone()[0]
            print(f"  📊 Total a exportar: {total_export:,} registros")

            export_batch = 50000
            for offset in range(0, total_export, export_batch):
                cursor.execute(f"""
                    SELECT 
                        cnpj_completo,
                        razao_social,
                        endereco_completo,
                        email,
                        ano_abertura,
                        cnae_principal,
                        natureza_juridica,
                        telefones
                    FROM {nomeTabela}
                    LIMIT {export_batch} OFFSET {offset}
                """)
                
                for row in cursor:
                    formatted_row = [
                        str(col) if col is not None else '' 
                        for col in row
                    ]
                    line = ';'.join(formatted_row)
                    f.write(line + '\n')
                
                print(f"  ✅ Exportados {min(offset+export_batch, total_export):,}/{total_export:,} registros")

        print(f"\n🎉 Exportação concluída! Total: {total_export:,} registros")
        print(f"📁 Arquivo salvo em: {csv_path}")

   
        print("\n📤 Exportando dados para XLSX (em partes)...")
        xlsx_path = os.path.join(export_dir, f"{nomeTabela}.xlsx")
        
        with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
            for chunk in pd.read_sql_query(f"SELECT * FROM {nomeTabela}", conn, chunksize=100000):
                chunk.to_excel(writer, index=False, header=not writer.sheets, sheet_name='Dados')
                print(f"  ✅ Exportados {len(chunk):,} registros para XLSX")
        
        print(f"📁 Arquivo XLSX salvo em: {xlsx_path}")

        return True

    except mysql.connector.Error as err:
        print(f"🚨 Erro ao criar tabela simplificada: {err}")
        conn.rollback()
        return False
    except Exception as e:
        print(f"🚨 Erro inesperado: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def apagarDentroPasta(caminhoPasta):
    """
    Apaga todo o conteúdo de uma pasta específica, mantendo a pasta raiz.
    """
    try:
       
        if not os.path.exists(caminhoPasta):
            print(f"⚠️ A pasta {caminhoPasta} não existe.")
            return False
        
        if not os.path.isdir(caminhoPasta):
            print(f"⚠️ O caminho {caminhoPasta} não é uma pasta.")
            return False
        
        print(f"\n🧹 Limpando pasta: {caminhoPasta}")
        
    
        for nome in os.listdir(caminhoPasta):
            caminho_completo = os.path.join(caminhoPasta, nome)
            
            try:
                if os.path.isfile(caminho_completo) or os.path.islink(caminho_completo):
                    os.unlink(caminho_completo)  # Remove arquivos e links simbólicos
                    print(f"🗑️ Arquivo removido: {nome}")
                elif os.path.isdir(caminho_completo):
                    shutil.rmtree(caminho_completo)  # Remove subpastas e seu conteúdo
                    print(f"🗑️ Pasta removida: {nome}")
            except Exception as e:
                print(f"⚠️ Falha ao deletar {caminho_completo}. Razão: {e}")
        
        print(f"✅ Pasta limpa com sucesso: {caminhoPasta}")
        return True
    
    except Exception as e:
        print(f"🚨 Erro ao limpar pasta {caminhoPasta}: {e}")
        return False
    
def apagar_arquivos_especificos():
    """Apaga arquivos CSV específicos dentro da pasta 'arquivo receita deszipados'"""
    pasta = "arquivo receita deszipados"
    arquivos_para_apagar = [
        "empresas.csv",
        "estabelecimentos.csv",
        "socios.csv"
    ]

    for arquivo in arquivos_para_apagar:
        caminho = os.path.join(pasta, arquivo)
        if os.path.exists(caminho):
            os.remove(caminho)
            print(f"🗑️ Apagado: {caminho}")
        else:
            print(f"⚠️ Arquivo não encontrado: {caminho}")

    print("✔️ Exclusão concluída.")

def apagarDentroPasta10Para():
    """
    Limpa o conteúdo das pastas de dados divididos
    """
    pastas = [
        os.path.join('dividir', '10 PARTES EMPRESAS'),
        os.path.join('dividir', '10 PARTES ESTABELECIMENTOS'), 
        os.path.join('dividir', '10 PARTES SOCIOS')
    ]
    
    for pasta in pastas:
        apagarDentroPasta(pasta)

def validar_cnpj(cnpj):
    """Valida CNPJ usando dígitos verificadores"""
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    
   
    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    dv1 = 11 - (sum(int(cnpj[i])*pesos1[i] for i in range(12)) % 11)
    dv1 = 0 if dv1 >= 10 else dv1
    
    pesos2 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
    dv2 = 11 - (sum(int(cnpj[i])*pesos2[i] for i in range(13)) % 11)
    dv2 = 0 if dv2 >= 10 else dv2
    
    return int(cnpj[12]) == dv1 and int(cnpj[13]) == dv2

def limpar_telefone(tel):
    """Extrai apenas números de telefone"""
    if pd.isna(tel) or tel == '':
        return np.nan
    nums = re.sub(r'[^0-9]', '', str(tel))
    return nums if len(nums) >= 10 else np.nan

def processar_arquivo_com_fallback(input_file):
    """Tenta ler o arquivo com diferentes abordagens"""
    try:
       
        return pd.read_csv(input_file, sep=';', dtype=str)
    except pd.errors.ParserError:
        try:
       
            return pd.read_csv(input_file, sep=';', dtype=str, on_bad_lines='warn')
        except:
            
            with open(input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
           
            cleaned_lines = []
            for line in lines:
                parts = line.strip().split(';')
                cleaned_lines.append(';'.join(parts[:7]))
            
            return pd.read_csv(StringIO('\n'.join(cleaned_lines)), sep=';', dtype=str)

def fazerLeads():
  
    input_xlsx = f"export/{nomeTabela}.xlsx"
    output_dir = "LEADS"
    os.makedirs(output_dir, exist_ok=True)

    try:
       
        df = pd.read_excel(input_xlsx, engine='openpyxl')
        
      
        col_map = {
            'CNPJ_COMPLETO': 'CNPJ',
            'CNPJ': 'CNPJ',
            'RAZAO_SOCIAL': 'RAZAO_SOCIAL',
            'ENDERECO_COMPLETO': 'ENDERECO',
            'ENDERECO': 'ENDERECO',
            'CORREIO_ELETRONICO': 'EMAIL',
            'EMAIL': 'EMAIL',
            'ANO_DE_ABERTURA': 'ANO_ABERTURA',
            'ANO_ABERTURA': 'ANO_ABERTURA',
            'NATUREZA_JURIDICA': 'NATUREZA_JURIDICA',
            'TELEFONE': 'TELEFONES',
            'TELEFONES': 'TELEFONES'
        }
        
     
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
     
        cols_necessarias = ['CNPJ','RAZAO_SOCIAL','ENDERECO','EMAIL',
                           'ANO_ABERTURA','NATUREZA_JURIDICA','TELEFONES']
        df = df[[c for c in cols_necessarias if c in df.columns]].copy()
        
      
        df['CNPJ'] = df['CNPJ'].astype(str).str.zfill(14)
        df = df[df['CNPJ'].apply(validar_cnpj)]
        
        df['RAZAO_SOCIAL'] = df['RAZAO_SOCIAL'].fillna('nan').str.upper()
        ltda_mask = df['RAZAO_SOCIAL'].str.endswith('LTDA')
        
        df['ENDERECO'] = df['ENDERECO'].fillna('nan')
        df['EMAIL'] = df['EMAIL'].apply(lambda x: 'nan' if pd.isna(x) or '@' not in str(x) else x)
        df['ANO_ABERTURA'] = df['ANO_ABERTURA'].astype(str).str[:4].fillna('nan')
        df['NATUREZA_JURIDICA'] = df['NATUREZA_JURIDICA'].fillna('nan')
        df['TELEFONES'] = df['TELEFONES'].apply(limpar_telefone)
        df = df.dropna(subset=['TELEFONES'])
        df['TELEFONES'] = df['TELEFONES'].astype(str)
        
     
        with pd.ExcelWriter(f"{output_dir}/LTDA.xlsx", engine='openpyxl') as writer:
            df[ltda_mask].to_excel(writer, index=False)
        
        with pd.ExcelWriter(f"{output_dir}/MEI_ME.xlsx", engine='openpyxl') as writer:
            df[~ltda_mask].to_excel(writer, index=False)

        print(f"""
✅ Processamento concluído com sucesso!
• Empresas LTDA: {sum(ltda_mask)}
• Empresas MEI/ME: {sum(~ltda_mask)}
• Total de leads válidos: {len(df)}
Arquivos salvos em: {output_dir}/
        """)
        return True

    except Exception as e:
        print(f"🚨 Erro crítico: {str(e)}")
        return False
    
def organizarCNPJeTELEFONES(arquivoXLSX):
    def corrigir_cnpj(cnpj):
        cnpj = re.sub(r'\D', '', str(cnpj))
        if len(cnpj) == 14:
            return cnpj
        return None 

    def corrigir_telefone(telefone):
        telefone = re.sub(r'\D', '', str(telefone))
        if len(telefone) == 11 and telefone[2] == '0':
            telefone = telefone[:2] + '9' + telefone[3:]
        elif len(telefone) == 10:
            telefone = telefone[:2] + '9' + telefone[2:]
        elif len(telefone) != 11:
            return None 
        return telefone

    arquivo_xlsx = fr"LEADS/{arquivoXLSX}.xlsx"

    df = pd.read_excel(arquivo_xlsx, engine='openpyxl')

  
    if 'CNPJ' in df.columns:
        df['CNPJ_CORRIGIDO'] = df['CNPJ'].apply(corrigir_cnpj)

    if 'TELEFONES' in df.columns:
        df['TELEFONE_CORRIGIDO'] = df['TELEFONES'].apply(corrigir_telefone)

   
    df_validos = df.dropna(subset=['CNPJ_CORRIGIDO', 'TELEFONE_CORRIGIDO'])
    df_validos = df_validos.drop_duplicates(subset=['CNPJ_CORRIGIDO', 'TELEFONE_CORRIGIDO'])

   
    df_invalidos = df[df['CNPJ_CORRIGIDO'].isna() | df['TELEFONE_CORRIGIDO'].isna()]
    df_final = pd.concat([df_validos, df_invalidos], ignore_index=True)

  
    with pd.ExcelWriter(fr"LEADS/{arquivoXLSX}.xlsx", engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False)


def pegarDaApiBrasi():
    print(f"\n🚀 Iniciando coleta TURBO de CNPJs - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

   
    MAX_WORKERS = 15  
    REQUEST_TIMEOUT = 8  
    DELAY_ENTRE_LOTES = 0.1 
    TAMANHO_LOTE = 100  
    CACHE_TTL_HORAS = 24  

   
    cache_cnpj = {}
    cache_file = "cnpj_cache_turbo.json"
    
    
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
               
                now = datetime.now()
                for cnpj, data in cache_data.items():
                    if 'DATA_CONSULTA' in data:
                        consulta_time = datetime.strptime(data['DATA_CONSULTA'], '%d/%m/%Y %H:%M:%S')
                        if (now - consulta_time).total_seconds() < CACHE_TTL_HORAS * 3600:
                            cache_cnpj[cnpj] = data
            print(f"⚡ Cache carregado com {len(cache_cnpj)} CNPJs válidos")
        except Exception as e:
            print(f"⚠️ Erro ao carregar cache: {str(e)}")

   
    APIS = [
        {
            'nome': 'BrasilAPI',
            'url': 'https://brasilapi.com.br/api/cnpj/v1/{cnpj}',
            'headers': {},
            'parser': lambda data: {
                'NOME_SOCIO': data.get('qsa', [{}])[0].get('nome') if data.get('qsa') else None,
                'CNAE': data.get('cnae_fiscal_descricao'),
                'CAPITAL_SOCIAL': data.get('capital_social')
            },
            'weight': 10 
        },
        {
            'nome': 'CNPJA',
            'url': 'https://api.cnpja.com.br/companies/{cnpj}',
            'headers': {'Authorization': 'Bearer livre'},
            'parser': lambda data: {
                'NOME_SOCIO': data.get('partners', [{}])[0].get('name') if data.get('partners') else None,
                'CNAE': data.get('company', {}).get('primaryActivity', {}).get('description'),
                'CAPITAL_SOCIAL': data.get('company', {}).get('capital')
            },
            'weight': 8  
        },
        {
            'nome': 'ReceitaWS',
            'url': 'https://receitaws.com.br/v1/cnpj/{cnpj}',
            'headers': {},
            'parser': lambda data: {
                'NOME_SOCIO': data.get('qsa', [{}])[0].get('nome') if data.get('qsa') else None,
                'CNAE': data.get('atividade_principal', [{}])[0].get('text'),
                'CAPITAL_SOCIAL': data.get('capital_social')
            },
            'weight': 5 
        }
    ]

    def consultar_cnpj_turbo(cnpj):
        """Consulta turbo com fallback inteligente entre APIs"""
        cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))
        if len(cnpj_limpo) != 14:
            return None

       
        if cnpj_limpo in cache_cnpj:
            cached_data = cache_cnpj[cnpj_limpo]
            return cached_data

       
        apis_ordenadas = sorted(
            APIS,
            key=lambda x: x['weight'] * random.uniform(0.8, 1.2),
            reverse=True
        )
        
        for api in apis_ordenadas:
            try:
                start_time = time.time()
                response = requests.get(
                    api['url'].format(cnpj=cnpj_limpo),
                    headers=api['headers'],
                    timeout=REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    data = response.json()
                    
                    if not data:
                        continue
                        
                    resultado = {
                        'CNPJ': cnpj_limpo,
                        'FONTE': api['nome'],
                        'DATA_CONSULTA': datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                    }
                    resultado.update(api['parser'](data))
                    
                   
                    if 'CAPITAL_SOCIAL' in resultado:
                        try:
                            resultado['CAPITAL_SOCIAL'] = float(resultado['CAPITAL_SOCIAL'] or 0)
                        except:
                            resultado['CAPITAL_SOCIAL'] = 0.0
                    
                    
                    cache_cnpj[cnpj_limpo] = resultado
                    return resultado

                elif response.status_code == 429:
                    time.sleep(0.5)  
                    continue

            except requests.exceptions.RequestException:
                continue
            except Exception as e:
                continue

        return None

    def processar_lote_turbo(lote):
        """Processa um lote de CNPJs em paralelo"""
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            resultados = list(executor.map(consultar_cnpj_turbo, lote))
        return [r for r in resultados if r]

    def processar_arquivo_turbo(path):
        """Processamento turbo de arquivo XLSX"""
        try:
          
            df = pd.read_excel(path, engine='openpyxl', usecols=['CNPJ'], dtype={'CNPJ': str})
            cnpjs = df['CNPJ'].unique().tolist()
            
            print(f"\n📂 Arquivo: {os.path.basename(path)}")
            print(f"🔢 Total de CNPJs únicos: {len(cnpjs)}")
            print(f"⚡ Usando {MAX_WORKERS} threads paralelas (Turbo Mode)")

           
            resultados = []
            lotes = [cnpjs[i:i + TAMANHO_LOTE] for i in range(0, len(cnpjs), TAMANHO_LOTE)]
            
            for lote in tqdm(lotes, desc="🚀 Processando lotes Turbo"):
                resultados_lote = processar_lote_turbo(lote)
                resultados.extend(resultados_lote)
                time.sleep(DELAY_ENTRE_LOTES)

            
            if resultados:
                df_resultado = pd.DataFrame(resultados)
             
                for col in ['NOME_SOCIO', 'CNAE', 'CAPITAL_SOCIAL']:
                    if col not in df_resultado.columns:
                        df_resultado[col] = None
                
                return df_resultado[['CNPJ', 'NOME_SOCIO', 'CNAE', 'CAPITAL_SOCIAL', 'FONTE', 'DATA_CONSULTA']]
            return pd.DataFrame()

        except Exception as e:
            print(f"\n❌ Erro no processamento turbo: {str(e)}")
            return pd.DataFrame()

   
    arquivos_processados = 0
    for arquivo in os.listdir('LEADS'):
        if arquivo.endswith('.xlsx') and not arquivo.endswith('_ATUALIZADO.xlsx'):
            caminho = os.path.join('LEADS', arquivo)
            df = processar_arquivo_turbo(caminho)
            
            if not df.empty:
                caminho_saida = caminho.replace('.xlsx', '_ATUALIZADO.xlsx')
                with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                print(f"\n✅ {arquivo} processado com sucesso!")
                print(f"📊 Dados obtidos para {len(df)} CNPJs")
                arquivos_processados += 1

    
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_cnpj, f)
    except Exception as e:
        print(f"⚠️ Erro ao salvar cache: {str(e)}")

    print(f"\n🏁 Processamento concluído - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📂 Arquivos processados: {arquivos_processados}")
    print(f"⚡ Total de CNPJs consultados: {len(cache_cnpj)}")


def trasformarParaPegarOperadora():
   
    df_atualizada = pd.read_excel(r'LEADS\LTDA_ATUALIZADO.xlsx')
    df_original = pd.read_excel(r"LEADS\LTDA.xlsx")

   
    print("Colunas em LTDA_ATUALIZADO.xlsx:", df_atualizada.columns.tolist())
    print("Colunas em LTDA.xlsx:", df_original.columns.tolist())

    
    df_final = pd.merge(
        df_atualizada[["CNPJ", "NOME_SOCIO", "CNAE", "CAPITAL_SOCIAL"]],
        df_original[["CNPJ", "RAZAO_SOCIAL", "ENDERECO", "EMAIL", "ANO_ABERTURA", "TELEFONES"]],
        on="CNPJ",
        how="inner"
    )

   
    df_final = df_final[[
        "CNPJ",
        "RAZAO_SOCIAL",
        "NOME_SOCIO",
        "CNAE",
        "ENDERECO",
        "EMAIL",
        "ANO_ABERTURA",
        "CAPITAL_SOCIAL",
        "TELEFONES"
    ]]

  
    os.makedirs("pegar operadora", exist_ok=True)

    df_final.to_excel(r"pegar operadora\ltda-PegarOperadora.xlsx", index=False, engine="openpyxl")

def mainCompleto():
    """Executa o processo completo de ponta a ponta"""
    mainPegarDados()                # 1. Baixa os arquivos da Receita Federal
    time.sleep(1)
    dezipar()                       # 2. Descompacta os arquivos ZIP
    time.sleep(1)
    apagarDentroPasta('arquivo receita') # 3. Limpa arquivos ZIP baixados
    time.sleep(1)
    processar_arquivos_deszipados() # 4. Divide os CSVs em 10 partes
    time.sleep(1)
    recreate_database_tables()      # 5. Apaga e recria todas as tabelas no banco
    time.sleep(1)
    apagar_arquivos_especificos()   # 6. Limpa arquivos CSV descompactados
    time.sleep(1)
    recreate_database_tables()      # 7 Recriar as tabelas do banco
    time.sleep(1)
    mainJogarNoBanco()              # 8. Insere os dados divididos no banco
    time.sleep(36000)           # Espera 10 horas para garantir que o banco esteja atualizado
    apagarDentroPasta10Para()       # 9. Limpa arquivos divididos em 10 partes
    time.sleep(1)
    create_and_export_empresas()    # 10. Cria tabela final e exporta para CSV
    time.sleep(1)
    fazerLeads()                    # 11. Fazer LEDS
    time.sleep(1)
    apagarDentroPasta('export')     # apagar o Export
    time.sleep(1)
    organizarCNPJeTELEFONES('LTDA') # 12. Organiza CNPJ e Telefones
    time.sleep(1)
    organizarCNPJeTELEFONES('MEI_ME') # 13. Organiza CNPJ e Telefones
    time.sleep(1)
    pegarDaApiBrasi() # 14. Pega dados da API Brasil
    time.sleep(1)
    trasformarParaPegarOperadora()      # 14. Transforma para pegar a operadora

    print("\n✅ PROCESSO CONCLUÍDO COM SUCESSO!")

if __name__ == "__main__":
    mainCompleto()
