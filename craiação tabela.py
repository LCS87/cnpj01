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


anoMes = '2025-08'
nomeTabela = f'empresas_{anoMes.replace("-", "")}_ATLZ'
DBsenha = '33548084'
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

def get_db_connection():
    """Estabelece conexão com o banco de dados"""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as err:
        print(f"🚨 Erro ao conectar ao MySQL: {err}")
        return None

def verificar_tabelas_existentes(cursor):
    """Verifica quais tabelas existem no banco de dados"""
    cursor.execute("SHOW TABLES")
    tabelas = [tabela[0] for tabela in cursor.fetchall()]
    print(f"📊 Tabelas existentes no banco: {tabelas}")
    return tabelas

def verificar_estrutura_tabela(cursor, tabela_nome):
    """Verifica a estrutura de uma tabela específica"""
    try:
        cursor.execute(f"DESCRIBE {tabela_nome}")
        colunas = [coluna[0] for coluna in cursor.fetchall()]
        print(f"📋 Estrutura da tabela {tabela_nome}: {colunas}")
        return colunas
    except:
        print(f"⚠️  Não foi possível verificar a estrutura da tabela {tabela_nome}")
        return []


conn = get_db_connection()
cursor = conn.cursor()

try:
    print(f"\n🔧 Criando tabela consolidada {nomeTabela} com dados de empresas, estabelecimentos e sócios...")

   
    tabelas_existentes = verificar_tabelas_existentes(cursor)
    
    
    tabelas_necessarias = ['empresas', 'estabelecimentos', 'socios']
    for tabela in tabelas_necessarias:
        if tabela not in tabelas_existentes:
            print(f"🚨 Tabela {tabela} não encontrada no banco de dados!")
            exit()

   
    print("\n🔍 Verificando estrutura das tabelas...")
    colunas_empresas = verificar_estrutura_tabela(cursor, 'empresas')
    colunas_estabelecimentos = verificar_estrutura_tabela(cursor, 'estabelecimentos')
    colunas_socios = verificar_estrutura_tabela(cursor, 'socios')
    
  
    cursor.execute("SET GLOBAL innodb_buffer_pool_size=268435456")  # 256MB
    cursor.execute("SET GLOBAL innodb_lock_wait_timeout=120")
    cursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")

   
    cursor.execute(f"DROP TABLE IF EXISTS {nomeTabela}")
    
    create_structure_query = f"""
    CREATE TABLE {nomeTabela} (
        cnpj_completo VARCHAR(14),
        razao_social VARCHAR(150),
        capital_social DECIMAL(18,2),
        nome_fantasia VARCHAR(150),
        endereco_completo TEXT,
        telefone VARCHAR(20),
        email VARCHAR(115),
        nome_socio VARCHAR(150),
        cpf_cnpj_socio VARCHAR(20),
        tipo_socio VARCHAR(50),
        data_entrada_sociedade DATE,
        ano_abertura INT,
        cnae_principal VARCHAR(100),
        natureza_juridica VARCHAR(100),
        porte_empresa VARCHAR(50),
        situacao_cadastral VARCHAR(50),
        data_situacao_cadastral DATE,
        data_inicio_atividade DATE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
    cursor.execute(create_structure_query)
    
   
    if 'municipios' in tabelas_existentes:
        municipio_join = "LEFT JOIN municipios mun ON est.municipio = mun.codigo_municipio"
        municipio_select = "IFNULL(CONCAT(' - ', mun.nome_municipio), '')"
    else:
        print("⚠️  Tabela municipios não encontrada. Usando código de município diretamente.")
        municipio_join = ""
        municipio_select = "IFNULL(CONCAT(' - ', est.municipio), '')"
    
    
    nome_socio_select = "s.nome_do_socio AS nome_socio"
    cpf_cnpj_socio_select = "s.cnpj_ou_cpf_do_socio AS cpf_cnpj_socio"
    
    tipo_socio_select = """
    CASE 
        WHEN s.identificador_de_socio = '1' THEN 'Pessoa Jurídica'
        WHEN s.identificador_de_socio = '2' THEN 'Pessoa Física'
        WHEN s.identificador_de_socio = '3' THEN 'Estrangeiro'
        ELSE 'Não Informado'
    END AS tipo_socio"""
    
    data_entrada_select = "s.data_de_entrada_sociedade AS data_entrada_sociedade"
    cnae_principal_select = "est.cnae_fiscal_principal AS cnae_principal"
    natureza_juridica_select = "e.natureza_juridica AS natureza_juridica"
    
   
    insert_query = f"""
    INSERT INTO {nomeTabela} (
        cnpj_completo, razao_social, capital_social, nome_fantasia, 
        endereco_completo, telefone, email, nome_socio, cpf_cnpj_socio,
        tipo_socio, data_entrada_sociedade, ano_abertura, cnae_principal,
        natureza_juridica, porte_empresa, situacao_cadastral, 
        data_situacao_cadastral, data_inicio_atividade
    )
    SELECT
        CONCAT(e.cnpj_basico, LPAD(est.cnpj_ordem, 4, '0'), LPAD(est.cnpj_dv, 2, '0')) AS cnpj_completo,
        e.razao_social,
        e.capital_social,
        est.nome_fantasia,
        CONCAT(
            IFNULL(CONCAT(est.tipo_de_logradouro, ' '), ''),
            IFNULL(est.logradouro, ''),
            IFNULL(CONCAT(', ', est.numero), ''),
            IFNULL(CONCAT(' - ', est.complemento), ''),
            IFNULL(CONCAT(' - ', est.bairro), ''),
            {municipio_select},
            IFNULL(CONCAT(' - ', est.uf), ''),
            IFNULL(CONCAT(' - CEP: ', est.cep), '')
        ) AS endereco_completo,
        CONCAT(IFNULL(est.ddd1, ''), IFNULL(est.telefone1, '')) AS telefone,
        est.correio_eletronico AS email,
        {nome_socio_select},
        {cpf_cnpj_socio_select},
        {tipo_socio_select},
        {data_entrada_select},
        YEAR(est.data_de_inicio_da_atividade) AS ano_abertura,
        {cnae_principal_select},
        {natureza_juridica_select},
        CASE 
            WHEN e.porte_da_empresa = '01' THEN 'Micro Empresa'
            WHEN e.porte_da_empresa = '03' THEN 'Empresa de Pequeno Porte'
            WHEN e.porte_da_empresa = '05' THEN 'Demais'
            ELSE 'Não Informado'
        END AS porte_empresa,
        CASE 
            WHEN est.situacao_cadastral = '01' THEN 'Nula'
            WHEN est.situacao_cadastral = '02' THEN 'Ativa'
            WHEN est.situacao_cadastral = '03' THEN 'Suspensa'
            WHEN est.situacao_cadastral = '04' THEN 'Inapta'
            WHEN est.situacao_cadastral = '08' THEN 'Baixada'
            ELSE 'Não Informada'
        END AS situacao_cadastral,
        est.data_situacao_cadastral,
        est.data_de_inicio_da_atividade
    FROM empresas e
    INNER JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
    LEFT JOIN socios s ON e.cnpj_basico = s.cnpj_basico
    {municipio_join}
    WHERE est.uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE')
    AND est.data_de_inicio_da_atividade BETWEEN '1955-01-01' AND '2025-12-31'
    """
    
    print("🔍 Executando query de inserção...")
    print(f"Query: {insert_query[:500]}...") 
    
    
    cursor.execute(insert_query)
    conn.commit()
    
    row_count = cursor.rowcount
    print(f"✅ Inseridos {row_count:,} registros na tabela {nomeTabela}")

 
    cursor.execute("SET SESSION sql_mode=(SELECT CONCAT(@@sql_mode,',ONLY_FULL_GROUP_BY'))")

    
    print("\n📤 Exportando dados para CSV...")
    export_dir = "export"
    os.makedirs(export_dir, exist_ok=True)
    csv_path = os.path.join(export_dir, f"{nomeTabela}.csv")

    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        f.write("CNPJ_COMPLETO;RAZAO_SOCIAL;CAPITAL_SOCIAL;NOME_FANTASIA;ENDERECO_COMPLETO;TELEFONE;EMAIL;NOME_SOCIO;CPF_CNPJ_SOCIO;TIPO_SOCIO;DATA_ENTRADA_SOCIEDADE;ANO_ABERTURA;CNAE_PRINCIPAL;NATUREZA_JURIDICA;PORTE_EMPRESA;SITUACAO_CADASTRAL;DATA_SITUACAO_CADASTRAL;DATA_INICIO_ATIVIDADE\n")

        cursor.execute(f"SELECT COUNT(*) FROM {nomeTabela}")
        total_export = cursor.fetchone()[0]
        print(f"  📊 Total a exportar: {total_export:,} registros")

        export_batch = 50000
        for offset in range(0, total_export, export_batch):
            cursor.execute(f"""
                SELECT 
                    cnpj_completo, razao_social, capital_social, nome_fantasia,
                    endereco_completo, telefone, email, nome_socio, cpf_cnpj_socio,
                    tipo_socio, data_entrada_sociedade, ano_abertura, cnae_principal,
                    natureza_juridica, porte_empresa, situacao_cadastral,
                    data_situacao_cadastral, data_inicio_atividade
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

except mysql.connector.Error as err:
    print(f"🚨 Erro ao criar tabela consolidada: {err}")
    conn.rollback()
except Exception as e:
    print(f"🚨 Erro inesperado: {e}")
    import traceback
    traceback.print_exc()
finally:
    cursor.close()
    conn.close()