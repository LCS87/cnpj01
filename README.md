# 📊 Pipeline ETL: Big Data CNPJ Brasil
Este projeto é um pipeline completo de Extração, Transformação e Carga (ETL) desenhado para processar a base pública de dados do CNPJ da Receita Federal. O sistema automatiza desde o download de gigabytes de dados até o enriquecimento via APIs externas e a organização de leads qualificados.

## 🚀 Funcionalidades
Extração Automatizada: Crawler que identifica e baixa os arquivos zipados mais recentes da Receita Federal.

Processamento de Big Data: Sistema de descompactação e divisão de arquivos (chunking) para lidar com milhões de registros sem estourar a memória RAM.

Arquitetura SQL Otimizada: Carga massiva em banco de dados MySQL com recriação de estrutura e suporte a consultas complexas.

Enriquecimento de Dados: Integração com BrasilAPI, CNPJA e ReceitaWS para obter Capital Social e Quadro Societário (QSA).

Higienização de Leads: Algoritmos de limpeza de telefones (RegEx) e validação de CNPJ.

Exportação Multiformato: Geração de relatórios em CSV e XLSX segmentados por porte (LTDA e MEI/ME).

## 🛠️ Stack Tecnológica
Linguagem: Python 3.x

Manipulação de Dados: Pandas, Numpy

Banco de Dados: MySQL (MySQL Connector)

Web Scraping/Requests: BeautifulSoup4, Requests

Concorrência: ThreadPoolExecutor (Turbo Mode para APIs)

## 📂 Estrutura do Projeto

- ├── main.py                &nbsp;&nbsp;&nbsp; # Script principal (Orquestrador)
- ├── criação tabela.py      &nbsp;&nbsp;&nbsp;         # Definição de schemas SQL e joins
- ├── arquivo receita/       &nbsp;&nbsp;&nbsp;         # Dados brutos (ZIPs)
- ├── dividir/               &nbsp;&nbsp;&nbsp;         # CSVs fragmentados para carga rápida
- ├── export/                &nbsp;&nbsp;&nbsp;         # Tabelas consolidadas
- ├── LEADS/                 &nbsp;&nbsp;&nbsp;         # Arquivos finais prontos para uso
- └── cnpj_cache_turbo.json  &nbsp;&nbsp;&nbsp;       # Cache local para economia de requisições API


## ⚙️ Como Executar

Configuração do Banco: Certifique-se de que o MySQL está rodando e ajuste as credenciais no DB_CONFIG dentro do main.py.

Instalação de Dependências:

Bash
pip install -r requirements.txt

## ⚠️ Observações de Performance

Paciência na Carga: O processo de INSERT no banco de dados pode levar horas dependendo do seu hardware (especialmente para a tabela de estabelecimentos).

Rate Limit: O enriquecimento via APIs possui delays controlados para evitar banimento de IP.

Espaço em Disco: Recomenda-se pelo menos 100GB de espaço livre para o processamento dos arquivos temporários e índices do banco de dados.
