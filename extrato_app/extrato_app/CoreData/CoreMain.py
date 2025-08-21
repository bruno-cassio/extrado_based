"""
Este script realiza a importação de dados de arquivos Excel para um banco de dados PostgreSQL. 
Ele foi projetado para processar arquivos específicos, mapear tabelas no banco de dados e 
realizar a inserção de dados de forma eficiente. Abaixo está uma descrição detalhada das 
principais funções e do fluxo de execução:

Considerando o processo de extração desenvolvido para mais Cias, este módulo deverá atender univeralmente a call do extrator, seja qual for a CIA;

Funções principais:
-------------------
1. create_database_connection():
    - Estabelece uma conexão com o banco de dados PostgreSQL utilizando variáveis de ambiente 
      para configurar os parâmetros de conexão.
2. sanitize_table_name(table_name):
    - Normaliza o nome da tabela removendo caracteres inválidos, garantindo que o nome seja 
      compatível com o banco de dados.
3. get_table_mapping():
    - Obtém a lista de tabelas mapeadas a partir de uma variável de ambiente e retorna uma 
      lista de nomes de tabelas sanitizados.
4. get_column_types(cursor, table_name):
    - Recupera os tipos de dados das colunas de uma tabela no banco de dados, utilizando 
      a tabela de metadados `information_schema.columns`.
5. convert_df_to_db_schema(df, column_types):
    - Converte os tipos de dados de um DataFrame do pandas para corresponder aos tipos de 
      dados esperados no banco de dados.
6. import_data_to_db(file_dfs):
    - Realiza a importação dos dados de múltiplos DataFrames para as tabelas correspondentes 
      no banco de dados. Inclui validação de colunas, filtragem e inserção em lote para 
      melhorar a performance.
7. process_files(root_folder_path):
    - Processa os arquivos Excel em um diretório específico, sanitiza os nomes das colunas 
      e prepara os dados para importação.
Fluxo de execução:
------------------
1. O script começa carregando as variáveis de ambiente e definindo o caminho da pasta raiz 
    onde os arquivos Excel estão localizados.
2. A função `process_files()` é chamada para processar os arquivos Excel no diretório 
    especificado. Apenas arquivos que seguem o padrão "CONSOLIDADO-" são considerados.
3. Os dados processados são armazenados em um dicionário, onde as chaves são os nomes das 
    tabelas e os valores são os DataFrames correspondentes.
4. A função `import_data_to_db()` é chamada para importar os dados processados para o banco 
    de dados. Durante a importação, são realizadas validações de colunas e conversões de tipos.
5. O script exibe mensagens de progresso e erros durante o processamento e a importação, 
    garantindo que o usuário seja informado sobre o status da execução.
6. Ao final, o script informa se a importação foi concluída com sucesso ou se ocorreram erros.
Requisitos:
-----------
- Python 3.6 ou superior
- Bibliotecas: psycopg2, pandas, dotenv, tqdm, openpyxl
- Banco de dados PostgreSQL configurado e acessível
- Variáveis de ambiente configuradas para conexão com o banco de dados e mapeamento de tabelas
Como executar:
--------------
1. Configure as variáveis de ambiente necessárias no arquivo `.env`.
2. Certifique-se de que os arquivos Excel estejam no diretório especificado.
3. Execute o script diretamente para iniciar o processamento e a importação.
##-- Validar se a independência entre input x extração deve ser preservada independente de manutenção; 

========================================================================================================================================
*******  O processo abaixo esta desenhado com base em unificação dos arquivos, próximo passo para este módulo é preparar o ambiente para apenas corresponder
à importação dos arquivos extraídos, independente de unificação ou não e independente do start ser via call do extrator ou inicio programado ******

Herdar do extrator a construçao de cia e tabela no .env, para que o processo de importação seja feito sequencialmente ao extrator, tanto faseado quanto independentemente.
========================================================================================================================================

"""
from dotenv import load_dotenv
import os
from tqdm import tqdm
from typing import Dict, Any, Tuple, List, Optional
from extrato_app.CoreData.grande_conn import DatabaseManager
from extrato_app.CoreData.dba import DBA
from extrato_app.CoreData.data_handler import DataHandler
from extrato_app.CoreData.consolidador import Consolidador
import json
from extrato_app.CoreData.ds4 import parse_meses_opt, escolher_cia_e_atualizar_config, obter_mes_ano
from pathlib import Path
import pandas as pd
import time
import msoffcrypto
from io import BytesIO


load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))

ROOT_NUMS = os.getenv("ROOT_NUMS", "")
MESES_PT = parse_meses_opt(os.getenv("MESES_OPT", ""))

def obter_mes_ano_from_config() -> Tuple[int, int]:
    config_path = os.path.join(os.getcwd(), "config.json")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            try:
                config = json.load(f)
                competencia = config.get("competencia", "")
                if competencia:
                    return obter_mes_ano(competencia)
            except Exception:
                pass
    raise ValueError("Competência não definida nem no parâmetro nem no config.json")

class DataImporter:
    def __init__(self, cia_manual: Optional[str] = None, competencia_manual: Optional[str] = None):
        if not ROOT_NUMS:
            print("🚨 ROOT_NUMS não encontrado no .env")
            return
        if not MESES_PT:
            print("🚨 MESES_OPT não encontrado no .env")
            return

        if cia_manual:
            self.cia_escolhida = cia_manual
        else:
            self.cia_escolhida = escolher_cia_e_atualizar_config()
        
        if not self.cia_escolhida:
            return
        
        mes, ano = obter_mes_ano(competencia_manual) if competencia_manual else obter_mes_ano_from_config()
        
        self.root_path = os.path.join(
            ROOT_NUMS,
            str(ano),
            "Controle de produção",
            f"{mes} - {MESES_PT[mes]}",
            self.cia_escolhida
        )
        
        print(f"\n📂 Caminho selecionado: {self.root_path}")
        
        self.dba = DBA()
        self.data_handler = DataHandler()
        self.consolidador = Consolidador()

    def import_data_to_db(self, processed_data: Dict[str, Any], id_cia: str) -> bool:

        print('Iniciando importação principal...')
        overall_success = True
        
        for table_name, data in processed_data.items():
            print(f"📊 Importando {table_name}...")
            conn = DatabaseManager.get_connection()
            try:
                success = self.dba.import_main(
                    conn=conn,
                    df_filtered=data['df'],
                    table_name=table_name,
                    ordered_cols=data['ordered_cols'],
                    ordered_cols_escaped=data['ordered_cols_escaped']
                )
                if success is not None:
                    overall_success &= success
                else:
                    print(f"⚠️ A importação de {table_name} retornou None, considerado como falha")
                    overall_success = False
            finally:
                DatabaseManager.return_connection(conn)
        
        return overall_success

    def execute_pipeline(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        start = time.perf_counter()
        print(f"📂 Acessando pasta: {self.root_path}")
        file_dfs = self.data_handler.process_files(self.root_path)
        if not file_dfs:
            print("🚨 Nenhum dado válido encontrado")
            return False, None

        print("\n🔍 Validando CIAs...")
        existing_cias, non_existing_cias, cias_list, id_cia = self.dba.get_and_compare_cias()
        if not id_cia:
            print("🚨 Nenhum ID de CIA válido encontrado")
            return False, None

        print("\n⚙️ Processando dados...")
        processed_data = self.data_handler.treat_zero(file_dfs, id_cia)

        print("\n🚀 Iniciando importação...")
        import_success = self.import_data_to_db(processed_data, id_cia)

        print("\n💾 Exportando dados para Excel...")
        export_success = self.data_handler.export_to_excel(processed_data)
        
        overall_success = import_success and export_success

        if overall_success:
            print("\n🎉 Pipeline concluído com sucesso!")
        else:
            if not import_success:
                print("\n⚠️ Importação concluída com erros")
            if not export_success:
                print("\n⚠️ Exportação para Excel concluída com erros")


        end = time.perf_counter()
        print(f"⏱️ Tempo de leitura otimizada: {end - start:.2f}s")


        return overall_success, processed_data



def main(cia_manual: Optional[str] = None, competencia_manual: Optional[str] = None):
    importer = DataImporter(cia_manual, competencia_manual)
    return importer.execute_pipeline()

if __name__ == "__main__":
    main()
