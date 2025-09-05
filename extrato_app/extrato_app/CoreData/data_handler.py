import os
import re
import pandas as pd
from tqdm import tqdm
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv
from extrato_app.CoreData.dba import DBA
from extrato_app.CoreData.consolidador import Consolidador
from extrato_app.CoreData.trat_rec import TratamentoRecalculo
import json
from pathlib import Path
from datetime import datetime
from extrato_app.CoreData.handlers_registry import CIA_HANDLERS


pd.options.mode.chained_assignment = None

load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))

class DataHandler:
    
    def __init__(self):
        self.competencia = os.getenv('competencia')
        self.dba = DBA()
        self.consolidador = Consolidador()
        self.tratamento_recalculo = TratamentoRecalculo()
        
        # self.dispatcher = {
        #     "Bradesco": BradescoHandler(),
        #     "Bradesco Saude": BradescoSaudeHandler(),
        #     "Suhai": SuhaiHandler(),
        #     "Allianz": AllianzHandler(),
        #     "Junto Seguradora": JuntoHandler(),
        #     "Hdi": HDIHandler(),
        #     "Porto": PortoHandler(),
        #     "Yelum": YelumHandler(),
        #     "Axa": AxaHandler(),
        #     "Zurich": ZurichHandler(),
        #     "Chubb": ChubbHandler(),
        #     "Tokio": TokioHandler(),
        #     "Ezze": EzzeHandler(),
        #     "Sompo": SompoHandler(),
        #     "Mapfre": MapfreHandler(),
        #     "Swiss": SwissHandler(),
        # }

    @staticmethod
    def sanitize_table_name(table_name: str) -> str:
        return re.sub(r'[^a-zA-Z0-9_]', '', table_name)

    @staticmethod
    def padronizar_nomes(nome: str) -> str:
        nome = nome.strip().lower().replace(" ", "_")
        nome = re.sub(r'[áàâãä]', 'a', nome)
        nome = re.sub(r'[éèêë]', 'e', nome)
        nome = re.sub(r'[íìîï]', 'i', nome)
        nome = re.sub(r'[óòôõö]', 'o', nome)
        nome = re.sub(r'[úùûü]', 'u', nome)
        nome = re.sub(r'[ç]', 'c', nome)
        nome = re.sub(r'_/_', '_', nome)
        nome = re.sub(r'ramo_inteno', 'ramo_interno', nome)
        nome = re.sub(r'nº_form._venda', 'numero_form_venda', nome)
        nome = re.sub(r'cpf\\cnpj_do_segurado', 'cpf_cnpj_do_segurado', nome)
        nome = re.sub(r'r\$_premio_liquido', 'valor_premio_liquido', nome)
        nome = re.sub(r'%', 'pct', nome)
        nome = re.sub(r'r\$_comissao', 'valor_comissao', nome)
        nome = re.sub(r'novo?', 'novo', nome)
        nome = re.sub(r'_-_', '_', nome)
        return nome

    def get_table_mapping(self) -> List[str]:
        tables_str = os.getenv("input_history_tables", "")
        print(f"🔍 Tabelas mapeadas: {tables_str}")
        return [self.sanitize_table_name(t.strip().lower()) for t in tables_str.split(",") if t.strip()]

    def convert_df_to_db_schema(self, df: pd.DataFrame, column_types: Dict[str, str]) -> pd.DataFrame:
        
        # print('===================== validação de colunas previa a removação de duplicação de colunas aqui ===============================')
        # print(df.columns)
        # print('===================== validação de colunas previa a removação de duplicação de colunas aqui ===============================')
        
        
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        
        # print('===================== validação de colunas apos a removação de duplicação de colunas aqui ===============================')
        # print(df.columns)
        # print('===================== validação de colunas apos a removação de duplicação de colunas aqui ===============================')
        
        
        for col, dtype in column_types.items():
            if col not in df.columns:
                continue
            if 'int' in dtype:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")
            elif any(x in dtype for x in ['numeric', 'double', 'real', 'decimal']):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif 'date' in dtype or 'timestamp' in dtype:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif 'bool' in dtype:
                df[col] = df[col].astype('boolean')
            elif 'char' in dtype or 'text' in dtype:
                df[col] = df[col].astype(str).fillna('')
            else:
                df[col] = df[col].astype(str).fillna('')
        return df

    def read_df(self, root_folder_path: str, cia_escolhida: str) -> pd.DataFrame:
        try:
            # handler = self.dispatcher.get(cia_escolhida)
            handler = CIA_HANDLERS.get(cia_escolhida)

            if handler:
                df = handler.treat(root_folder_path)
                if not isinstance(df, pd.DataFrame):
                    print(f"❌ ERRO: O método treat do handler {cia_escolhida} não retornou um DataFrame.")
                    return pd.DataFrame()
                return df
            else:
                print(f"❌ CIA não reconhecida: {cia_escolhida}")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ Erro ao tratar arquivos da {cia_escolhida}: {e}")
            return pd.DataFrame()

    def process_files(self, root_folder_path: str) -> Dict[str, pd.DataFrame]:
        print('aqui insider -------')
        try:
            with open(os.path.join(os.getcwd(), "config.json"), "r", encoding="utf-8") as f:
                cia_escolhida = json.load(f).get("cia_corresp", "")
        except Exception as e:
            print(f"❌ Erro ao ler CIA do config.json: {e}")
            return {}

        cias_corresp = [cia.strip() for cia in os.getenv("cia_corresp", "").split(",") if cia.strip()]
        input_tables = [tbl.strip() for tbl in os.getenv("input_history_tables", "").split(",") if tbl.strip()]
        
        if len(cias_corresp) != len(input_tables) or not cia_escolhida:
            print("❌ Configuração inválida: Verifique cia_corresp e input_history_tables no .env")
            return {}

        table_name = None
        for cia, table in zip(cias_corresp, input_tables):

            if cia == cia_escolhida:
                table_name = self.sanitize_table_name(table)
                break

        if not table_name:
            print(f"❌ Nenhuma tabela mapeada para a CIA: {cia_escolhida}")
            return {}

        print(f"🔍 CIA selecionada: {cia_escolhida} → Tabela: {table_name}")

        try:
            files = [f for f in os.listdir(root_folder_path) 
                    if f.lower().endswith(('.xls', '.xlsx', '.xlsb')) 
                    ]
            
            if not files:
                print("❌ Nenhum arquivo encontrado na pasta")
                return {}

            print(' ================================ checagem aqui de cia_escolhida:', cia_escolhida)
            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(root_folder_path, f)))
            file_path = os.path.join(root_folder_path, latest_file)
            # print(f"📂 Processando arquivo mais recente: {latest_file}")

            df = self.read_df(root_folder_path, cia_escolhida)
            print('Primeiras linhas do DataFrame:', df.head())
            
            df = self.dba.add_id_unidade_from_database(df, cia_escolhida)
            print('check de tempo de execução')
            df.columns = [self.padronizar_nomes(c) for c in df.columns]
            df['origem_arquivo'] = latest_file
            
            premio_db = Decimal(str(self.consolidador.cons_caixa_declarado()))

            if cia_escolhida == "Junto Seguradora" and df.shape[1] > 1:

                print('df columns antes da padronização:')
                print(df.columns)
                print('🔍 Renomeando apenas a segunda coluna para "nome_corretora"')
                col_list = list(df.columns)
                col_list[1] = "nome_corretora"
                df.columns = col_list
                print('Colunas do DataFrame após padronização:')
                print(df.columns)
            
            df.columns = df.columns.str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
            colunas_correspondem, df, ult_premio_valido, premio_exec, fator_hist = self.dba.cons_columns(df)
            
            if not colunas_correspondem:
                df, premio_vigente, premio_exec = self.dba.analise_autonoma(
                    df=df,
                    ult_premio_valido=ult_premio_valido,
                    fator_melchiori_hist=fator_hist,
                    premio_db=ult_premio_valido,
                    cia_escolhida=cia_escolhida
                )

            premio_rel = Decimal(str(self.tratamento_recalculo.cons_rel(df, cias_corresp, latest_file, table_name, premio_exec)))
            print('premio_rel:', premio_rel)
            print('premio_db:', premio_db)
            
            getcontext().prec = 8
            
            print(cia)
            
            ###MODULAR DISPATCHER PARA CALCULAR FATOR MELCHIORI VIA HANDLER
            fator_melchiori = (premio_db / premio_rel).quantize(Decimal('0.000000'))
            
            if cia_escolhida == 'Yelum':
                fator_melchiori = 0.964999
                print('🔍 Fator Melchiori fixo para Yelum:', fator_melchiori)

            elif cia_escolhida == 'Axa' or cia_escolhida == 'Chubb':
                fator_melchiori = ( premio_db / premio_rel).quantize(Decimal('0.000000'))
                print(f'🔍 Fator Melchiori fixo para {cia_escolhida}:', fator_melchiori)

            print(f"🔍 Fator Melchiori: {fator_melchiori}")
            insert_padroes = self.dba.insert_padroes(df=df, premio=premio_exec, fator_melchiori=float(fator_melchiori) if fator_melchiori is not None else None)
            premio_exec = self.padronizar_nomes(premio_exec)
            print(f"🔍 Premio Exec: {premio_exec}")
            print('==================================================passed 96==================================================')
            print('premio_db:', premio_db) 

            print(cia_escolhida)
            if cia_escolhida == 'Yelum':
                print('NEITHEr')
                self.tratamento_recalculo.process_recalculo(df, cias_corresp,table_name,premio_exec,fator_melchiori)
            elif cia_escolhida == 'Ezze':
                self.tratamento_recalculo.process_recalculo(df, cias_corresp, table_name, premio_exec, fator_melchiori)
            else:
                self.tratamento_recalculo.process_recalculo(df, cias_corresp, latest_file, table_name, premio_exec=premio_exec, fator_melchiori=float(fator_melchiori), premio_db=premio_db)

            print(f"✅ {latest_file} | Linhas: {len(df)}")
            return {table_name: df}

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return {}

    def treat_zero(self, file_dfs: Dict[str, Any], id_cia: str) -> Dict[str, Any]:
        processed_dfs = {}

        for table_name, data in file_dfs.items():
            print(f"\n📤 Processando tabela: {table_name}")

            if isinstance(data, dict) and 'df' in data:
                df = data['df']
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                print(f"⚠️ Dados inválidos para a tabela {table_name}: tipo {type(data)}")
                continue

            if id_cia == '556':
                print('=================================== validação inicial para EZZE em TREAT ZERO ===================================')
                print(df.columns)
                df = df[df['cd_apolice'].astype(str).str.lower().str.strip().replace('nan', pd.NA).notna()]
            
            print(df.head())

            df = df.rename(columns={'cv': 'premio_rec', 'vi': 'valor_cv', 'as': 'valor_as'})
            # print(df[['premio_base', 'premio_rec', 'valor_cv', 'valor_as']])

            df['id_seguradora_quiver'] = id_cia
            df.columns = [col.lower() for col in df.columns]
            
            df.columns = [col.replace('(', '').replace(')', '') for col in df.columns]
            print('Colunas após padronização:')
            print(df.columns)
            column_types = self.dba.get_column_types(table_name)
            db_columns = list(column_types.keys())
            print('colunas do banco de dados:', db_columns)

            df_filtered = df[[col for col in db_columns if col in df.columns]]
            df_filtered = self.convert_df_to_db_schema(df_filtered, column_types)
            print(f"🔍 Colunas filtradas para {table_name}: {df_filtered.columns.tolist()}")

            processed_dfs[table_name] = {
                'df': df_filtered,
                'ordered_cols': [col for col in db_columns if col in df_filtered.columns],
                'ordered_cols_escaped': [f'"{col}"' for col in db_columns if col in df_filtered.columns]
            }

        return processed_dfs
    
    def cons_div(self, df):
        print('validando as colunas para consolidação divisional...')
        print(df.columns)
        cols_to_sum = ['premio_rec', 'valor_cv', 'valor_as', 'valor_vi']
        extra_cols = ['id_unidade', 'id_cor_cliente']
        required_cols = cols_to_sum + ['nome_unidade'] + extra_cols
        if not all(col in df.columns for col in required_cols):
            print("⚠️ Colunas necessárias não encontradas para consolidação divisional.")
            return None

        agg_dict = {col: 'sum' for col in cols_to_sum}
        agg_dict.update({col: 'max' for col in extra_cols})

        df_cons = (
            df.groupby('nome_unidade', as_index=False)
            .agg(agg_dict)
        )
        return df_cons

    def export_to_excel(self, processed_data: Dict[str, Any], output_folder: str = None) -> bool:
        
        overall_success = True
        if output_folder is None:
            output_folder = str(Path.home() / "Downloads")
        
        print(f"\n📁 Exportando arquivos Excel para: {output_folder}")
        
        for table_name, data in processed_data.items():
            try:
                df = data['df'].copy()

                print("========================= validação pre export. =========================")
                print(df.head())

            except Exception as e:
                print(f"❌ Erro ao exportar {table_name}: {str(e)}")
                overall_success = False
        
        return overall_success



    def read_incentivo_via_dispatcher(self, cia_escolhida: str) -> pd.DataFrame:
        """
        Usa o dispatcher (CIA_HANDLERS) para chamar, se existir, a leitura de incentivo
        especifica da CIA (ex.: BradescoHandler.read_incentivo).
        """
        try:
            handler = CIA_HANDLERS.get(cia_escolhida)
            if not handler:
                print(f"⚠️ Sem handler para {cia_escolhida}; pulando leitura de incentivo.")
                return pd.DataFrame()
            if hasattr(handler, "read_incentivo") and callable(handler.read_incentivo):
                print(f"🔎 Lendo incentivo via handler de {cia_escolhida}...")
                inc_df = handler.read_incentivo()
                if isinstance(inc_df, pd.DataFrame) and not inc_df.empty:
                    inc_df.columns = [self.padronizar_nomes(c) for c in inc_df.columns]
                    return inc_df
                print("⚠️ Incentivo retornou vazio.")
                return pd.DataFrame()
            else:
                print(f"ℹ️ Handler de {cia_escolhida} não implementa read_incentivo().")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ Erro ao ler incentivo via dispatcher: {e}")
            return pd.DataFrame()
        
        
