import pandas as pd
from extrato_app.CoreData.grande_conn import DatabaseManager
import os
from dotenv import load_dotenv
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_batch
import numpy as np
from typing import Dict, Tuple, Optional, List, Any, Union
import  json

load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))

class DBA:
    
    def __init__(self):
        try:
            config_path = os.path.join(os.getcwd(), "config.json")
            with open(config_path, "r", encoding="utf-8") as f:
                try:
                    config_data = json.load(f)
                    cia_escolhida = config_data.get("cia_corresp", "")
                    competencia_escolhida = config_data.get("competencia", "")
                    
                    self.cia = cia_escolhida
                    self.cia_corresp = self.cia
                    self.competencia = competencia_escolhida
                    
                    print(f"🔍 CIA escolhida: {self.cia}")
                    print(f"📅 Competência: {self.competencia}")
                    
                except json.JSONDecodeError:
                    print("❌ Erro: config.json está mal formatado ou vazio")
                    return [], [], [], None
        
        except FileNotFoundError:
            print("❌ Erro: arquivo config.json não encontrado")
            return [], [], [], None

    def get_column_types(self, table_name: str) -> Dict[str, str]:
        query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        conn = DatabaseManager.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (table_name,))
                return {row[0].lower(): row[1] for row in cursor.fetchall()}
        finally:
            DatabaseManager.return_connection(conn)

    def add_id_unidade_from_database(self, df: pd.DataFrame, cia_escolhida) -> pd.DataFrame:
        # print(f'cia_escolhida:{cia_escolhida}')
        config_path = os.path.join(os.getcwd(), "config.json")
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                ref_nom = config.get('ref_nom', '')
                
                if not ref_nom:
                    print("🚨 Coluna de referência não definida no config.json")
                    return df
        except Exception as e:
            print(f"❌ Erro ao ler config.json: {e}")
            return df

        if ref_nom not in df.columns:
            print(f"🚨 DataFrame não contém a coluna '{ref_nom}' definida no config.json")
            return df

        
        if cia_escolhida == 'Sompo':
            # print('cia é SOMPO')
            nomenclaturas = df[ref_nom].astype(str).str.extract(r'\((.*?)\)')[0].dropna().unique().tolist()
           
        else:
            nomenclaturas = df[ref_nom].unique().tolist()
        
        # print(f"🔍 Nomenclaturas encontradas na coluna '{ref_nom}': {nomenclaturas}")
        
        conn = DatabaseManager.get_connection()
        try:
            query = """
            SELECT 
                an.nomenclatura_externa, 
                an.unidade as id_unidade,
                tu.nome as nome_unidade,
                tu.corretora_cliente as id_cor_cliente
            FROM tabela_apoionomenclaturas an
            LEFT JOIN tabela_unidades tu ON an.unidade = tu.unidade
            WHERE an.nomenclatura_externa = ANY(%s::text[])
            """
            
            with conn.cursor() as cursor:
                cursor.execute(query, (nomenclaturas,))
                # print("Query executada:", cursor.mogrify(query, (nomenclaturas,)).decode())
                resultados = cursor.fetchall()
            
            mapping_unidade = {}
            mapping_nome = {}
            mapping_cor_cliente = {}
            
            if cia_escolhida == 'Sompo':
                df[ref_nom] = df[ref_nom].astype(str).str.extract(r'\((.*?)\)')[0]
            
            for nomenclatura, id_unidade, nome_unidade, id_cor_cliente in resultados:
                mapping_unidade[nomenclatura] = id_unidade
                mapping_nome[nomenclatura] = nome_unidade
                mapping_cor_cliente[nomenclatura] = id_cor_cliente
            
            df['id_unidade'] = df[ref_nom].map(mapping_unidade)
            df['nome_unidade'] = df[ref_nom].map(mapping_nome)
            df['id_cor_cliente'] = df[ref_nom].map(mapping_cor_cliente)
            df['competencia'] = self.competencia
            
            print("✅ Mapeamento de unidades concluído com sucesso")
            print(df[["id_unidade", "nome_unidade", "id_cor_cliente", "competencia"]].head())

            nomenclaturas_sem_corresp = [n for n in df[ref_nom].unique() if n not in mapping_unidade]
            if nomenclaturas_sem_corresp:
                print(f"⚠️ Nomenclaturas sem correspondência: {nomenclaturas_sem_corresp}")
            self.nomenclaturas_sem_corresp = nomenclaturas_sem_corresp
            
            return df
        finally:
            DatabaseManager.return_connection(conn)
    
    def cons_columns(self, df: pd.DataFrame) -> Tuple[bool, pd.DataFrame, Optional[str], Optional[str], Optional[float]]:
        conn = DatabaseManager.get_connection()
        
        try:
            concatenated_columns = ",".join(df.columns)
            # print(f"🔍 Colunas concatenadas: {concatenated_columns}")
            
            if self.cia_corresp in ['Porto']:
                self.cia_corresp = 'Porto Seguro'

            query_padrao = """
            SELECT cols, premio_valido
            FROM padrao_cols
            WHERE cia = %s
            ORDER BY id DESC
            LIMIT 1
            """
            
            query_fator = """
            SELECT fator_melchiori
            FROM padrao_cols
            WHERE competencia != %s
            AND fator_melchiori IS NOT NULL
            ORDER BY id DESC
            LIMIT 3
            """
            with conn.cursor() as cursor:
                print('query_padrao:', query_padrao)

                cursor.execute(query_padrao, (self.cia_corresp,))
                resultado = cursor.fetchone()
                print('resultado:', resultado)
                
                if not resultado:
                    print("⚠️ Nenhum padrão encontrado para esta competência")
                    return False, df, None, None, None
                
                db_cols, premio = resultado
                print(f"📌 Padrão mais recente - Cols: {db_cols}")
                if premio:
                    print(f"💰 Último prêmio válido: {premio}")

                cursor.execute(query_fator, (self.competencia,))
                fatores = cursor.fetchall()
                
                fator_melchiori_hist = None
                if fatores:
                    valores = [float(f[0]) for f in fatores if f[0] is not None]
                    if valores:
                        fator_melchiori_hist = sum(valores) / len(valores)
                        print(f"📊 Fator Melchiori histórico (média dos últimos 3): {fator_melchiori_hist}")


                db_cols_set = set(db_cols.split(',')) if isinstance(db_cols, str) else set(db_cols)
                concatenated_set = set(concatenated_columns.split(',')) if isinstance(concatenated_columns, str) else set(concatenated_columns)

                if db_cols_set == concatenated_set:
                    print("✅ Colunas correspondem ao padrão (independente de ordem)")
                    premio_exec = premio
                    return True, df, premio, premio_exec, None
                else:
                    print("❌ Colunas NÃO correspondem -- Será realizada análise autônoma para identificação da coluna válida de prêmio")
                    print(f"Recebido: {sorted(concatenated_set)}")
                    print(f"Esperado: {sorted(db_cols_set)}")
                    
                    premio_exec = premio if premio in df.columns else None
                    if premio_exec is None:
                        possible_premio_cols = [col for col in df.columns if 'premio' in col.lower()]
                        if possible_premio_cols:
                            premio_exec = possible_premio_cols[0]
                    
                    return False, df, premio, premio_exec, fator_melchiori_hist
        
        finally:
            DatabaseManager.return_connection(conn)

    def insert_padroes(self, df: pd.DataFrame, premio: str, fator_melchiori: Optional[float] = None):
        cia = self.cia_corresp
        conn = DatabaseManager.get_connection()
        try:
            concatenated_columns = ",".join(df.columns)
            print(f"🔍 Colunas concatenadas: {concatenated_columns}")
            query = """
            INSERT INTO padrao_cols (cols, premio_valido, competencia, fator_melchiori, cia)
            VALUES (%s, %s, %s, %s, %s)
            """
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    concatenated_columns, 
                    premio, 
                    self.competencia,
                    fator_melchiori,
                    self.cia_corresp
                ))
                conn.commit()
                
        finally:
            DatabaseManager.return_connection(conn)

    def analise_autonoma(
        self, 
        df: pd.DataFrame, 
        ult_premio_valido: str, 
        fator_melchiori_hist: Optional[float] = None, 
        premio_db: Optional[Union[str, float]] = None,
        cia_escolhida: Optional[str] = None
    ) -> Tuple[pd.DataFrame, str, str]:
        
        print(self.cia_corresp)
        if df.empty or not isinstance(df, pd.DataFrame):
            raise ValueError("❌ DataFrame inválido ou vazio")

        print('\n=== Análise Autônoma de Prêmio ===')
        
        try:
            fator_float = float(fator_melchiori_hist) if fator_melchiori_hist is not None else None
        except (TypeError, ValueError) as e:
            print(f"⚠️ Erro na conversão do fator Melchiori: {e}")
            fator_float = None

        premio_float = None
        if premio_db is not None:
            try:
                if isinstance(premio_db, (int, float)):
                    premio_float = float(premio_db)
                elif isinstance(premio_db, str):
                    cleaned = premio_db.replace('R$', '').replace('.', '').replace(',', '.').strip()
                    if cleaned.replace('.', '', 1).isdigit() or (cleaned.startswith('-') and cleaned[1:].replace('.', '', 1).isdigit()):
                        premio_float = float(cleaned)
                    else:
                        print(f"⚠️ Valor de prêmio não numérico: {premio_db}")
                else:
                    print(f"⚠️ Tipo de prêmio não suportado: {type(premio_db)}")
            except Exception as e:
                print(f"⚠️ Erro ao converter prêmio: {e}")

        print(f"💰 Fator Melchiori (float): {fator_float}")
        print(f"💰 Valor do Prêmio DB (float): {premio_float}")

        float_columns = [
            col for col in df.columns 
            if pd.api.types.is_numeric_dtype(df[col])  
            and not df[col].dropna().head(50).apply(
                lambda x: float(x).is_integer() if pd.notnull(x) else False
            ).all()
        ]

        if not float_columns:
            raise ValueError("❌ Nenhuma coluna numérica válida encontrada")

        totais_colunas = {
            col: float(df[col].sum()) 
            for col in float_columns
        }
        
        cinco_porcento = {
            col: total * 0.05 
            for col, total in totais_colunas.items()
        }

        print(f"🔍 Colunas numéricas válidas: {float_columns}")
        print(f"💰 Totais por coluna: {totais_colunas}")
        print(f"📊 5% dos totais: {cinco_porcento}")

        try:
            if premio_float is not None and fator_float is not None:
                diferencas = {
                    col: abs(premio_float / cinco_porcento[col] - fator_float)
                    for col in float_columns
                }
                premio_vigente = min(diferencas.items(), key=lambda x: x[1])[0]
                print(f"🎯 Coluna selecionada: {premio_vigente} (Diferença: {min(diferencas.values()):.4f})")
            else:
                premio_vigente = max(float_columns, key=lambda col: df[col].nunique())
                print("ℹ️ Usando fallback (coluna com mais valores únicos)")
        except Exception as e:
            print(f"⚠️ Erro na seleção da coluna: {e}")
            premio_vigente = max(float_columns, key=lambda col: df[col].nunique())

        print('check here cia')
        print(cia_escolhida)
        
        premio_exec = premio_vigente
        if cia_escolhida == 'Porto':
            print('premio para porto')
            premio_exec = 'producao_emitida_atual_para_pgto'
        if cia_escolhida == 'Sompo':
            premio_exec = 'premio'
        

        print(f"\n🔍 Resultados Finais:")
        print(f"Prêmio vigente: {premio_vigente}")
        print(f"Prêmio a executar: {premio_exec}")

        return df, premio_vigente, premio_exec

    def get_and_compare_cias(self) -> Tuple[List[str], List[str], List[str], Optional[str]]:
        if not self.cia_corresp:
            print("⚠️ Variável 'cia_corresp' não encontrada")
            return [], [], [], None
        
        try:
            config_path = os.path.join(os.getcwd(), "config.json")
            with open(config_path, "r", encoding="utf-8") as f:
                try:
                    config_data = json.load(f)
                    cia_escolhida = config_data.get("cia_corresp", "")
                    competencia_escolhida = config_data.get("competencia", "")
                    
                    self.cias_corresp_list = cia_escolhida
                    self.competencia = competencia_escolhida
                    
                    print(f"🔍 CIA escolhida: {cia_escolhida}")
                    print(f"📅 Competência: {competencia_escolhida}")
                    
                except json.JSONDecodeError:
                    print("❌ Erro: config.json está mal formatado ou vazio")
                    return [], [], [], None
        
        except FileNotFoundError:
            print("❌ Erro: arquivo config.json não encontrado")
            return [], [], [], None
        
        cias_corresp_list = [cia.strip() for cia in self.cia_corresp.split(",") if cia.strip()]
        print(f"🔍 Cias do Extrator: {cias_corresp_list}")
        
        conn = DatabaseManager.get_connection()
        try:
            with conn.cursor() as cursor:

                cias_corresp_list = cias_corresp_list[0]

                if cias_corresp_list == 'Porto':
                    cias_corresp_list = ['Porto Seguro']
                    cia_escolhida = 'Porto Seguro'
                
                if cias_corresp_list == 'Ezze':
                    cias_corresp_list = ['Ezze Seguros']
                    cia_escolhida = 'Ezze Seguros'
                
                if cias_corresp_list == 'Tokio':
                    cias_corresp_list = ['Tokio Marine']
                    cia_escolhida = 'Tokio Marine'
                
                cursor.execute("SELECT distinct seg_nome_correto from tabela_correcao_seguradora")
                cias_db = [row[0].strip() for row in cursor.fetchall() if row[0].strip()]

                # print(f"🔍 Cias do Banco de Dados: {cias_db}")
                
                existing_cias = [cia for cia in cias_db if cia in cias_corresp_list]
                non_existing_cias = [cia for cia in cias_corresp_list if cia not in cias_db]
                
                if not existing_cias:
                    print("⚠️ Nenhuma CIA existente encontrada")
                    return existing_cias, non_existing_cias, cias_corresp_list, None
                
                cursor.execute("""
                    SELECT id_seguradora_quiver, seg_nome_correto
                    FROM tabela_correcao_seguradora
                    WHERE seg_nome_correto IN %s
                """, (tuple(existing_cias),))
                
                ids_cias = {row[1]: row[0] for row in cursor.fetchall()}
                # print(f"🔍 IDs das Cias: {ids_cias}")
                
                id_cia = str(ids_cias.get(cia_escolhida, None))
                print(f"📍 ID CIA selecionada ({cia_escolhida}): {id_cia}")
                
                return existing_cias, non_existing_cias, cias_corresp_list, id_cia
        finally:
            DatabaseManager.return_connection(conn)

    def import_main(self, conn,df_filtered, table_name, ordered_cols, ordered_cols_escaped):
        cursor = None
        success = False
        

        try:
            cursor = conn.cursor()
            df_filled = df_filtered.replace([pd.NA, pd.NaT, np.nan], None)
            competencia = df_filled['competencia'].iloc[0] if 'competencia' in df_filled.columns else self.competencia
            
            for col in df_filled.columns:
                if pd.api.types.is_datetime64_any_dtype(df_filled[col]):
                    df_filled[col] = pd.to_datetime(df_filled[col], errors='coerce').replace({pd.NaT: None})
            
            sql = f"INSERT INTO {table_name} ({','.join(ordered_cols_escaped)}) VALUES ({','.join(['%s']*len(ordered_cols))})"
            print(sql)
            
            with tqdm(total=len(df_filled), desc=f"Importando {table_name}") as pbar:
                execute_batch(cursor, sql, [tuple(x) for x in df_filled.values], page_size=1000)
                pbar.update(len(df_filled))
            
            conn.commit()
            print(f"✅ {len(df_filled)} linhas importadas em {table_name}")
            success = True

            
        except Exception as e:
            conn.rollback()
            print(f"❌ Erro na importação de {table_name}: {e}")
            success = False
            
        finally:
            if cursor:
                cursor.close()
        
        return success
    