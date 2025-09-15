import os
import pandas as pd
import re
from unidecode import unidecode
import datetime
import json
from extrato_app.CoreData.ds4 import parse_meses_opt, obter_mes_ano
from extrato_app.CoreData.dba import DBA
from extrato_app.CoreData.IncentivoMain import (
    norm_str,
    montar_pasta_incentivo,
    encontrar_arquivo,
    get_ref_nom,
)
from extrato_app.CoreData.dba import DBA



class PortoHandler:


####query consulta consolidado por unidade 
# SELECT 
#     nome_unidade,
#     SUM(premio_rec) AS total_premio_rec,
#     SUM(valor_cv) AS total_cv,
#     SUM(valor_vi) AS total_vi
# FROM 
#     cont_prod_porto
# GROUP BY 
#     nome_unidade
# ORDER BY 
#     nome_unidade

    def treat(self, folder_path: str) -> pd.DataFrame:

        def normalizar_colunas(df):
            """
            Normaliza colunas com tratamento robusto
            """
            new_columns = (
                df.columns.astype(str)
                .str.strip()
                .str.replace(r'[\r\n]+', ' ', regex=True)
                .str.replace(r'\s+', '_', regex=True)
                .str.lower()
            )
            
            new_columns = [unidecode(col) for col in new_columns]
            
            column_mapping = {
                'producao_emitida_anterior': 'producao_emitida_anterior',
                'producao_emitida_atual_para_pgto': 'producao_emitida_atual_para_pgto',
                'p._emitida': 'p_emitida',
                'p._emitida.1': 'p_emitida_1',
                'p._emitida.2': 'p_emitida_2',
                'ganho_por_corretor': 'comissao_corretor',
                'susep_producao': 'codigo_susep'
            }
            
            new_columns = [column_mapping.get(col, col) for col in new_columns]
            
            df.columns = new_columns
            return df

        files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx')) and "gc autos e re" in f.lower()]
        if not files:
            print("❌ Nenhum arquivo da Porto encontrado com 'GC Autos e RE'")
            return pd.DataFrame()
        
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Porto - arquivo selecionado: {file}")

        xls = pd.ExcelFile(file_path)
        sheet_names = ['Consolidado Auto Ind', 'Residencia_Empresa_Condomínio']
        dfs = []

        for sheet_name in sheet_names:
            print(f"\n🔄 Processando aba: {sheet_name}")
            if sheet_name not in xls.sheet_names:
                print(f"❌ Aba '{sheet_name}' não encontrada")
                continue

            header_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            header_row = None
            for idx, row in header_df.iterrows():
                if any('nome corretor' in str(cell).lower() for cell in row[:5]):
                    header_row = idx
                    print(f"✅ Cabeçalho encontrado na linha {header_row + 1}")
                    break

            if header_row is None:
                print(f"❌ Cabeçalho não encontrado na aba '{sheet_name}'")
                continue

            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=header_row,
                engine='openpyxl'
            ).dropna(how='all')
            
            df = normalizar_colunas(df)
            
            df['origem_arquivo'] = file
            df['origem_aba'] = sheet_name
            
            dfs.append(df)
            print(f"📊 Dados processados: {df.shape[0]} linhas")

        if not dfs:
            print("❌ Nenhum dado válido para processar")
            return pd.DataFrame()

        try:
            all_columns = set()
            for df in dfs:
                all_columns.update(df.columns)
            processed_dfs = []
            for df in dfs:
                missing_cols = all_columns - set(df.columns)
                for col in missing_cols:
                    df[col] = None
                processed_dfs.append(df[list(all_columns)])
            
            final_df = pd.concat(processed_dfs, ignore_index=True)
            
            # print(f"\n✅ Concatenação concluída - Shape final: {final_df.shape}")
            # print("🔍 Colunas disponíveis:")
            # print(final_df.columns.tolist())
            
            df = final_df.copy()

            print(f"\n✅ Concatenação concluída - Shape final: {final_df.shape}")
            print("🔍 Colunas disponíveis:")
            print(df.columns.tolist())

            return df
            
        except Exception as e:
            print(f"❌ Erro na concatenação: {str(e)}")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.01
                df['valor_vi'] = df['premio_rec'] * 0.01
                print(df[[coluna, 'premio_rec','valor_cv','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")

    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        
        coluna = coluna.replace(' ', '_')
        print('============================ checagenzinha aqui ============================')
        print('PORTO aqui')
        print(df.columns)
        print(f"📋 Colunas atuais no df: {df.columns.tolist()}")
        print(f"🔎 Procurando a coluna: {coluna}")
        duplicadas = df.columns[df.columns.duplicated()].tolist()
        if duplicadas:
            print(f"🚨 Atenção: Colunas duplicadas detectadas: {duplicadas}")
        
        df = df[df['nome_corretor'].notna() & (df['nome_corretor'].astype(str).str.strip() != '') & (df['nome_corretor'].astype(str).str.strip() != 'Total')]
        
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        if not os.path.exists(downloads_dir):
            os.makedirs(downloads_dir)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        xls_filename = f"{table_name}_{timestamp}.xlsx"
        xls_path = os.path.join(downloads_dir, xls_filename)

        df.to_excel(xls_path, index=False)
        print(f"✅ Arquivo salvo em: {xls_path}")
        
        print('prefire')
        df = df[df[coluna].notna()]
        print('inside')
        print(df[coluna].tail())
        print('inner')
        
        premio_total_relatorio = round(
            (df[
                (df['nome_corretor'].astype(str).str.strip() != 'Total') &
                (df['nome_corretor'].astype(str).str.strip() != '')
            ][coluna].sum() * fator), 2
        )

        # premio_total_relatorio = round(df[coluna].sum() * fator, 2)
        print(f"Total de 'premio' para Porto: {premio_total_relatorio}")
        print('================================================== passed // retornando ==================================================')
        return premio_total_relatorio, df


    def read_incentivo(self, competencia: str) -> pd.DataFrame:
        """
        Lê o incentivo da Porto:
        - Arquivo contendo 'GC Autos e RE'
        - Abas 'Consolidado Auto Ind' e 'Residencia_Empresa_Condomínio'
        - Cabeçalho dinâmico ('Nome Corretor')
        - Unifica dados
        - Aplica mapeamento de unidade
        - Consolida incentivos
        """
        pasta = montar_pasta_incentivo("Porto", competencia)
        if not pasta:
            return pd.DataFrame()

        nome_arquivo = encontrar_arquivo(pasta, ["gc autos e re"])
        if not nome_arquivo:
            return pd.DataFrame()
        file_path = os.path.join(pasta, nome_arquivo)

        try:
            engine = "xlrd" if file_path.endswith(".xls") else "openpyxl"
            excel_obj = pd.ExcelFile(file_path, engine=engine)
            abas = excel_obj.sheet_names

            abas_alvo = [
                s for s in abas
                if norm_str(s) in [norm_str("Consolidado Auto Ind"), norm_str("Residencia_Empresa_Condomínio")]
            ]
            if not abas_alvo:
                print("❌ Nenhuma aba alvo encontrada no arquivo")
                return pd.DataFrame()

            df_list = []
            for aba in abas_alvo:
                df_raw = pd.read_excel(file_path, sheet_name=aba, header=None, engine=engine)
                header_idx = next(
                    (i for i, row in df_raw.iterrows() if any(str(c).strip().lower() == "nome corretor" for c in row)),
                    None
                )
                if header_idx is None:
                    continue

                df = pd.read_excel(file_path, sheet_name=aba, header=header_idx, engine=engine)
                df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
                df.columns = [str(c).strip().lower() for c in df.columns]
                df['aba_origem'] = aba
                df_list.append(df)

            if not df_list:
                return pd.DataFrame()

            df_final = pd.concat(df_list, ignore_index=True)
            df_final["nome_unidade"] = df_final["nome corretor"]
            df_final["ganho por corretor"] = pd.to_numeric(df_final["ganho por corretor"], errors="coerce").fillna(0)

            df_final, ref_nom = get_ref_nom(df_final, ["nome corretor"])
            try:
                dba = DBA()
                df_final = dba.add_id_unidade_from_database(df_final, "Porto")
            except Exception as e:
                print(f"⚠️ Falha no mapeamento de unidades: {e}")

            group_keys = ['id_unidade', 'id_cor_cliente', 'competencia', 'nome_unidade']
            df_grp = (
                df_final.groupby(group_keys, dropna=False)['ganho por corretor']
                .sum()
                .reset_index()
                .rename(columns={'ganho por corretor': 'valor_incentivo'})
            )
            df_grp['tipo_fonte'] = 'incentivo'
            df_grp['origem_arquivo'] = nome_arquivo
            df_grp['cia'] = 'Porto'

            print(f"✅ Consolidado Porto ({df_grp.shape[0]} linhas)")
            return df_grp

        except Exception as e:
            print(f"❌ Erro ao ler incentivo Porto: {e}")
            return pd.DataFrame()
        
        