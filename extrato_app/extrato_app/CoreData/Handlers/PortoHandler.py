import os
import pandas as pd
import re
from unidecode import unidecode

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
