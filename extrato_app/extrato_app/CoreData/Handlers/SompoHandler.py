import os
import pandas as pd
import json
import re
import getpass

class SompoHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        def normaliza(texto):
            return re.sub(r'[^a-z0-9]', '', texto.lower())

        try:
            with open('config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
            competencia = config.get('competencia', '')
            print(f"🔧 Competência configurada: {competencia}")
            
            if competencia:
                month_map = {
                    "01": "JAN", "02": "FEV", "03": "MAR", "04": "ABR",
                    "05": "MAI", "06": "JUN", "07": "JUL", "08": "AGO",
                    "09": "SET", "10": "OUT", "11": "NOV", "12": "DEZ"
                }
                month_str = competencia.split('-')[0].zfill(2)
                year_str = competencia.split('-')[1][-2:]
                month_name = month_map.get(month_str, "")
                sheet_name = f"{month_name}{year_str}"
                print(f"🔍 Procurando aba: {sheet_name}")

        except Exception as e:
            print(f"❌ Erro ao ler config.json: {str(e)}")
            return pd.DataFrame()

        files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx'))]
        if not files:
            print("❌ Nenhum arquivo encontrado para HDI")
            return pd.DataFrame()
        
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 SOMPO - arquivo selecionado: {file}")

        try:
            xls = pd.ExcelFile(file_path)
            target_sheet = None
            
            if competencia:
                for sheet in xls.sheet_names:
                    if sheet.upper() == sheet_name:
                        target_sheet = sheet
                        break
                if not target_sheet:
                    for sheet in xls.sheet_names:
                        if sheet_name[:3].upper() in sheet.upper():
                            target_sheet = sheet
                            break
            
            if not target_sheet:
                target_sheet = xls.sheet_names[-1]
                print(f"⚠️ Aba da competência não encontrada. Usando última aba: {target_sheet}")
            else:
                print(f"📑 Aba selecionada: {target_sheet} (correspondente à competência {competencia})")

            header_row_index = None
            for i in range(50):  
                df_temp = pd.read_excel(
                    file_path,
                    sheet_name=target_sheet,
                    header=None,
                    nrows=1,
                    skiprows=i,
                    usecols=[0]
                )
                
                if len(df_temp.iloc[0]) > 0:
                    cell_value = str(df_temp.iloc[0, 0]).strip().upper()
                    if cell_value == "CORRETOR":
                        header_row_index = i
                        print(f"✅ Cabeçalho encontrado na linha {i+1} - Valor exato: '{df_temp.iloc[0,0]}'")
                        break

            if header_row_index is None:
                print("❌ Cabeçalho exato 'CORRETOR' não encontrado na coluna A nas primeiras 50 linhas.")
                return pd.DataFrame()

            

            print(' ============================================================================_BASED ABAIXO_============================================================================')

            df_based_a = pd.read_excel(
                file_path,
                sheet_name=target_sheet,
                header=header_row_index-1 ,
                engine='openpyxl'
            )
            df_based = pd.read_excel(
                file_path,
                sheet_name=target_sheet,
                header=header_row_index,
                engine='openpyxl'
            )
            print(df_based_a.columns)
            print(df_based.columns)
            print(df_based.shape)
            print(df_based['CORRETOR'].notna().sum())

            print(' ============================================================================ BASED acima ===============================================================================')
            
            df = pd.read_excel(
                file_path,
                sheet_name=target_sheet,
                header=header_row_index,
                engine='openpyxl'
            )

            if df.empty:
                print("❌ DataFrame vazio após leitura.")
                return pd.DataFrame()
            df.columns = [str(col).strip().lower() for col in df.columns]
            print(f"🔍 Cabeçalho detectado na linha {header_row_index + 1}")
            print(f"🧠 Colunas detectadas: {df.columns.tolist()}")
            
            df = df.dropna(how='all').dropna(axis=1, how='all')
            df.columns = [str(col).strip() for col in df.columns]
            
            if df.empty:
                print("❌ DataFrame vazio após limpeza.")
                return pd.DataFrame()

            df['origem_arquivo'] = file
            df['competencia'] = competencia

            print('============================================================')
            print('VALIDAÇÃO COLUNA RETIDO TEC AQUI')
            print(df.columns)
            print('============================================================')

            coluna_competencia = 'prêmio retido técnico.1'
            coluna_competencia_normalizada = normaliza(coluna_competencia)

            colunas_renomear = {}
            for col in df.columns:
                col_normalizada = normaliza(str(col))
                if col_normalizada == coluna_competencia_normalizada:
                    if 'premio' not in colunas_renomear:
                        colunas_renomear[col] = 'premio'
                    else:
                        colunas_renomear[col] = 'premio_target'

            if colunas_renomear:
                df = df.rename(columns=colunas_renomear)
                print(f"✅ Colunas renomeadas: {colunas_renomear}")
            else:
                print(f"⚠️ Coluna '{coluna_competencia}' não encontrada")

            print('================================= VALIDAÇÃO DF TRATADO ABAIXO=================================')

            df_fn = df[df['cresc'].astype(str).str.upper() == 'SIM']
            primeira_coluna = df_fn.columns[0]
            df_fn = df_fn[
                (~df_fn[primeira_coluna].astype(str).str.strip().isin(['', 'Total Geral'])) &
                (df_fn[primeira_coluna].notna())
            ]

            df = df_fn

            return df

        except Exception as e:
            print(f"❌ Erro ao processar arquivo: {str(e)}")
            import traceback

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        print(df.shape)
        
        fator_melchiori = 0.965000
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.016
                df['valor_as'] = df['premio_rec'] * 0.01
                print(df[[coluna, 'premio_rec','valor_cv','valor_as']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
            
    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        try:
            premio_total_relatorio = round(df[coluna].sum() * fator, 2)

            return premio_total_relatorio,df
        except Exception as e:
            print(f"❌ Erro ao converter para Decimal: {e}")
            return {}
        