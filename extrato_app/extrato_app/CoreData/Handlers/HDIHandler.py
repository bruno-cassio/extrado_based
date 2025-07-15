import os
import pandas as pd
import json
import re
import time

class HDIHandler:
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
                sheet_pattern = f"{month_name} {year_str}"
                print(f"🔍 Procurando aba: {sheet_pattern}")

        except Exception as e:
            print(f"❌ Erro ao ler config.json: {str(e)}")
            return pd.DataFrame()

        
        start = time.perf_counter()
        
        files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx'))]
        if not files:
            print("❌ Nenhum arquivo encontrado para HDI")
            return pd.DataFrame()
        
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 HDI - arquivo selecionado: {file}")

        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None, header=None)
            
            target_sheet = None
            
            if competencia:
                for sheet_name in all_sheets.keys():
                    if sheet_pattern in sheet_name.upper():
                        target_sheet = sheet_name
                        break
            
            if not target_sheet:
                target_sheet = list(all_sheets.keys())[-1]
                print(f"⚠️ Aba da competência não encontrada. Usando última aba: {target_sheet}")
            else:
                print(f"📑 Aba selecionada: {target_sheet} (correspondente à competência {competencia})")

            df_raw = all_sheets[target_sheet]

            header_row_index = None
            for i, row in df_raw.iterrows():
                if any(str(cell).strip().lower() == "cnpj filho" for cell in row.values):
                    header_row_index = i
                    break

            if header_row_index is None:
                print("❌ Cabeçalho com 'CNPJ Filho' não encontrado.")
                return pd.DataFrame()

            df = pd.read_excel(
                file_path,
                sheet_name=target_sheet,
                header=header_row_index,
                engine='openpyxl'
            )
            
            df = df.dropna(how='all')
            df = df.dropna(axis=1, how='all')
            df['origem_arquivo'] = file
            df['competencia'] = competencia

            df.columns = [str(col).strip().lower() for col in df.columns]

            coluna_competencia = f"prod._{month_name.lower()}/{year_str}"
            coluna_competencia_normalizada = normaliza(coluna_competencia)

            matching_indices = [
                idx for idx, col in enumerate(df.columns)
                if normaliza(col) == coluna_competencia_normalizada
            ]

            print("🔍 Índices de colunas que batem com a competência:")
            for i in matching_indices:
                print(f"  → [{i}]: {df.columns[i]}")

            if len(matching_indices) >= 2:
                second_idx = matching_indices[1]
                new_columns = list(df.columns)
                new_columns[second_idx] = 'premio_target'
                df.columns = new_columns
                print(f"🎯 Segunda ocorrência da coluna renomeada para 'premio_target': {second_idx}")
                print(df.columns)
            else:
                print(f"⚠️ Menos de duas ocorrências da coluna '{coluna_competencia}' encontradas.")


            end = time.perf_counter()
            print(f"⏱️ Tempo de leitura otimizada: {end - start:.2f}s")



            return df

        except Exception as e:
            print(f"❌ Erro ao ler o arquivo: {str(e)}")
            return pd.DataFrame()


    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.016
                df['valor_vi'] = df['premio_rec'] * 0.006
                print(df[[coluna, 'premio_rec','valor_cv','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")