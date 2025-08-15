import os
import pandas as pd
from pyxlsb import open_workbook


class ChubbHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        print('starting ChubbHandler.treat()')
        files = [f for f in os.listdir(folder_path) if f.lower().endswith('.xlsb')]
        print('📂 Chubb - arquivos encontrados:', files)
        if not files:
            print("❌ Nenhum arquivo encontrado para Chubb")
            return pd.DataFrame()
        try:
            file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
            file_path = os.path.join(folder_path, file)
            print(f"📂 Chubb - arquivo selecionado: {file}")

            is_xlsb = file.lower().endswith('.xlsb')
            sheet_name = "Valor por Corretor"

            def find_header():
                if is_xlsb:
                    try:
                        with open_workbook(file_path) as wb:
                            with wb.get_sheet(sheet_name) as sheet:
                                for row_idx, row in enumerate(sheet.rows()):
                                    if len(row) > 1 and str(row[1].v).strip() == 'Corretor':
                                        return row_idx
                    except Exception as e:
                        print(f"⚠️ Erro ao ler XLSB: {str(e)}")
                        return None
                else:
                    for row in range(30):
                        try:
                            df_temp = pd.read_excel(
                                file_path,
                                sheet_name=sheet_name,
                                header=None,
                                nrows=1,
                                skiprows=row,
                                usecols="B"
                            )
                            if not df_temp.empty and str(df_temp.iloc[0, 0]).strip() == 'Corretor':
                                return row
                        except:\
                            continue
                return None

            header_row = find_header()
            
            if header_row is None:
                print("❌ Cabeçalho 'Corretor' não encontrado")
                return pd.DataFrame()

            print(f"✅ Cabeçalho encontrado na linha {header_row + 1}")

            if is_xlsb:
                try:
                    with open_workbook(file_path) as wb:
                        with wb.get_sheet(sheet_name) as sheet:
                            data = []
                            for row in sheet.rows():
                                if len(row) > 1:
                                    data.append([cell.v for cell in row[1:9]])
                    
                    headers = [str(cell).strip() for cell in data[header_row]]
                    df = pd.DataFrame(data[header_row+1:], columns=headers)
                except Exception as e:
                    print(f"❌ Erro ao ler dados XLSB: {str(e)}")
                    return pd.DataFrame()
            else:
                df = pd.read_excel(
                    file_path,
                    sheet_name=sheet_name,
                    header=header_row,
                    usecols="B:D"
                )
            if df.empty:
                print("❌ DataFrame vazio após leitura")
                return pd.DataFrame()
            if 'Corretor' not in df.columns:
                print("❌ Coluna 'Corretor' não encontrada")
                return pd.DataFrame()
            df = df[df['Corretor'].notna() | 
                   (df['Corretor'] != 'Total') |
                   (df['Corretor'] != 'Total Geral') |
                   (df['Corretor'] != '')].copy()
            
            df = df[~df['Corretor'].isin([None, '', 'Total', 'Total Geral']) & df['Corretor'].notna()]

            
            if df.empty:
                print("❌ Nenhum dado válido após filtragem")
                return pd.DataFrame()
            df['origem_arquivo'] = file
            
            print(f"✅ Tratamento concluído. Shape: {df.shape}")
            
            return df

        except Exception as e:
            print(f"❌ Erro geral no processamento: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            if fator_melchiori is not None:
                print('check_columns:', df.columns)
                fdb = df[coluna] / df['premio_liquido']
                df['premio_rec'] = df['premio_liquido'] * (fdb / 0.05) * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.02
                df['valor_vi'] = df['premio_rec'] * 0.006
                df['valor_as'] = df['premio_rec'] * 0.024
                print(df[[coluna, 'premio_rec','valor_cv','valor_vi','valor_as']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
            
    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        
        try:
            df = df[df['corretor'].astype(str).str.strip() != 'Total Geral']
            print('cade a coluna ========== staged')
            print(df.columns)
            print('SOMANDO PREMIO CHUBB')
            print(coluna)
            print('soma inicial:', df[coluna].sum())
            
            premio_total_relatorio = round(df[coluna].sum() * fator, 2)
            print(df.tail())
            return premio_total_relatorio, df
        except Exception as e:
            print(f"❌ Erro ao converter para Decimal: {e}")
            return {}

