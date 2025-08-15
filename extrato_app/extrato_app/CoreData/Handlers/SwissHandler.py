import os
import pandas as pd
from io import BytesIO
import msoffcrypto


class SwissHandler:

    def ler_excel_com_senha(self, caminho_arquivo: str, senha: str, sheet_name=None) -> pd.DataFrame:

        with open(caminho_arquivo, 'rb') as file:
            office_file = msoffcrypto.OfficeFile(file)
            office_file.load_key(password=senha)

            buffer = BytesIO()
            office_file.decrypt(buffer)

            buffer.seek(0)
            df = pd.read_excel(buffer, sheet_name=sheet_name)
            return df

    def treat(self, folder_path: str) -> pd.DataFrame:
        print(f"📂 Verificando arquivos em: {folder_path}")

        files = [
            f for f in os.listdir(folder_path)
            if f.lower().endswith(('.xls', '.xlsx', '.xlsb')) and 'swiss' in f.lower()
        ]
        print('check abaixo')
        print(files)
        if not files:
            print("❌ Nenhum arquivo encontrado contendo 'swiss'")
            return pd.DataFrame()

        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Arquivo selecionado: {file}")

        try:
            df_tmp = self.ler_excel_com_senha(file_path, senha='SWISS', sheet_name=0)
            first_data_row = df_tmp.notna().any(axis=1).idxmax()
            df_tmp.columns = df_tmp.iloc[first_data_row]
            df = df_tmp.iloc[first_data_row + 1:].reset_index(drop=True)
            df['origem_arquivo'] = file
            
            if 'Nome Assessoria' in df.columns:
                df = df[df['Nome Assessoria'].astype(str).str.contains('GRANDE CORRETORA', case=False, na=False)]
            else:
                print("⚠️ Coluna 'Nome Assessoria' não encontrada no arquivo.")
                
            print(f"✅ DataFrame carregado ({df.shape[0]} linhas)")
            return df

        except Exception as e:
            print(f"❌ Erro ao ler {file}: {e}")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            fator_melchiori = 0.9385
            if fator_melchiori is not None:

                df['premio_rec'] = df['soma_de_valor_liquido_da_parcela'] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.015
                df['valor_as'] = df['premio_rec'] * 0.009
                df['valor_vi'] = df['premio_rec'] * 0.006
                print(df[['soma_de_valor_liquido_da_parcela','premio_rec', 'valor_cv', 'valor_vi']].head())

            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
            
            
            
    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        try:
            premio_total_relatorio = round(df['soma_de_valor_liquido_da_parcela'].sum() * fator, 2)
            print('premio total de SWISS ->')
            print(premio_total_relatorio)
            print('====================== acima premio da swiss ======================')
            return premio_total_relatorio,df
        except Exception as e:
            print(f"❌ Erro ao calcular prêmio da Swiss: {e}")
            return {}