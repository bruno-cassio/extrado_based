import os
import pandas as pd

class SuhaiHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx'))]
        if not files:
            print("❌ Nenhum arquivo encontrado para Suhai")
            return pd.DataFrame()

        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Suhai - arquivo selecionado: {file}")
        df = pd.read_excel(file_path, sheet_name=0)
        df['origem_arquivo'] = file
        print(f"DEBUG: treat retornou DataFrame com shape {df.shape}")

        return df

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'vlr_tarifario'
        if coluna in df.columns:
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.03
                df['valor_as'] = df['premio_rec'] * 0.014
                df['valor_vi'] = df['premio_rec'] * 0.006
                print(df[[coluna, 'premio_rec','valor_cv','valor_as','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
            
    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        try:
            premio_total_relatorio = round(df[coluna].sum() * fator, 2)
            return premio_total_relatorio, df
        except Exception as e:
            print(f"❌ Erro ao converter para Decimal: {e}")
            return {}