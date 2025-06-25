import os
import pandas as pd

class TokioHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        print(f"📂 Verificando arquivos em: {folder_path}")

        files = [
            f for f in os.listdir(folder_path)
            
            if f.lower().endswith(('.xls', '.xlsx', '.xlsb')) and 'apuração' in f.lower()
        ]
        print('check abaixo')
        print(files)
        if not files:
            print("❌ Nenhum arquivo encontrado contendo 'apuração'")
            return pd.DataFrame()

        print(f"📂 Arquivos encontrados: {files}")
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Arquivo selecionado: {file}")

        try:
            df = pd.read_excel(file_path, sheet_name='Produção')
            df['origem_arquivo'] = file  
            print(f"✅ DataFrame carregado ({df.shape[0]} linhas)")
            return df
        except Exception as e:
            print(f"❌ Erro ao ler {file}: {e}")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            fator_melchiori = 0.938500
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.04
                df['valor_as'] = df['premio_rec'] * 0.01 * 0.27868759
                df['valor_vi'] = df['premio_rec'] * 0.01 * 0.72131241
                print(df[[coluna, 'premio_rec','valor_cv','valor_as','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
