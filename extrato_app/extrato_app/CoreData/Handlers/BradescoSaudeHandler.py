import os
import pandas as pd
from decimal import Decimal

class BradescoSaudeHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        print(f"📂 Verificando arquivos em: {folder_path}")
        files = [
            f for f in os.listdir(folder_path) 
            if f.lower().endswith(('.xls', '.xlsx')) and 
            ('Producao' in f or 'Consolidado' in f)
        ]
        
        if not files:
            print("❌ Nenhum arquivo encontrado contendo 'Producao' ou 'Consolidado'")
            return pd.DataFrame()

        print(f"📂 Arquivos encontrados: {files}")
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Arquivo selecionado: {file}")

        try:
            df = pd.read_excel(file_path, sheet_name=0)
            df['origem_arquivo'] = file  
            print(f"✅ DataFrame carregado ({df.shape[0]} linhas)")
            return df
        except Exception as e:
            print(f"❌ Erro ao ler {file}: {e}")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db: Decimal):

        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            if fator_melchiori is not None:
                
                fator_melchiori = float(0.938500)
                print(fator_melchiori)
                print('Processando Bradesco Saúde... IDENTIFICAR AQUI A APLICAÇÃO DO FATOR MELCHIORI FIXO EM 0.9385')
                print('premio_db:', premio_db)
                
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.01

                fator_as = (premio_db - Decimal(str(df['valor_cv'].sum()))) / Decimal(str(df['premio_rec'].sum()))
                df['valor_as'] = df['premio_rec'].apply(lambda x: Decimal(str(x)) * fator_as)



                print(df[[coluna, 'premio_rec','valor_cv','valor_as']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
