import os
import pandas as pd

class MapfreHandler:

    def treat(self, folder_path: str) -> pd.DataFrame: 
            print(f"📂 Verificando arquivos em: {folder_path}")
            files = [
                f for f in os.listdir(folder_path) 
                if f.lower().endswith(('.xls', '.xlsx')) and 
                ('Apuração' in f)
            ]
            
            if not files:
                print("❌ Nenhum arquivo encontrado contendo 'Apuração'")
                return pd.DataFrame()

            print(f"📂 Arquivos encontrados: {files}")
            file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
            file_path = os.path.join(folder_path, file)
            print(f"📂 Arquivo selecionado: {file}")

            try:
                preview = pd.read_excel(file_path, sheet_name='Aberto corretor', header=None)
                print("🔍 Pré-visualização lida com sucesso.")

                col_a = preview.iloc[:, 0].astype(str)

                header_row = None
                for i, value in enumerate(col_a):
                    if 'prêmios emitidos como valores' in value.lower():
                        header_row = i
                        break

                if header_row is None:
                    print("❌ Texto 'Prêmios Emitidos como valores' não encontrado na coluna A")
                    return pd.DataFrame()

                print(f"🔍 Cabeçalho localizado na linha: {header_row + 1} (índice {header_row})")

                df = pd.read_excel(file_path, sheet_name='Aberto corretor', header=header_row)
                df['origem_arquivo'] = file  
                print(f"✅ DataFrame carregado ({df.shape[0]} linhas)")
                print('=========================== Validação DF abaixo ============================')
                return df

            except Exception as e:
                print(f"❌ Erro ao ler {file}: {e}")
                return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):

        cols_base = ['empresarial', 'residencial', 'residencial_simplificado', 'rc_profissional']
        df['premio_base'] = df[cols_base].sum(axis=1, skipna=True)
        premio_exec = 'premio_base'
        coluna = premio_exec
         
        if coluna in df.columns:
            fator_melchiori = 0.965001
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.024
                df['valor_vi'] = df['premio_rec'] * 0.001
                print(df[[coluna, 'premio_rec','valor_cv','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")