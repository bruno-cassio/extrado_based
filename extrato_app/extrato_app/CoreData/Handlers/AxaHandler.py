import os
import pandas as pd

class AxaHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:

        try:
            files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx'))]
            if not files:
                print("❌ Nenhum arquivo encontrado para Axa")
                return pd.DataFrame()

            file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
            file_path = os.path.join(folder_path, file)
            print(f"📂 Axa - arquivo selecionado: {file}")

            excel_file = pd.ExcelFile(file_path)
            
            df_raw = pd.read_excel(excel_file, sheet_name=0, header=None)
            aba = sheet_name = excel_file.sheet_names[0]
            print(f"ℹ️ Aba selecionada: {aba}")
            header_row = None
            for idx, row in df_raw.iterrows():
                if row.count() > len(row) // 2:
                    header_row = idx
                    break
                    
            if header_row is None:
                print("❌ Não foi possível encontrar o cabeçalho - arquivo pode estar vazio")
                return pd.DataFrame()

            df = pd.read_excel(
                excel_file,
                sheet_name=0,
                skiprows=header_row
            )
            
            df.dropna(how='all', inplace=True)
            df.reset_index(drop=True, inplace=True)
            df['origem_arquivo'] = file
            print(f"✅ Dados processados. Shape final: {df.shape}")
            print(f"ℹ️ Cabeçalho encontrado na linha: {header_row + 1}")
            
            print("ℹ️ Cabeçalho identificado:", df.columns.tolist())
            
            
            return df

        except Exception as e:
            print(f"❌ Erro ao processar arquivo da Axa: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        
        df = df[df['pagamento_convertido'] == df['competencia']]
        print('validação se somente competencia vigente')
        print(df['pagamento_convertido'].unique())
        
        if coluna in df.columns:
            if fator_melchiori is not None:
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.02
                df['valor_as'] = df['premio_rec'] * 0.024
                df['valor_vi'] = df['premio_rec'] * 0.006
                print(df[[coluna, 'premio_rec','valor_cv','valor_as','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
