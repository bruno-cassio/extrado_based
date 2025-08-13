import os
import pandas as pd

class ZurichHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx'))]
        if not files:
            print("❌ Nenhum arquivo encontrado para Zurich")
            return pd.DataFrame()

        try:
            file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
            file_path = os.path.join(folder_path, file)
            print(f"📂 Zurich - arquivo selecionado: {file}")
            
            xl = pd.ExcelFile(file_path)
            if 'Produção por LoB vs Corretor' not in xl.sheet_names:
                print("❌ Aba 'Produção por LoB vs Corretor' não encontrada no arquivo")
                return pd.DataFrame()
            print('✅ Aba "Produção por LoB vs Corretor" encontrada')
            header_row = None
            for row in range(30):
                try:
                    df_temp = pd.read_excel(
                        file_path,
                        sheet_name='Produção por LoB vs Corretor',
                        header=None,
                        nrows=1,
                        skiprows=row,
                        usecols="B"
                    )
                    if not df_temp.empty and df_temp.iloc[0, 0] == 'CNPJ ':
                        header_row = row
                        print(f"✅ Cabeçalho 'CNPJ' encontrado na linha {row + 1}")
                        break
                except Exception as e:
                    print(f"⚠️ Erro ao ler linha {row}: {str(e)}")
                    continue
            
            if header_row is None:
                print("❌ Cabeçalho 'CNPJ' não encontrado nas primeiras 20 linhas da coluna B")
                return pd.DataFrame()
            
            print(header_row)
            df = pd.read_excel(
                
                file_path,
                sheet_name='Produção por LoB vs Corretor',
                header=header_row,
                usecols="B:I"
            )
            
            print(df.head())
            if df.empty:
                print("❌ DataFrame vazio após leitura")
                return pd.DataFrame()
            
            if 'CNPJ ' not in df.columns:
                print("❌ Coluna 'CNPJ' não encontrada no DataFrame")
                return pd.DataFrame()
            
            df = df[df['CNPJ '].notna() & (df['CNPJ '] != 'Total') & (df['CNPJ '] != '')]
            
            if df.empty:
                print("❌ Nenhum dado válido encontrado após filtragem")
                return pd.DataFrame()
            
            df['origem_arquivo'] = file
            print(f"✅ Tratamento concluído. DataFrame com shape {df.shape}")
            
            return df

        except Exception as e:
            print(f"❌ Erro ao processar arquivo da Zurich: {str(e)}")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        coluna = premio_exec if premio_exec in df.columns else 'premio'
        if coluna in df.columns:
            if fator_melchiori is not None:
                fator_melchiori = 0.965000
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.02
                df['valor_vi'] = df['premio_rec'] * 0.006
                print(df[[coluna, 'premio_rec','valor_cv','valor_vi']].head())
            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")

    def calcular_premio_relatorio(self, df, coluna, fator, table_name):

        try:
            premio_total_relatorio = round(df[coluna].sum() * fator, 2)
            self.file_dfs[table_name] = df
            return premio_total_relatorio
        except Exception as e:
            print(f"❌ Erro ao converter para Decimal: {e}")
            return {}