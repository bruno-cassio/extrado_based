import os
import pandas as pd
import re

class AllianzHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx')) and "producaoallianz" in f.lower()]
        if not files:
            print("❌ Nenhum arquivo da Allianz encontrado com 'ProducaoAllianz'")
            return pd.DataFrame()
        
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Allianz - arquivo selecionado: {file}")

        df_raw = pd.read_excel(file_path, sheet_name=0, header=None, dtype=str)
        start_idx = df_raw[df_raw[0].str.contains("COMISSÃO REGULAR", na=False, flags=re.IGNORECASE)].index
        if start_idx.empty:
            print("❌ Título 'COMISSÃO REGULAR' não encontrado na coluna A")
            return pd.DataFrame()

        start_row = start_idx[0] + 1
        df_table = pd.read_excel(file_path, sheet_name=0, header=start_row)

        df_table['origem_arquivo'] = file
        return df_table

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        print('valiação aqui =========================s')
        print(df.columns)
        print('validação acima =========================s')

        coluna = premio_exec if premio_exec in df.columns else 'valor_comissao'

        if coluna in df.columns:
            if fator_melchiori is not None:
                print('aqui')
                df['premio_rec'] = (df[coluna]) /0.03 * fator_melchiori

                df['valor_cv'] = df.apply(
                    lambda row: row['premio_rec'] * 0.02 if row['ramo'] not in ["1251 - Frota Automóvel Dig.", "115 - Vida Individual"] else 0,
                    axis=1
                )
                df['valor_vi'] = df.apply(
                    lambda row: row['premio_rec'] * 0.01 if row['ramo'] not in ["1251 - Frota Automóvel Dig.", "115 - Vida Individual"] else 0,
                    axis=1
                )
                df['valor_as'] = df.apply(
                    lambda row: row['premio_rec'] * 0.03 if row['ramo'] in ["1251 - Frota Automóvel Dig.", "115 - Vida Individual"] else 0,
                    axis=1
                )
                print("\n💼 Processando Allianz - Amostra dos cálculos:")
                print(df[['ramo', coluna, 'premio_rec', 'valor_cv', 'valor_vi', 'valor_as']].head())

            else:
                print("⚠️ Fator Melchiori não fornecido para cálculo")
        else:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
            
    # def calcular_premio_relatorio(self, df, coluna, fator, table_name):
    #     try:
            
    #         print('validação de fatores')
    #         print(coluna)
    #         print(f'fator: {fator}')

    #         df = df.loc[:, ~df.columns.duplicated(keep='first')]

    #         if coluna not in df.columns:
    #             print(f"⚠️ Coluna '{coluna}' não encontrada no DataFrame.")
    #             return 0.0, df

    #         df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

    #         premio_total_relatorio = round(df[coluna].sum() * fator, 2)
    #         print(f"✅ Total de 'premio' para Allianz: {premio_total_relatorio}")
            
            
    #         print('validação previa return de calculo de premio')

            
    #         return premio_total_relatorio, df

    #     except Exception as e:
    #         print(f"❌ Erro ao calcular prêmio Allianz: {e}")
    #         return 0.0, df


