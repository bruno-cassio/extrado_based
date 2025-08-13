import os
import pandas as pd

class YelumHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        """
        Processa arquivos Yelum, lendo as abas 'Automóvel' e 'Demais Ramos',
        unificando os dados em um único DataFrame.
        
        Args:
            folder_path (str): Caminho da pasta contendo os arquivos
            
        Returns:
            pd.DataFrame: DataFrame unificado com os dados das duas abas
        """
        try:
            files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xls', '.xlsx'))]
            if not files:
                print("❌ Nenhum arquivo encontrado para Yelum")
                return pd.DataFrame()

            file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
            file_path = os.path.join(folder_path, file)
            print(f"📂 Yelum - arquivo selecionado: {file}")

            excel_file = pd.ExcelFile(file_path)
            required_sheets = ['Automóvel', 'Demais Ramos']
            available_sheets = [sheet for sheet in excel_file.sheet_names if sheet in required_sheets]
            
            if not available_sheets:
                print(f"❌ Nenhuma das abas requeridas ({required_sheets}) encontrada no arquivo")
                return pd.DataFrame()

            dfs = []
            for sheet in available_sheets:
                df_sheet = pd.read_excel(excel_file, sheet_name=sheet)
                df_sheet['origem_arquivo'] = file
                df_sheet['aba_origem'] = sheet
                dfs.append(df_sheet)
            
            df_final = pd.concat(dfs, ignore_index=True)
            print(f"✅ Yelum - Dados processados. Total de registros: {len(df_final)}")
            print(f"DEBUG: Abas combinadas - {available_sheets}")
            df = df_final.copy()
            df.columns = [col.lower() for col in df.columns]
            print(df.head())
            print(df.columns)

            return df

        except Exception as e:
            print(f"❌ Erro ao processar arquivo Yelum: {str(e)}")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, cias_corresp, file_name: str, table_name: str, premio_exec: str, fator_melchiori: float = None):
        coluna = premio_exec if premio_exec in df.columns else 'premio_1'
        
        if coluna in df.columns:
            fator_melchiori = 0.964999
            if fator_melchiori is not None:
                print(f'premio REC baseado no fator Melchiori FIXO', fator_melchiori)
                df['premio_rec'] = df[coluna] * fator_melchiori
                df['valor_cv'] = df['premio_rec'] * 0.01
                df['valor_vi'] = df['premio_rec'] * 0.06 
                print(df[[coluna, 'premio_rec', 'valor_cv','valor_vi']].head())
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
