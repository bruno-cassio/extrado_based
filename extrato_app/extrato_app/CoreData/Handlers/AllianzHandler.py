import os
import pandas as pd
import re
import json
from extrato_app.CoreData.ds4 import parse_meses_opt, obter_mes_ano
from extrato_app.CoreData.dba import DBA
from pandas import ExcelFile
from openpyxl import load_workbook
from extrato_app.CoreData.IncentivoMain import (
    norm_str,
    montar_pasta_incentivo,
    encontrar_arquivo,
    get_ref_nom,
)
from extrato_app.CoreData.dba import DBA




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
            
    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        try:
            
            print('validação de fatores')
            print(coluna)
            print(f'fator: {fator}')

            df = df.loc[:, ~df.columns.duplicated(keep='first')]

            if coluna not in df.columns:
                print(f"⚠️ Coluna '{coluna}' não encontrada no DataFrame.")
                return 0.0, df

            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

            premio_total_relatorio = round(df[coluna].sum() * fator, 2)
            print(f"✅ Total de 'premio' para Allianz: {premio_total_relatorio}")
            
            
            print('validação previa return de calculo de premio')

            
            return premio_total_relatorio, df

        except Exception as e:
            print(f"❌ Erro ao calcular prêmio Allianz: {e}")
            return 0.0, df

    def read_incentivo(self, competencia: str) -> pd.DataFrame:
        """
        Lê o incentivo da Allianz:
        - Busca arquivo que contenha 'acordo apuração'
        - Aba 'Aberto Corretor - Ajustado' (ou fallback 'Aberto Corretor')
        - Cabeçalho dinâmico
        - Soma adicionais
        - Aplica mapeamento de unidade
        - Consolida incentivos
        """
        pasta = montar_pasta_incentivo("Allianz", competencia)
        if not pasta:
            return pd.DataFrame()

        nome_arquivo = encontrar_arquivo(pasta, ["acordo apuracao"])
        if not nome_arquivo:
            return pd.DataFrame()
        file_path = os.path.join(pasta, nome_arquivo)
        print(f"📂 Arquivo alvo: {file_path}")

        try:
            engine = "xlrd" if file_path.endswith(".xls") else "openpyxl"
            excel_obj = pd.ExcelFile(file_path, engine=engine)
            abas = excel_obj.sheet_names

            aba_alvo = next((s for s in abas if "aberto corretor - ajustado" in norm_str(s)), None)
            if not aba_alvo:
                aba_alvo = next((s for s in abas if "aberto corretor" in norm_str(s)), None)
            if not aba_alvo:
                print("❌ Nenhuma aba de 'Aberto Corretor' encontrada")
                return pd.DataFrame()

            df_raw = pd.read_excel(file_path, sheet_name=aba_alvo, header=None, engine=engine)
            header_idx = next((i for i, v in enumerate(df_raw.iloc[:, 0]) if pd.notna(v) and str(v).strip()), None)
            if header_idx is None:
                print("❌ Não foi possível localizar cabeçalho")
                return pd.DataFrame()

            df = pd.read_excel(file_path, sheet_name=aba_alvo, header=header_idx, engine=engine)
            df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
            df.columns = [str(c).strip().lower() for c in df.columns]

            df = df[df['susep filho'].notna() & (df['susep filho'].astype(str).str.strip() != '')]

            adicional_cols = [c for c in df.columns if 'adicional' in c and c != 'total adicional']
            df['incentivo'] = df[adicional_cols].sum(axis=1, skipna=True) if adicional_cols else 0

            df, ref_nom = get_ref_nom(df, ["nome susep filho"])
            try:
                dba = DBA()
                df = dba.add_id_unidade_from_database(df, "Allianz")
            except Exception as e:
                print(f"⚠️ Falha no mapeamento de unidades: {e}")

            group_keys = ['id_unidade', 'id_cor_cliente', 'competencia', ref_nom or 'nome_unidade']
            df_grp = (
                df.groupby(group_keys, dropna=False)['incentivo']
                .sum()
                .reset_index()
                .rename(columns={'incentivo': 'valor_incentivo'})
            )
            df_grp['tipo_fonte'] = 'incentivo'
            df_grp['origem_arquivo'] = nome_arquivo
            df_grp['cia'] = 'Allianz'

            print(f"✅ Consolidado Allianz ({df_grp.shape[0]} linhas)")
            return df_grp

        except Exception as e:
            print(f"❌ Erro ao ler incentivo Allianz: {e}")
            return pd.DataFrame()