import os
import pandas as pd
import re
import json
from extrato_app.CoreData.ds4 import parse_meses_opt, obter_mes_ano
from extrato_app.CoreData.dba import DBA
from pandas import ExcelFile
from openpyxl import load_workbook

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

    def read_incentivo(self) -> pd.DataFrame:
        """
        Lê o arquivo de incentivo da Allianz a partir do root dinâmico
        (ROOT_NUMS/ano/Controle de produção/mes - NOME/Allianz),
        localiza o arquivo que contenha 'acordo apuração' no nome,
        a aba que contenha 'aberto corretor', e define o cabeçalho
        como a primeira linha com dado na coluna A.
        Apenas imprime o DF inicial para debug.
        """
        ROOT_NUMS = os.getenv("ROOT_NUMS", "")
        if not ROOT_NUMS:
            print("🚨 ROOT_NUMS não definido no .env")
            return pd.DataFrame()

        competencia = None
        try:
            with open(os.path.join(os.getcwd(), "config.json"), "r", encoding="utf-8") as f:
                competencia = json.load(f).get("competencia")
        except Exception:
            pass
        if not competencia:
            print("🚨 'competencia' não encontrada no config.json")
            return pd.DataFrame()

        try:
            mes, ano = obter_mes_ano(competencia)
        except Exception:
            print(f"🚨 Competencia inválida: {competencia}")
            return pd.DataFrame()

        MESES_PT = parse_meses_opt(os.getenv("MESES_OPT", ""))
        nome_mes = MESES_PT.get(mes)
        if not nome_mes:
            print(f"🚨 Nome do mês não encontrado em MESES_OPT para mes={mes}")
            return pd.DataFrame()

        pasta = os.path.join(
            ROOT_NUMS, str(ano),
            "Controle de produção",
            f"{mes} - {nome_mes}",
            "Allianz"
        )
        if not os.path.isdir(pasta):
            print(f"❌ Pasta do incentivo não encontrada: {pasta}")
            return pd.DataFrame()

        def _norm(s: str) -> str:
            return (s or "").lower()\
                .replace("á","a").replace("ã","a").replace("â","a").replace("à","a")\
                .replace("é","e").replace("ê","e").replace("è","e")\
                .replace("í","i").replace("ì","i")\
                .replace("ó","o").replace("õ","o").replace("ô","o").replace("ò","o")\
                .replace("ú","u").replace("ù","u").replace("û","u")\
                .replace("ç","c")

        padrao = "acordo apuracao"
        candidatos = [
            f for f in os.listdir(pasta)
            if f.lower().endswith(('.xls', '.xlsx'))
            and padrao in _norm(f)
            and not f.startswith("~$")
        ]
        if not candidatos:
            print(f"❌ Nenhum arquivo encontrado em: {pasta} (esperado conter '{padrao}')")
            return pd.DataFrame()

        nome_arquivo = max(candidatos, key=lambda f: os.path.getmtime(os.path.join(pasta, f)))
        file_path = os.path.join(pasta, nome_arquivo)
        print(f"📂 Arquivo alvo: {file_path}")

        try:
            
            ext = os.path.splitext(file_path)[1].lower()
            engine = "xlrd" if ext == ".xls" else "openpyxl"

            excel_obj = pd.ExcelFile(file_path, engine=engine)
            abas = excel_obj.sheet_names
            print(f"📑 Abas disponíveis: {abas}")

            aba_alvo = next((s for s in abas if "aberto corretor" in _norm(s)), None)
            if not aba_alvo:
                print(f"❌ Nenhuma aba contendo 'Aberto Corretor' encontrada no arquivo {nome_arquivo}")
                return pd.DataFrame()

            print(f"📑 Aba selecionada: {aba_alvo}")

            df_raw = pd.read_excel(file_path, sheet_name=aba_alvo, header=None, engine=engine)

            header_idx = None
            for i, val in enumerate(df_raw.iloc[:, 0]):
                if pd.notna(val) and str(val).strip():
                    header_idx = i
                    break
            if header_idx is None:
                print("❌ Não foi possível localizar linha de cabeçalho (coluna A vazia)")
                return pd.DataFrame()

            print(f"📌 Cabeçalho identificado na linha {header_idx}")

            df = pd.read_excel(file_path, sheet_name=aba_alvo, header=header_idx, engine=engine)
            df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

            print(f"✅ Incentivo Allianz lido ({df.shape[0]} linhas x {df.shape[1]} colunas)")
            df.columns = [str(col).strip().lower() for col in df.columns]
            
            df = df[
                df['susep filho'].notna() &
                (df['susep filho'].astype(str).str.strip() != '') &
                (~df['susep filho'].astype(str).str.lower().isin(['nan', 'none']))
            ]

            adicional_cols = [col for col in df.columns if 'adicional' in col]
            print('validação de adicional_cols')
            print(adicional_cols)

            print('validação de adicionais ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print(df[['adicional','adicional.1', 'adicional.2']].head())

            if adicional_cols:
                df['incentivo'] = df[adicional_cols].sum(axis=1, skipna=True)
            else:
                df['incentivo'] = 0

            print('...................................................................')
            print('validação head e tail')
            print(df.columns)
            print(df[['susep filho', 'nome susep filho', 'incentivo']].head(10))
            print(df[['susep filho', 'nome susep filho', 'incentivo']].tail(10))
            incentivo_total = df['incentivo'].sum()
            print(f"🔢 Soma total da coluna 'incentivo': {incentivo_total}")
            print('...................................................................')

            return df

        except Exception as e:
            print(f"❌ Erro ao ler incentivo Allianz: {e}")
            return pd.DataFrame()

