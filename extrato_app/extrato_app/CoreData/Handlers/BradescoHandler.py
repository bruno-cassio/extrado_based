import os
import pandas as pd
import time
import msoffcrypto
from io import BytesIO
import re
import json
from extrato_app.CoreData.ds4 import parse_meses_opt, obter_mes_ano
from extrato_app.CoreData.dba import DBA
from extrato_app.CoreData.IncentivoMain import (
    norm_str,
    montar_pasta_incentivo,
    encontrar_arquivo,
    get_ref_nom,
)
from extrato_app.CoreData.dba import DBA


class BradescoHandler:

    def treat(self, folder_path: str) -> pd.DataFrame:
        
        files = [
            f for f in os.listdir(folder_path) 
            if f.lower().endswith(('.xls', '.xlsx')) and 
            ('Producao' in f or 'Consolidado' in f)
        ]
        
        if not files:
            print("❌ Nenhum arquivo encontrado contendo 'Producao' ou 'Consolidado'")
            return pd.DataFrame()

        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)

        try:
            start = time.perf_counter()
            df = pd.read_excel(file_path, sheet_name=0)
            end = time.perf_counter()
            print(f"⏱️ Tempo de leitura otimizada: {end - start:.2f}s")
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

    def calcular_premio_relatorio(self, df, coluna, fator, table_name):
        try:
            premio_total_relatorio = round(df[coluna].sum() * fator, 2)
            # self.file_dfs[table_name] = df
            return premio_total_relatorio, df
        except Exception as e:
            print(f"❌ Erro ao converter para Decimal: {e}")
            return {}

    def read_incentivo(self, competencia: str) -> pd.DataFrame:
        """
        Lê o incentivo da Bradesco:
        - Arquivo contendo 'apuracao consolidada' ou 'cesp'
        - Cabeçalho dinâmico detectado
        - Normaliza colunas
        - Aplica mapeamento de unidade
        - Consolida valores
        """
        pasta = montar_pasta_incentivo("Bradesco", competencia)
        if not pasta:
            return pd.DataFrame()

        nome_arquivo = encontrar_arquivo(pasta, ["apuracao consolidada", "cesp"])
        if not nome_arquivo:
            return pd.DataFrame()
        file_path = os.path.join(pasta, nome_arquivo)

        def _provavel_header_idx(df_top: pd.DataFrame) -> int | None:
            esperados = {"cnpj", "corretora", "sucursal", "valor a pagar"}
            melhor_idx, melhor_score = None, 0
            for i in range(min(len(df_top), 30)):
                row = [norm_str(str(x)) for x in df_top.iloc[i].tolist()]
                score = sum(any(tok in v for tok in esperados) for v in row)
                if score > melhor_score:
                    melhor_score, melhor_idx = score, i
            return melhor_idx if melhor_score >= 2 else None

        def _to_float_ptbr(x):
            try:
                s = str(x).strip()
                if not s:
                    return 0.0
                s = re.sub(r'[^0-9,.-]', '', s)
                s = re.sub(r'(?<=\d)\.(?=\d{3}(,|$))', '', s)
                s = s.replace(',', '.')
                return float(s)
            except Exception:
                return 0.0

        try:
            with open(file_path, "rb") as f:
                import msoffcrypto
                from io import BytesIO
                office = msoffcrypto.OfficeFile(f)
                office.load_key(password="BARE")
                buf = BytesIO()
                office.decrypt(buf)

                buf.seek(0)
                topo = pd.read_excel(buf, sheet_name=0, header=None, nrows=50)
                header_idx = _provavel_header_idx(topo) or 0

                buf.seek(0)
                df = pd.read_excel(buf, sheet_name=0, header=header_idx)
                df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
                df.columns = [re.sub(r'\s+', '_', norm_str(str(c).strip())) for c in df.columns]

                col_valor = next((c for c in df.columns if 'valor_a_pagar' in c), None)
                if col_valor:
                    df[col_valor] = df[col_valor].astype(str).str.strip()

                df["origem_arquivo"] = nome_arquivo
                df["tipo_fonte"] = "incentivo"

                df, ref_nom = get_ref_nom(df, ["corretora", "sucursal", "nome_unidade"])
                try:
                    dba = DBA()
                    df = dba.add_id_unidade_from_database(df, "Bradesco")
                except Exception as e:
                    print(f"⚠️ Falha no mapeamento de unidades: {e}")

                if not col_valor:
                    return df
                group_keys = ['nome_unidade']
                for aux in ['id_unidade', 'id_cor_cliente', 'competencia']:
                    if aux in df.columns:
                        group_keys.append(aux)

                df_grp = (
                    df.groupby(group_keys, dropna=False)[col_valor]
                    .apply(lambda s: sum(_to_float_ptbr(v) for v in s))
                    .reset_index()
                    .rename(columns={col_valor: 'valor_incentivo'})
                )
                df_grp['tipo_fonte'] = 'incentivo'
                df_grp['origem_arquivo'] = nome_arquivo
                df_grp['cia'] = 'Bradesco'

                print(f"✅ Consolidado Bradesco ({df_grp.shape[0]} linhas)")
                return df_grp

        except Exception as e:
            print(f"❌ Erro ao ler incentivo Bradesco: {e}")
            return pd.DataFrame()      

        
