import os
import pandas as pd
import time
import msoffcrypto
from io import BytesIO
import re
import json
from extrato_app.CoreData.ds4 import parse_meses_opt, obter_mes_ano
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

    def read_incentivo(self) -> pd.DataFrame:
        """
        Lê o arquivo de incentivo da Bradesco a partir do root dinâmico (ROOT_NUMS/ano/Controle de produção/mes - NOME/Bradesco),
        detecta dinamicamente a linha do cabeçalho, enriquece com unidade e devolve  DF CONSOLIDADO por nome_unidade
        (mantendo a coluna de valor original como string com vírgulas; a soma converte temporariamente).
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
            ROOT_NUMS,
            str(ano),
            "Controle de produção",
            f"{mes} - {nome_mes}",
            "Bradesco"
        )

        if not os.path.isdir(pasta):
            print(f"❌ Pasta do incentivo não encontrada: {pasta}")
            return pd.DataFrame()

        def _norm(s: str) -> str:
            s = s.lower()
            s = s.replace('á','a').replace('à','a').replace('â','a').replace('ã','a').replace('ä','a')
            s = s.replace('é','e').replace('è','e').replace('ê','e').replace('ë','e')
            s = s.replace('í','i').replace('ì','i').replace('î','i').replace('ï','i')
            s = s.replace('ó','o').replace('ò','o').replace('ô','o').replace('õ','o').replace('ö','o')
            s = s.replace('ú','u').replace('ù','u').replace('û','u').replace('ü','u')
            s = s.replace('ç','c')
            return s

        padroes = ["apuracao consolidada", "cesp"]
        candidatos = [
            f for f in os.listdir(pasta)
            if f.lower().endswith(('.xls', '.xlsx'))
            and any(p in _norm(f) for p in padroes)
        ]

        if not candidatos:
            print(f"❌ Nenhum arquivo de incentivo encontrado em: {pasta} "
                f"(esperado conter {' ou '.join(padroes).upper()})")
            return pd.DataFrame()

        nome_arquivo = max(candidatos, key=lambda f: os.path.getmtime(os.path.join(pasta, f)))
        file_path = os.path.join(pasta, nome_arquivo)

        def _provavel_header_idx(df_top: pd.DataFrame) -> int | None:
            esperados = {"cnpj", "corretora", "sucursal", "ramos", "mercado", "cdl", "segmento", "observacao", "valor a pagar"}
            melhor_idx, melhor_score = None, 0
            for i in range(min(len(df_top), 30)):
                row = [str(x).strip().lower() for x in df_top.iloc[i].tolist()]
                norm = []
                for v in row:
                    v = _norm(v)
                    norm.append(v)
                score = sum(any(tok in v for tok in esperados) for v in norm)
                if score > melhor_score:
                    melhor_score, melhor_idx = score, i
            return melhor_idx if melhor_score >= 3 else None

        def _to_float_ptbr(x):
            """
            Converte string em formato brasileiro para float.
            """
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

                def norm_col(c: str) -> str:
                    s = str(c).strip().lower()
                    s = _norm(s)
                    s = re.sub(r'\s+', '_', s)
                    return s
                df.columns = [norm_col(c) for c in df.columns]

                col_valor = next((c for c in df.columns if 'valor_a_pagar' in c), None)
                if col_valor:
                    df[col_valor] = df[col_valor].astype(str).str.strip()

                df["origem_arquivo"] = nome_arquivo
                df["tipo_fonte"] = "incentivo"

                ref_nom = None
                try:
                    with open(os.path.join(os.getcwd(), "config.json"), "r", encoding="utf-8") as cf:
                        ref_nom = json.load(cf).get("ref_nom")
                except Exception:
                    pass

                if ref_nom and ref_nom not in df.columns:
                    for candidato in ["corretora", "sucursal", "nome_unidade"]:
                        if candidato in df.columns:
                            df[ref_nom] = df[candidato]
                            print(f"ℹ️ ref_nom='{ref_nom}' ausente; usando '{candidato}' como origem.")
                            break
                elif not ref_nom:
                    if "corretora" in df.columns:
                        df["corretora_ref"] = df["corretora"]
                        ref_nom = "corretora_ref"
                        print("ℹ️ config sem ref_nom; usando 'corretora' como referência provisória.")
                    elif "sucursal" in df.columns:
                        df["sucursal_ref"] = df["sucursal"]
                        ref_nom = "sucursal_ref"
                        print("ℹ️ config sem ref_nom; usando 'sucursal' como referência provisória.")

                try:
                    dba = DBA()
                    df = dba.add_id_unidade_from_database(df, "Bradesco")
                except Exception as e:
                    print(f"⚠️ Falha ao enriquecer incentivo com unidade: {e}")

                print(f"✅ Incentivo Bradesco lido (header linha {header_idx}) | {df.shape[0]}x{df.shape[1]}")
                print(df[['origem_arquivo'] + ([col_valor] if col_valor else [])].head())

                col_valor = next((c for c in df.columns if 'valor_a_pagar' in str(c).lower()), None)
                if not col_valor:
                    print("⚠️ Coluna 'valor_a_pagar' não encontrada. Retornando DF original.")
                    return df

                if 'nome_unidade' not in df.columns:
                    print("⚠️ 'nome_unidade' não encontrado após o enriquecimento; retornando DF original.")
                    return df

                group_keys = ['nome_unidade']
                for aux in ['id_unidade', 'id_cor_cliente', 'competencia']:
                    if aux in df.columns:
                        group_keys.append(aux)

                df_grp = (
                    df.groupby(group_keys, dropna=False)[col_valor]
                      .apply(lambda s: sum(_to_float_ptbr(v) for v in s))
                      .reset_index()
                      .rename(columns={col_valor: 'valor_a_pagar_total'})
                )
                df_grp['tipo_fonte'] = 'incentivo'
                df_grp['origem_arquivo'] = nome_arquivo
                df_grp = df_grp.rename(columns={'valor_a_pagar_total': 'valor_incentivo'})
                df_grp['cia'] = 'Bradesco'

                df_incentivo = df_grp

                # print('tratamento INNER BDC concluido, validação de colunas totais abaixo:')
                # print(df_incentivo.columns.tolist())
                # print("✅ Consolidado por nome_unidade (preview):")
                # print(df_incentivo[['nome_unidade', 'valor_incentivo']].head())

                return df_incentivo

        except Exception as e:
            print(f"❌ Erro ao ler incentivo Bradesco: {e}")
            return pd.DataFrame()
        

        
