import os
from dotenv import load_dotenv
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import pandas as pd
import datetime
from extrato_app.CoreData.handlers_registry import CIA_HANDLERS

load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))

class TratamentoRecalculo:


    REGRAS_PREMIO = {
        'Bradesco': {'fator': 0.05},
        'Bradesco Saude': {'fator': 1},
        'Suhai': {'fator': 0.05},
        'Allianz': {'fator': 1},
        'Junto Seguradora': {'fator': 0.05},
        'Hdi': {'fator': 0.022},
        'Porto': {'fator': 0.02},
        'Yelum': {'fator': 0.016},
        'Axa': {'fator': 0.05},
        'Zurich': {'fator': 0.05},
        'Chubb': {'fator': 1},
        'Tokio': {'fator': 1},
        'Ezze': {'fator': 1},
        'Sompo': {'fator': 1},
        'Mapfre': {'fator': 1},
        'Swiss': {'fator': 1}
    }

    def __init__(self):

        self.file_dfs = {}
        self.handlers = CIA_HANDLERS
            
    def cons_rel(self, df, cias_corresp_list, file_name, table_name, premio_exec):
        print('cons_rel started')

        try:
            with open(os.path.join(os.getcwd(), "config.json"), "r", encoding="utf-8") as f:
                config_data = json.load(f)
                cia_escolhida = config_data.get("cia_corresp", "")
                competencia_escolhida = config_data.get("competencia", "")
        except json.JSONDecodeError:
            print("❌ Arquivo config.json vazio ou mal formatado. Criando novo...")
            with open(os.path.join(os.getcwd(), "config.json"), "w", encoding="utf-8") as f:
                base_config = {"cia_corresp": "", "competencia": ""}
                json.dump(base_config, f)
            cia_escolhida = ""
            competencia_escolhida = ""
        except Exception as e:
            print(f"❌ Erro ao ler config.json: {e}")
            return {}

        cia = cia_escolhida
        regra = self.REGRAS_PREMIO.get(cia)
        competencia = competencia_escolhida
        print(competencia)
        print('premio_exec:', premio_exec)
        print(f'Regra para {cia}: {regra}')


        if regra:
            coluna = premio_exec
            fator = regra['fator']
            print(f'Cálculo de prêmio para {cia} usando a coluna {coluna} com fator {fator}.')

            handler = CIA_HANDLERS.get(cia)
            if handler and hasattr(handler, "calcular_premio_relatorio"):
                try:
                    premio_total_relatorio, df = handler.calcular_premio_relatorio(df, coluna, fator, table_name)

                    df = df.loc[:, ~df.columns.duplicated(keep='first')]

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except Exception as e:
                    print(f"❌ Erro ao calcular prêmio para {cia}: {e}")
                    return {}
            else:
                print(f"⚠️ Handler para {cia} não implementa calcular_premio_relatorio()")

    def _get_coluna_padrao(self, cia):
        padroes = {
            'Bradesco': 'premio',
            'Suhai': 'vlr_tarifario',
            'Allianz': 'valor_comissao'
        }
        return padroes.get(cia, 'premio')  

    def process_recalculo(self, df, cias_corresp_list, file_name, table_name, premio_exec, fator_melchiori=None, premio_db=None):
        try:
            print('================================ process_recalculo started ================================')
            with open(os.path.join(os.getcwd(), "config.json"), "r", encoding="utf-8") as f:
                config_data = json.load(f)
                cia_escolhida = config_data.get("cia_corresp", "")
                competencia_escolhida = config_data.get("competencia", "")
        except json.JSONDecodeError:
            print("❌ Arquivo config.json vazio ou mal formatado. Criando novo...")
            with open(os.path.join(os.getcwd(), "config.json"), "w", encoding="utf-8") as f:
                base_config = {"cia_corresp": "", "competencia": ""}
                json.dump(base_config, f)
            cia_escolhida = ""
            competencia_escolhida = ""
        except Exception as e:
            print(f"❌ Erro ao ler config.json: {e}")
            return {}

        cia = cia_escolhida

        handler = CIA_HANDLERS.get(cia)

        if handler:
            handler.process(df, file_name, premio_exec, fator_melchiori, premio_db)

            if cia == 'Ezze':
                print('validação ezze')
                print(df.columns)
                df = df.rename(columns={'cv': 'premio_rec', 'vi': 'valor_cv', 'as': 'valor_as'})
                print(df[['premio_base', 'premio_rec', 'valor_cv', 'valor_as']])

                print('============================================================================= validação acima erro de agora =============================================================================')

            elif cia == 'Tokio':

                print('pretreaT')
                print(df['anomes_referencia'].unique())
                df['anomes_referencia'] = pd.to_datetime(df['anomes_referencia'], format='%Y%m').dt.strftime('%m-%Y')

                print('VALIDAÇÃO PASSO INTERMEDIARIO TOKIO ANTES DE EQUIPARAÇÃO')
                print(df['anomes_referencia'].unique())

                print(f"Quantidade de linhas antes do filtro: {len(df)}")
                df = df[df['anomes_referencia'] == competencia_escolhida]
                print(f"Quantidade de linhas após o filtro: {len(df)}")
                print(df['anomes_referencia'].unique())
                print('validação tokio por equiparação')
                print(df.columns)

            print('dispatched bitch:', cia)
            print(f"Processado para cia: {cia}")

        else:
            print(f"⚠️ Companhia {cias_corresp_list} não reconhecida para cálculo de 'premio_rec'.")

        self.file_dfs[table_name] = df
        return self.file_dfs