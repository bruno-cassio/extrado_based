import os
from dotenv import load_dotenv
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import pandas as pd
import datetime
from extrato_app.CoreData.Handlers.BradescoHandler import BradescoHandler
from extrato_app.CoreData.Handlers.SuhaiHandler import SuhaiHandler
from extrato_app.CoreData.Handlers.AllianzHandler import AllianzHandler
from extrato_app.CoreData.Handlers.JuntoHandler import JuntoHandler
from extrato_app.CoreData.Handlers.HDIHandler import HDIHandler
from extrato_app.CoreData.Handlers.PortoHandler import PortoHandler
from extrato_app.CoreData.Handlers.BradescoSaudeHandler import BradescoSaudeHandler
from extrato_app.CoreData.Handlers.YelumHandler import YelumHandler
from extrato_app.CoreData.Handlers.AxaHandler import AxaHandler
from extrato_app.CoreData.Handlers.ZurichHandler import ZurichHandler
from extrato_app.CoreData.Handlers.ChubbHandler import ChubbHandler
from extrato_app.CoreData.Handlers.TokioHandler import TokioHandler
from extrato_app.CoreData.Handlers.EzzeHandler import EzzeHandler
from extrato_app.CoreData.Handlers.SompoHandler import SompoHandler
from extrato_app.CoreData.Handlers.MapfreHandler import MapfreHandler


load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))

class TratamentoRecalculo:
    
    REGRAS_PREMIO = {
        'Bradesco': {'fator': 0.05},
        'Bradesco Saude': {'fator': 1},
        'Suhai': {'fator': 0.05},
        'Allianz': {'fator': 0.03},
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
        'Mapfre': {'fator': 1}
    }

    def __init__(self):
        
        self.bradesco_handler = BradescoHandler()
        self.suhai_handler = SuhaiHandler()
        self.allianz_handler = AllianzHandler()
        self.junto_handler = JuntoHandler()
        self.hdi_handler = HDIHandler()
        self.porto_handler = PortoHandler()
        self.bradesco_saude_handler = BradescoSaudeHandler()
        self.Yelum_handler = YelumHandler()
        self.axa_handler = AxaHandler()
        self.zurich_handler = ZurichHandler()
        self.chubb_handler = ChubbHandler()
        self.tokio_handler = TokioHandler()
        self.ezze_handler = EzzeHandler()
        self.sompo_handler = SompoHandler()
        self.mapfre_handler = MapfreHandler()

        self.process_dispatcher = {
            "Bradesco": self.bradesco_handler.process,
            "Suhai": self.suhai_handler.process,
            "Allianz": self.allianz_handler.process,
            "Junto Seguradora": self.junto_handler.process,
            "Hdi": self.hdi_handler.process,
            "Porto": self.porto_handler.process,
            "Bradesco Saude": self.bradesco_saude_handler.process,
            "Yelum": self.Yelum_handler.process,
            "Axa": self.axa_handler.process,
            "Zurich": self.zurich_handler.process,
            "Chubb": self.chubb_handler.process,
            "Tokio": self.tokio_handler.process,
            "Ezze": self.ezze_handler.process,
            "Sompo": self.sompo_handler.process,
            "Mapfre": self.mapfre_handler.process
                    }
        
        self.file_dfs = {}

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

        #Criar modulo para tratamnto dispatcher de premio relatorio.
        if regra:
            coluna = premio_exec
            fator = regra['fator']
            print(f'Cálculo de premio para {cia} usando a coluna {coluna} com fator {fator}.')

            if coluna in df.columns:
                print('tTESTE HEREE')
                print(df.columns)

                # premio_total_relatorio = round(df[coluna].sum() * fator, 2)


            if cia in ['Mapfre']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            if cia in ['Sompo']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            if cia in ['Tokio']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            if cia in ['Ezze']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            if cia in ['Yelum']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            elif cia in ['Axa']:
                try:
                    df['pagamento_convertido'] = pd.to_datetime(df['data_pagamento'], dayfirst=True).dt.strftime('%m-%Y')
                    
                    print('Valores únicos em competencia:', df['competencia'].unique())
                    print('Valores únicos em pagamento_convertido:', df['pagamento_convertido'].unique())
                    
                    df = df[df['pagamento_convertido'] == df['competencia']]
                    
                    print('Valores únicos após filtro:', df['pagamento_convertido'].unique())
                    print('Número de linhas após filtro:', len(df))
                    
                    df.drop(columns=['pagamento_convertido'], inplace=True)
                    
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)
                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                    
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}
                except Exception as e:
                    print(f"❌ Erro ao processar dados da Axa: {str(e)}")
                    return {}

            elif cia in ['Hdi']:
                try:
                    
                    print('🧼 Convertendo dados da coluna para Decimal...')
                    df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

                    print(f"Últimas linhas das colunas '{coluna}' e 'susep':")
                    print(df[[coluna, 'susep']].tail())

                    premioBASE = df.loc[df['descricao_corretor_coligado'].astype(str).str.strip() != 'Total Geral', coluna].sum()
                    print(f'📊 Soma da coluna {coluna}: {premioBASE}')
                    print(premioBASE)

                    premio_total_relatorio = round((df[df['descricao_corretor_coligado'].astype(str).str.strip() != 'Total Geral'][coluna].sum() * fator), 2)

                    print(f"-==-=-=-==-=-=-=-=-=--=- Total de 'premio' para {cia}: {premio_total_relatorio} -==-=-=-==-=-=-=-=-=--=- ")
                    self.file_dfs[table_name] = df
                    print('✅ Cálculo realizado com sucesso.')
                    return premio_total_relatorio

                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}           

            elif cia in ['Chubb']:
                try:
                    df = df[df['corretor'].astype(str).str.strip() != 'Total Geral']
                    print('cade a coluna ========== staged')
                    print(df.columns)
                    print('SOMANDO PREMIO CHUBB')
                    print(coluna)
                    print('soma inicial:', df[coluna].sum())
                    
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)
                    print(f" ====================================> Total de 'premio' FATORADO para {cia}: {premio_total_relatorio}")
                    self.file_dfs[table_name] = df
                    print(df.tail())
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            elif cia in ['Bradesco', 'Suhai', 'Bradesco Saude', 'Zurich']:
                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)
                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}
            
            elif cia in ['Porto']:
                
                coluna = coluna.replace(' ', '_')
                print('============================ checagenzinha aqui ============================')
                print('PORTO aqui')
                print(df.columns)
                print(f"📋 Colunas atuais no df: {df.columns.tolist()}")
                print(f"🔎 Procurando a coluna: {coluna}")
                duplicadas = df.columns[df.columns.duplicated()].tolist()
                if duplicadas:
                    print(f"🚨 Atenção: Colunas duplicadas detectadas: {duplicadas}")
                
                df = df[df['nome_corretor'].notna() & (df['nome_corretor'].astype(str).str.strip() != '') & (df['nome_corretor'].astype(str).str.strip() != 'Total')]
                
                downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
                if not os.path.exists(downloads_dir):
                    os.makedirs(downloads_dir)

                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                xls_filename = f"{table_name}_{timestamp}.xlsx"
                xls_path = os.path.join(downloads_dir, xls_filename)

                df.to_excel(xls_path, index=False)
                print(f"✅ Arquivo salvo em: {xls_path}")
                
                print('prefire')
                df = df[df[coluna].notna()]
                print('inside')
                print(df[coluna].tail())
                print('inner')
                
                premio_total_relatorio = round(
                    (df[
                        (df['nome_corretor'].astype(str).str.strip() != 'Total') &
                        (df['nome_corretor'].astype(str).str.strip() != '')
                    ][coluna].sum() * fator), 2
                )

                # premio_total_relatorio = round(df[coluna].sum() * fator, 2)
                print(f"Total de 'premio' para {cia}: {premio_total_relatorio}")
                print('================================================== passed // retornando ==================================================')
                self.file_dfs[table_name] = df
                return premio_total_relatorio
            
            elif cia in ['Junto Seguradora']:
                df = df[df[coluna].notna()]
                print(df[coluna].head())
                premio_total_relatorio = round(df[coluna].sum() * fator, 2)
                print(f"Total de 'premio' para {cia}: {premio_total_relatorio}")
                self.file_dfs[table_name] = df
                return premio_total_relatorio

            elif cia == 'Allianz':
                premio_total_relatorio = round(df[coluna].sum(), 2)
                print(f"Total de 'premio' para {cia}: {premio_total_relatorio}")
                self.file_dfs[table_name] = df
                return premio_total_relatorio
            
            else:
                print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
        else:
            print(f"⚠️ Companhia {cia} não reconhecida para cálculo de 'premio_rec'.")

        return self.file_dfs

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

        if cia in self.process_dispatcher:
            self.process_dispatcher[cia](df, file_name, premio_exec, fator_melchiori, premio_db)

            if cia == 'Ezze':
                print('validação ezze')
                print(df.columns)
                df = df.rename(columns={'cv': 'premio_rec', 'vi': 'valor_cv', 'as': 'valor_as'})
                print(df[['premio_base', 'premio_rec', 'valor_cv', 'valor_as']])

                print('============================================================================= validação acima erro de agora =============================================================================')

            print('dispatched bitch:', cia)
            print(f"Processado para cia: {cia}")
        else:
            print(f"⚠️ Companhia {cias_corresp_list} não reconhecida para cálculo de 'premio_rec'.")

        self.file_dfs[table_name] = df
        return self.file_dfs
