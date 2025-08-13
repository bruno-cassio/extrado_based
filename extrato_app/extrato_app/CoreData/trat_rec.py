import os
from dotenv import load_dotenv
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import pandas as pd
import datetime
from extrato_app.CoreData.handlers_registry import CIA_HANDLERS, CIA_PROCESS_DISPATCHER

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
        'Mapfre': {'fator': 1},
        'Swiss': {'fator': 1}
    }

    def __init__(self):
        self.file_dfs = {}
        self.process_dispatcher = CIA_PROCESS_DISPATCHER
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
        
        #Criar modulo para tratamnto dispatcher de premio relatorio.
        #Construir função individualizada em cada handler para calculo de premio do relatório. 

        if regra:
            coluna = premio_exec
            fator = regra['fator']
            print(f'Cálculo de premio para {cia} usando a coluna {coluna} com fator {fator}.')


            if cia in ['Swiss']:
                print('CIA EM PROCESSAMENTO DE CONS REL > SWISS')
                try:
                    premio_total_relatorio = round(df['soma_de_valor_liquido_da_parcela'].sum() * fator, 2)
                    self.file_dfs[table_name] = df
                    
                    print('premio total de SWIIS ->')
                    print(premio_total_relatorio)
                    print('====================== acima premio da swiss ======================')
                    
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}            

            elif cia in ['Tokio']:

                try:
                    df['premio_base'] = df['total_com'] / df['total_com_pct']
                    premio_total_relatorio = round(df['premio_base'].sum() * fator, 2)
                    self.file_dfs[table_name] = df
                    
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            elif cia in ['Mapfre']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            elif cia in ['Sompo']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            elif cia in ['Ezze']:

                try:
                    premio_total_relatorio = round(df[coluna].sum() * fator, 2)

                    self.file_dfs[table_name] = df
                    return premio_total_relatorio
                except InvalidOperation as e:
                    print(f"❌ Erro ao converter para Decimal: {e}")
                    return {}

            elif cia in ['Yelum']:

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
                    
                    coluna = 'premio_target'
                    print(' ================================ validação de colunas previa ao tratamento de exponenciais.================================ ')
                    print(df.columns)
                    print(' ================================ validação de colunas previa ao tratamento de exponenciais.================================ ')
                    
                    print('🧼 Convertendo dados da coluna para Decimal...')
                    df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

                    df = df[df['susep'].notna() & (df['susep'].astype(str).str.strip() != '') & (df['susep'].astype(str).str.lower() != 'nan')]

                    print('validação da remoção de linhas com susep vazia ou inválida:')
                    print(df.tail())

                    print(f"Últimas linhas das colunas '{coluna}' e 'susep':")
                    print(df[['descricao_corretor_coligado', coluna, 'susep']].tail())

                    premioBASE = df.loc[df['descricao_corretor_coligado'].astype(str).str.strip() != 'Total Geral', coluna].sum()
                    print(f'📊 Soma da coluna para premio base {coluna}: {premioBASE}')
                    print(premioBASE)

                    filtro_validacao = (
                    (df['descricao_corretor_coligado'].astype(str).str.strip() != 'Total Geral') &
                    (df['susep'].notna()) &
                    (~df['susep'].astype(str).str.lower().str.strip().isin(['', 'nan', 'none']))
                )

                    premio_total_relatorio = round(df[filtro_validacao][coluna].sum() * fator, 2)

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
