import os
import sys
import time
import logging
from dotenv import load_dotenv
from extrato_app.CoreData.CoreMain import DataImporter
from extrato_app.CoreData.ds4 import parse_meses_opt, atualizar_config, processar_automaticamente
import pandas as pd
from pathlib import Path
from extrato_app.CoreData import grande_conn
from extrato_app.CoreData.grande_conn import DatabaseManager
from collections import defaultdict
from decimal import Decimal
import json
import traceback
from extrato_app.CoreData.grande_conn import DatabaseManager
import uuid
import logging
from io import BytesIO
from django.http import HttpResponse
from django.core.cache import cache
from django.conf import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("batch_runner.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BatchRunner:
    def __init__(self):
        load_dotenv(override=True)
        self.cias_disponiveis = self._carregar_cias()
        self.meses_disponiveis = parse_meses_opt(os.getenv("MESES_OPT", ""))
        self.cia_originais = ",".join(self.cias_disponiveis)
        logger.info(f"BatchRunner inicializado com {len(self.cias_disponiveis)} CIAs disponíveis")

    def _carregar_cias(self) -> list:
        """Carrega as CIAs disponíveis a partir das variáveis de ambiente"""
        cias_opt = os.getenv("CIAS_OPT", "")
        return [cia.strip() for cia in cias_opt.split(",") if cia.strip()]

    def validar_competencia(self, competencia_str: str) -> tuple:
        """Valida e converte a string de competência no formato MM-AAAA"""
        try:
            mes, ano = map(int, competencia_str.split('-'))
            if 1 <= mes <= 12 and 2000 <= ano <= 2100:
                return (mes, ano)
            raise ValueError("Mês deve ser entre 1-12 e Ano entre 2000-2100")
        except (ValueError, IndexError) as e:
            logger.error(f"Erro na validação da competência: {str(e)}")
            raise ValueError("Formato de competência inválido. Use MM-AAAA (ex: 05-2024)") from e

    def verificar_geracao_anterior(self, cias: list, competencia: str) -> list:
        """
        Verifica se já existem registros para a CIA e competência na tabela mapeada.
        Retorna uma lista com CIAs que já foram processadas.
        """
        logger.info(f"Verificando se já existe geração para as CIAs na competência {competencia}")

        load_dotenv()
        tabela_list = os.getenv("input_history_tables", "").split(",")
        cia_list = os.getenv("cia_corresp", "").split(",")

        mapeamento = dict(zip([cia.strip() for cia in cia_list], [tabela.strip() for tabela in tabela_list]))

        conn = DatabaseManager.get_connection()
        if not conn:
            logger.error("Conexão com o banco de dados indisponível")
            return []

        cias_existentes = []

        try:
            with conn.cursor() as cursor:
                for cia in cias:
                    tabela = mapeamento.get(cia)
                    if not tabela:
                        logger.warning(f"CIA '{cia}' não encontrada no mapeamento. Pulando verificação.")
                        continue

                    query = f"SELECT 1 FROM {tabela} WHERE competencia = %s LIMIT 1;"
                    
                    logger.info(f"Executando query: SELECT 1 FROM {tabela} WHERE competencia = '{competencia}' LIMIT 1;")

                    cursor.execute(query, (competencia,))
                    if cursor.fetchone():
                        logger.warning(f" Já foi gerado Conta Virtual para {cia} na competência {competencia}")
                        cias_existentes.append(cia)

            return cias_existentes

        except Exception as e:
            logger.error(f"Erro ao verificar geração anterior: {str(e)}\n{traceback.format_exc()}")
            return []

        finally:
            DatabaseManager.return_connection(conn)

    def executar_combinações(self, cias: list, competencia_str: str) -> dict:
        """
        Executa o processamento para as combinações de CIA e competência
        
        Args:
            cias: Lista de CIAs selecionadas
            competencia_str: Competência no formato MM-AAAA
            
        Returns:
            Dicionário com resultados do processamento
        """
        logs_sucesso = []
        logs_pulados = []

        
        start_time = time.time()
        logger.info(f"Iniciando processamento para {len(cias)} CIAs e competência {competencia_str}")
        logger.info(f"CIAs selecionadas: {cias}")

        try:
            mes, ano = self.validar_competencia(competencia_str)
            competencia_formatada = f"{mes:02d}-{ano}"

            cias_ja_processadas = self.verificar_geracao_anterior(cias, competencia_formatada)

            for cia in cias_ja_processadas:
                logs_pulados.append(f"[EXISTENTE] {cia} - Conta Virtual já gerada para {competencia_formatada}")


            if set(cias_ja_processadas) == set(cias):
                logger.info("Todas as CIAs informadas já possuem conta virtual gerada para essa competência. Gerando resumo mesmo assim.")

                try:
                    resumo_bytes = self.consulta_resumo_final(cias_ja_processadas, competencia_formatada)
                    safe_competencia = competencia_formatada.replace("-", "")
                    unique_id = f"conta_virtual_{safe_competencia}"
                    resumo_filename = f"{unique_id}.xlsx"
                    resumo_path = os.path.join(settings.MEDIA_ROOT, resumo_filename)

                    if isinstance(resumo_bytes, BytesIO):
                        os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
                        with open(resumo_path, 'wb') as f:
                            f.write(resumo_bytes.read())
                        resumo_status = 'ok'
                    else:
                        resumo_status = 'erro'

                    log_txt = "\n".join(logs_pulados)
                    log_filename = f"{unique_id}.txt"
                    log_path = os.path.join(settings.MEDIA_ROOT, log_filename)

                    os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
                    with open(log_path, 'w', encoding='utf-8') as f:
                        f.write(log_txt)

                    with open(os.path.join(settings.MEDIA_ROOT, f'finished_{unique_id}.txt'), 'w') as f:
                        f.write('ready')

                    return {
                        'status': 'success',
                        'id': unique_id, 
                        'mensagem': 'Todas as CIAs já tinham conta gerada. Resumo final gerado mesmo assim.',
                        'log_execucao': log_filename,
                        'resumo': resumo_status
                    }

                except Exception as e:
                    logger.error(f"Erro ao salvar log e resumo para CIAs já geradas: {str(e)}")
                    return {
                        'status': 'success',
                        'id': unique_id,
                        'mensagem': 'Todas as CIAs já tinham conta gerada, mas houve erro ao salvar log ou resumo.',
                        'log_execucao_error': str(e)
                    }


            cias = [cia for cia in cias if cia not in cias_ja_processadas]
            logger.info(f" CIAs que ainda serão processadas: {cias}")
            
            
            resultados = {}
            df_resumos = []

            for cia in cias:
                chave = f"{cia}_{competencia_formatada}"
                logger.info(f"Processando CIA: {cia} - {competencia_formatada}")

                try:
                    atualizar_config("cia_corresp", cia)
                    atualizar_config("competencia", competencia_formatada)

                    processar_automaticamente(cia, competencia_formatada)
                    os.environ["CIAS_OPT"] = cia

                    importer = DataImporter(cia_manual=cia, competencia_manual=competencia_formatada)
                    success, processed_data = importer.execute_pipeline()

                    resultados[chave] = {
                        'success': success,
                        'data': processed_data
                    }

                    if success:
                        logs_sucesso.append(f"[SUCESSO] {cia} - Conta Virtual gerada para {competencia_formatada}")
                    else:
                        try:
                            from extrato_app.CoreData.consolidador import Consolidador
                            consolidador = Consolidador()
                            caixa_valor, caixa_msg = consolidador.cons_caixa_declarado()
                            
                            if caixa_valor is None:
                                logs_sucesso.append(f"[FALHA] {cia} - Erro durante geração da Conta Virtual para {competencia_formatada} | Motivo: Caixa nao identificado {caixa_msg}")
                            else:
                                logs_sucesso.append(f"[FALHA] {cia} - Erro durante geração da Conta Virtual para {competencia_formatada} | Motivo: Sem Dados Relatório {caixa_valor})")

                        except Exception as e:
                            logs_sucesso.append(f"[FALHA] {cia} - Erro durante geração da Conta Virtual para {competencia_formatada} | Motivo: Caixa não declarado ou Relatório Não disponível")


                    logger.info(f"Processamento concluído para {cia} com {'sucesso' if success else 'falha'}")

                except Exception as e:
                    error_msg = f"Erro ao processar {cia}-{competencia_formatada}: {str(e)}\n{traceback.format_exc()}"
                    logger.error(error_msg)
                    resultados[chave] = {
                        'success': False,
                        'error': error_msg
                    }

            try:
                # Chamar resumo para todas as cias do processo (mesmo que algumas tenham sido puladas)
                resumo_bytes = self.consulta_resumo_final(cias + cias_ja_processadas, competencia_formatada)
                safe_competencia = competencia_formatada.replace("-", "")
                unique_id = f"conta_virtual_{safe_competencia}"
                file_path = os.path.join(settings.MEDIA_ROOT, f'{unique_id}.xlsx')

                if isinstance(resumo_bytes, BytesIO):
                    os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(resumo_bytes.read())
                    resultados['resumo'] = 'ok'
                else:
                    logger.warning("Resumo retornado está vazio ou inválido.")
                    resultados['resumo'] = 'vazio'

            except Exception as e:
                logger.error(f"Erro ao gerar resumo final concatenado: {str(e)}")
                resultados['resumo_error'] = str(e)


            os.environ["CIAS_OPT"] = self.cia_originais

            elapsed_time = time.time() - start_time
            logger.info(f"Processamento concluído em {elapsed_time:.2f} segundos")

            
            try:
                log_txt = "\n".join(logs_pulados + logs_sucesso)
                log_filename = f"{unique_id}.txt"
                log_path = os.path.join(settings.MEDIA_ROOT, log_filename)

                os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
                with open(log_path, 'w', encoding='utf-8') as f:
                    f.write(log_txt)

                resultados['log_execucao'] = log_filename

                with open(os.path.join(settings.MEDIA_ROOT, f'finished_{unique_id}.txt'), 'w') as f:
                    f.write('ready')


            except Exception as e:
                logger.error(f"Erro ao salvar log de inconsistências: {str(e)}")
                resultados['log_execucao_error'] = str(e)
                        
            return {
                'status': 'completed',
                'execution_time': elapsed_time,
                'processed_items': len(cias),
                'results': resultados
            }

        except Exception as e:
            logger.error(f"Erro fatal no processamento: {str(e)}\n{traceback.format_exc()}")
            return {
                'status': 'error',
                'message': str(e),
                'traceback': traceback.format_exc()
            }

    def consulta_resumo_final(self, cias: list, competencia: str) -> dict:
        """
        Consulta o resumo final dos dados processados.
        Agora também inclui CIAs que já têm Conta Virtual gerada previamente.
        """

        logger.info(f"Iniciando geração de resumo final para competência {competencia}")

        load_dotenv()
        tabela_list = os.getenv("input_history_tables", "").split(",")
        cia_list = os.getenv("cia_corresp", "").split(",")

        mapeamento = dict(zip([cia.strip() for cia in cia_list], [tabela.strip() for tabela in tabela_list]))

        cias_existentes = self.verificar_geracao_anterior(cias, competencia)

        todas_cias = list(set(cias + cias_existentes))

        logger.info(f"Consultando resumo para {len(todas_cias)} CIAs (solicitadas + geradas anteriormente)")

        conn = DatabaseManager.get_connection()
        if not conn:
            logger.error("Conexão com o banco de dados indisponível")
            return {"error": "Conexão com o banco de dados indisponível"}

        todos_resultados = []

        try:
            with conn.cursor() as cursor:
                for cia in todas_cias:
                    tabela = mapeamento.get(cia)
                    if not tabela:
                        logger.warning(f"Cia '{cia}' não encontrada no mapeamento, pulando...")
                        continue

                    query = f"""
                        SELECT 
                            id_seguradora_quiver::int4,
                            nome_unidade,
                            COALESCE(SUM(premio_rec), 0)::float8 AS total_premio_rec,
                            COALESCE(SUM(valor_cv), 0)::float8 AS total_cv,
                            COALESCE(SUM(valor_vi), 0)::float8 AS total_vi,
                            COALESCE(SUM(valor_as), 0)::float8 AS total_as
                        FROM 
                            {tabela}
                        WHERE 
                            competencia = %s
                        GROUP BY 
                            id_seguradora_quiver, nome_unidade;
                    """

                    try:
                        cursor.execute(query, (competencia,))
                        resultados = cursor.fetchall()

                        for row in resultados:
                            todos_resultados.append({
                                "cia": cia,
                                "id_seguradora_quiver": row[0],
                                "nome_unidade": row[1],
                                "total_premio_rec": float(row[2]),
                                "total_cv": float(row[3]),
                                "total_vi": float(row[4]),
                                "total_as": float(row[5])
                            })

                    except Exception as e:
                        logger.error(f"Erro ao consultar tabela {tabela} (cia: {cia}): {str(e)}")

            df = pd.DataFrame(todos_resultados)

            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='ResumoFinal')

            output.seek(0)
            return output

        except Exception as e:
            logger.error(f"Erro na consulta final: {str(e)}")
            return {"error": str(e)}

        finally:
            DatabaseManager.return_connection(conn)
