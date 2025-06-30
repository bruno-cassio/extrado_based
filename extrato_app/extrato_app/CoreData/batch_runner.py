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

    def executar_combinações(self, cias: list, competencia_str: str) -> dict:
        """
        Executa o processamento para as combinações de CIA e competência
        
        Args:
            cias: Lista de CIAs selecionadas
            competencia_str: Competência no formato MM-AAAA
            
        Returns:
            Dicionário com resultados do processamento
        """
        start_time = time.time()
        logger.info(f"Iniciando processamento para {len(cias)} CIAs e competência {competencia_str}")

        try:
            mes, ano = self.validar_competencia(competencia_str)
            competencia_formatada = f"{mes:02d}-{ano}"

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

                    # Gera resumo individual SOMENTE SE sucesso
                    if success:
                        resumo_bytes = self.consulta_resumo_final([cia], competencia_formatada)
                        if isinstance(resumo_bytes, BytesIO):
                            resumo_bytes.seek(0)
                            df_cia = pd.read_excel(resumo_bytes)
                            df_cia['cia'] = cia
                            df_resumos.append(df_cia)

                    logger.info(f"Processamento concluído para {cia} com {'sucesso' if success else 'falha'}")

                except Exception as e:
                    error_msg = f"Erro ao processar {cia}-{competencia_formatada}: {str(e)}\n{traceback.format_exc()}"
                    logger.error(error_msg)
                    resultados[chave] = {
                        'success': False,
                        'error': error_msg
                    }

            try:
                if df_resumos:
                    df_final = pd.concat(df_resumos, ignore_index=True)
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_final.to_excel(writer, index=False, sheet_name='ResumoFinal')

                    output.seek(0)
                    unique_id = "_".join(sorted(cias)) + "_" + competencia_formatada.replace("-", "")
                    file_path = os.path.join(settings.MEDIA_ROOT, f'resumo_{unique_id}.xlsx')
                    os.makedirs(settings.MEDIA_ROOT, exist_ok=True)

                    with open(file_path, 'wb') as f:
                        f.write(output.read())

                    with open(os.path.join(settings.MEDIA_ROOT, f'finished_{unique_id}.txt'), 'w') as f:
                        f.write('ready')

                    resultados['resumo'] = 'ok'
                else:
                    logger.warning("Nenhum resumo individual foi gerado.")
                    resultados['resumo'] = 'vazio'

            except Exception as e:
                logger.error(f"Erro ao gerar resumo final concatenado: {str(e)}")
                resultados['resumo_error'] = str(e)

            os.environ["CIAS_OPT"] = self.cia_originais

            elapsed_time = time.time() - start_time
            logger.info(f"Processamento concluído em {elapsed_time:.2f} segundos")

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
        
        """Consulta o resumo final dos dados processados"""


        logger.info(f"Gerando resumo final para {len(cias)} CIAs na competência {competencia}")
        

        load_dotenv()
        tabela_list = os.getenv("input_history_tables", "").split(",")
        cia_list = os.getenv("cia_corresp", "").split(",")

        mapeamento = dict(zip([cia.strip() for cia in cia_list],
                            [tabela.strip() for tabela in tabela_list]))

        conn = DatabaseManager.get_connection()
        if not conn:
            logger.error("Conexão com o banco de dados indisponível")
            return {"error": "Conexão com o banco de dados indisponível"}

        todos_resultados = []

        try:
            with conn.cursor() as cursor:
                for cia in cias:
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
