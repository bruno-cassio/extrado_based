from extrato_app.CoreData.grande_conn import DatabaseManager
from dotenv import load_dotenv
import os
from typing import Optional, Dict, Any
import pandas as pd
import json

load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))

class Consolidador:
    
    TABELAS_CONSULTA = {
        'cont_prod_bare': {
            'premio': 'premio',
            'cia': 'cia',
            'competencia': 'competencia',
            'id_seguradora_quiver': 'id_seguradora_quiver'
        },
        'cont_prod_bdc_saude': {
            'premio': 'premio',
            'cia': 'cia',
            'competencia': 'competencia',
            'id_seguradora_quiver': 'id_seguradora_quiver'
        },
        'cont_prod_suhai': {
            'premio': 'vlr_tarifario',
            'cia': 'cia',
            'competencia': 'competencia',
            'id_seguradora_quiver': 'id_seguradora_quiver'
        },
        'cont_prod_allianz': {
            'premio': 'valor_comissao',
            'cia': 'cia',
            'competencia': 'competencia',
            'id_seguradora_quiver': 'id_seguradora_quiver'
        }
    }

    def __init__(self):
        config_path = os.path.join(os.getcwd(), "config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                try:
                    config = json.load(f)
                    self.competencia = config.get("competencia", "")
                    self.cia_corresp = config.get("cia_corresp", "")
                except json.JSONDecodeError:
                    self.competencia = ""
                    self.cia_corresp = ""
        except FileNotFoundError:
            self.competencia = ""
            self.cia_corresp = ""
        
    def cons_caixa_declarado(self) -> Optional[float]:

        conn = None
        try:
            conn = DatabaseManager.get_connection()
            if not conn:
                print("Falha ao conectar ao banco de dados.")
                return None

            if not self.cia_corresp or not self.competencia:
                print("Variáveis 'cia_corresp' ou 'competencia' não encontradas.")
                return None

            if self.cia_corresp in ['Porto']:
                query = f"""
                SELECT valor_liq_declarado 
                FROM caixa_declarado
                WHERE cia = 'Porto Seguro' AND competencia = '{self.competencia}';
                """
                print('query:', query)

            elif self.cia_corresp in ['Axa']:
                query = f"""
                SELECT valor_bruto_declarado
                FROM caixa_declarado
                WHERE cia = 'Axa' AND competencia = '{self.competencia}';
                """
                print('query:', query)

            else:
                query = f"""
                SELECT valor_liq_declarado 
                FROM caixa_declarado
                WHERE cia = '{self.cia_corresp}' AND competencia = '{self.competencia}';
                """
                print('query:', query)

            with conn.cursor() as cursor:
                cursor.execute(query)
                resultado = cursor.fetchone()
                if resultado:
                    caixa = resultado[0]
                    print(f"Caixa declarado (DB): {caixa}")
                    return float(caixa)
                else:
                    print("Nenhum valor encontrado para 'valor_liq_declarado'.")
                    return None

        except Exception as e:
            print(f"\nErro ao consultar caixa declarado: {e}")
            return None

        finally:
            if conn:
                DatabaseManager.return_connection(conn)
                print("\nConexão encerrada com sucesso.")

    def get_tabelas_config(self) -> Dict[str, Any]:
        return self.TABELAS_CONSULTA

def main() -> Optional[float]:
    consolidador = Consolidador()
    return consolidador.cons_caixa_declarado()

if __name__ == "__main__":
    main()