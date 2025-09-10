import traceback
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from extrato_app.CoreData.data_handler import DataHandler
from extrato_app.CoreData.ds4 import processar_automaticamente
from extrato_app.CoreData.dba import DBA
import os


class IncentivoImporter:
    """
    Orquestrador dedicado ao fluxo de Incentivo.
    Executa leitura via dispatcher para a CIA e competência informadas,
    aplica enriquecimento de unidades, gera version_id e insere dados na tabela incentivo_geral.
    """

    TABLE_NAME = "incentivo_geral"

    def __init__(self, cia_manual: str, competencia_manual: str, user_name: str = "system"):
        self.cia_escolhida = cia_manual
        self.competencia = competencia_manual
        self.user_name = user_name
        self.data_handler = DataHandler()
        self.dba = DBA()

        try:
            processar_automaticamente(self.cia_escolhida, self.competencia)
        except Exception as e:
            print(f"⚠️ Falha ao processar config para incentivo: {e}")

    def _create_connection(self):
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )

    def _get_next_version_id(self, conn) -> int:
        
        """Consulta o maior version_id existente para cia+competencia e retorna o próximo"""
        
        query = f"""
            SELECT COALESCE(MAX(version_id), 0)
            FROM {self.TABLE_NAME}
            WHERE cia = %s AND competencia = %s
        """
        with conn.cursor() as cur:
            cur.execute(query, (self.cia_escolhida, self.competencia))
            max_version = cur.fetchone()[0] or 0
        return max_version + 1

    def _convert_df_to_schema(self, df: pd.DataFrame, version_id: int) -> pd.DataFrame:
        """Força o DataFrame para os tipos esperados e adiciona version_id"""
        df_conv = df.copy()

        expected_cols = [
            "nome_unidade", "id_unidade", "id_cor_cliente", "competencia",
            "valor_incentivo", "tipo_fonte", "origem_arquivo", "cia", "version_id"
        ]
        for col in expected_cols:
            if col not in df_conv.columns:
                df_conv[col] = None

        df_conv["nome_unidade"] = df_conv["nome_unidade"].astype(str)
        df_conv["id_unidade"] = pd.to_numeric(df_conv["id_unidade"], errors="coerce").astype("Int64")
        df_conv["id_cor_cliente"] = pd.to_numeric(df_conv["id_cor_cliente"], errors="coerce").astype("Int64")
        df_conv["competencia"] = str(self.competencia)
        df_conv["cia"] = str(self.cia_escolhida)
        df_conv["valor_incentivo"] = pd.to_numeric(df_conv["valor_incentivo"], errors="coerce")
        df_conv["tipo_fonte"] = df_conv["tipo_fonte"].astype(str)
        df_conv["origem_arquivo"] = df_conv["origem_arquivo"].astype(str)

        df_conv["version_id"] = int(version_id)

        return df_conv.where(pd.notnull(df_conv), None)

    def _import_to_db(self, df: pd.DataFrame):
        conn = self._create_connection()
        try:
            version_id = self._get_next_version_id(conn)
            print(f"🆕 Próximo version_id definido: {version_id}")

            df_conv = self._convert_df_to_schema(df, version_id)

            cols = [
                "nome_unidade", "id_unidade", "id_cor_cliente", "competencia",
                "valor_incentivo", "tipo_fonte", "origem_arquivo", "cia", "version_id"
            ]
            values = df_conv[cols].astype(object).where(pd.notnull(df_conv[cols]), None).values.tolist()

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"INSERT INTO {self.TABLE_NAME} ({', '.join(cols)}) VALUES %s",
                    values,
                )
            conn.commit()
            print(f"💾 {len(values)} registros importados em {self.TABLE_NAME} (version_id={version_id})")
        finally:
            conn.close()

    def execute_pipeline(self):
        try:
            print(f"▶️ Iniciando IncentivoImporter para {self.cia_escolhida} ({self.competencia})")

            df_incentivo = self.data_handler.read_incentivo_via_dispatcher(self.cia_escolhida)

            if df_incentivo is None or df_incentivo.empty:
                msg = f"Nenhum dado de incentivo encontrado para {self.cia_escolhida} {self.competencia}"
                print(f"⚠️ {msg}")
                return False, {"msg": msg, "rows": 0}

            # Auditoria
            try:
                self.dba.registrar_auditoria(
                    payload={
                        "event": "incentivo.pipeline",
                        "cia": self.cia_escolhida,
                        "competencia": self.competencia,
                        "rows": len(df_incentivo),
                        "columns": list(df_incentivo.columns),
                    },
                    summary=f"[incentivo] cia={self.cia_escolhida} comp={self.competencia} rows={len(df_incentivo)}",
                    user_name=self.user_name,
                )
            except Exception as e:
                print(f"⚠️ Falha ao registrar auditoria do incentivo: {e}")

            # Importação
            try:
                self._import_to_db(df_incentivo)
            except Exception as e:
                print(f"❌ Erro ao importar incentivo_geral: {e}")

            print(f"✅ Incentivo concluído: {len(df_incentivo)} linhas para {self.cia_escolhida} {self.competencia}")
            return True, {"msg": "Cálculo de incentivo concluído com sucesso.", "rows": len(df_incentivo)}

        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Erro no IncentivoImporter: {e}\n{tb}")
            return False, {"msg": f"Erro no incentivo: {e}", "rows": 0}
