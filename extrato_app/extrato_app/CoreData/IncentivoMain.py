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
    aplica enriquecimento de unidades e insere dados na tabela incentivo_geral.
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

    def _convert_df_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        df_conv = df.copy()

        expected_cols = [
            "nome_unidade", "id_unidade", "id_cor_cliente", "competencia",
            "valor_incentivo", "tipo_fonte", "origem_arquivo", "cia"
        ]
        for col in expected_cols:
            if col not in df_conv.columns:
                df_conv[col] = None

        df_conv["nome_unidade"] = df_conv["nome_unidade"].astype(str)

        for col in ["id_unidade", "id_cor_cliente"]:
            df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").astype("Int64")
            df_conv[col] = df_conv[col].apply(
                lambda x: x if (x is not None and pd.notna(x) and -2147483648 <= int(x) <= 2147483647) else None
            )

        df_conv["competencia"] = df_conv["competencia"].astype(str)
        df_conv["valor_incentivo"] = pd.to_numeric(df_conv["valor_incentivo"], errors="coerce")
        df_conv["tipo_fonte"] = df_conv["tipo_fonte"].astype(str)
        df_conv["origem_arquivo"] = df_conv["origem_arquivo"].astype(str)
        df_conv["cia"] = df_conv["cia"].astype(str)

        df_conv = df_conv.where(pd.notnull(df_conv), None)

        return df_conv


    def _import_to_db(self, df: pd.DataFrame):
        conn = self._create_connection()
        try:
            df_conv = self._convert_df_to_schema(df)
            cols = [
                "nome_unidade", "id_unidade", "id_cor_cliente", "competencia",
                "valor_incentivo", "tipo_fonte", "origem_arquivo", "cia"
            ]

            values = (
                df_conv[cols]
                .applymap(lambda x: None if pd.isna(x) else x)
                .values.tolist()
            )

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"INSERT INTO {self.TABLE_NAME} ({', '.join(cols)}) VALUES %s",
                    values,
                )
            conn.commit()
            print(f"💾 {len(values)} registros importados em {self.TABLE_NAME}")
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
