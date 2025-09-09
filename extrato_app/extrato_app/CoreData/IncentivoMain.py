import traceback
from extrato_app.CoreData.data_handler import DataHandler
from extrato_app.CoreData.ds4 import processar_automaticamente
from extrato_app.CoreData.dba import DBA


class IncentivoImporter:
    """
    Orquestrador dedicado ao fluxo de Incentivo.
    Executa leitura via dispatcher para a CIA e competência informadas.
    """

    def __init__(self, cia_manual: str, competencia_manual: str):
        self.cia_escolhida = cia_manual
        self.competencia = competencia_manual
        self.data_handler = DataHandler()
        self.dba = DBA()

        try:
            processar_automaticamente(self.cia_escolhida, self.competencia)
        except Exception as e:
            print(f"⚠️ Falha ao processar config para incentivo: {e}")

    def execute_pipeline(self):
        """
        Executa o pipeline de incentivo:
          - Lê o incentivo via dispatcher
          - Enriquecimento de dados (feito nos handlers + dba)
          - Retorna DataFrame consolidado ou erro
        """
        try:
            print(f"▶️ Iniciando IncentivoImporter para {self.cia_escolhida} ({self.competencia})")

            df_incentivo = self.data_handler.read_incentivo_via_dispatcher(self.cia_escolhida)

            if df_incentivo is None or df_incentivo.empty:
                msg = f"Nenhum dado de incentivo encontrado para {self.cia_escolhida} {self.competencia}"
                print(f"⚠️ {msg}")
                return False, {"msg": msg, "rows": 0}

            try:
                self.dba.registrar_auditoria(
                    payload={
                        "event": "incentivo.pipeline",
                        "cia": self.cia_escolhida,
                        "competencia": self.competencia,
                        "rows": len(df_incentivo)
                    },
                    summary=f"Incentivo {self.cia_escolhida} {self.competencia} • {len(df_incentivo)} linhas"
                )
            except Exception as e:
                print(f"⚠️ Falha ao registrar auditoria do incentivo: {e}")

            print(f"✅ Incentivo concluído: {len(df_incentivo)} linhas para {self.cia_escolhida} {self.competencia}")
            return True, {"msg": "Cálculo de incentivo concluído com sucesso.", "rows": len(df_incentivo)}

        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Erro no IncentivoImporter: {e}\n{tb}")
            return False, {"msg": f"Erro no incentivo: {e}", "rows": 0}
