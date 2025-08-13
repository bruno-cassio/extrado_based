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
from extrato_app.CoreData.Handlers.SwissHandler import SwissHandler

CIA_HANDLERS = {
    "Bradesco": BradescoHandler(),
    "Suhai": SuhaiHandler(),
    "Allianz": AllianzHandler(),
    "Junto Seguradora": JuntoHandler(),
    "Hdi": HDIHandler(),
    "Porto": PortoHandler(),
    "Bradesco Saude": BradescoSaudeHandler(),
    "Yelum": YelumHandler(),
    "Axa": AxaHandler(),
    "Zurich": ZurichHandler(),
    "Chubb": ChubbHandler(),
    "Tokio": TokioHandler(),
    "Ezze": EzzeHandler(),
    "Sompo": SompoHandler(),
    "Mapfre": MapfreHandler(),
    "Swiss": SwissHandler()
}

# CIA_PROCESS_DISPATCHER = {
#     cia: handler.process for cia, handler in CIA_HANDLERS.items()
# }
