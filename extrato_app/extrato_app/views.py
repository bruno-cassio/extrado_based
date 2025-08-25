import os
import json
import threading
from urllib import request
from django.shortcuts import render
from django.http import JsonResponse
from extrato_app.CoreData.batch_runner import BatchRunner
from pprint import pprint
from django.http import HttpResponse
from extrato_app.CoreData.batch_runner import BatchRunner
from django.conf import settings
from django.http import JsonResponse
import pandas as pd
import threading
from django.conf import settings
from extrato_app.CoreData.batch_runner import BatchRunner
from django.core.cache import cache
from io import BytesIO
from django.http import HttpResponse
from django.http import FileResponse, Http404
from django.http import JsonResponse
import time
from django.shortcuts import render
from extrato_app.CoreData.dba import DBA
from django.views.decorators.csrf import csrf_exempt
from extrato_app.CoreData.ds4 import processar_automaticamente
import uuid


arquivos_em_memoria = {} 

def index(request):
    cias_opt = os.getenv("CIAS_OPT", "")
    cias_list = [cia.strip() for cia in cias_opt.split(",") if cia.strip()]
    
    return render(request, 'index.html', {
        'cias_opt': cias_list
    })

def limpar_arquivos(request):
    if request.method == "POST":
        media_dir = settings.MEDIA_ROOT
        try:
            for file_name in os.listdir(media_dir):
                file_path = os.path.join(media_dir, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return JsonResponse({'status': 'ok'})
        except Exception as e:
            return JsonResponse({'erro': str(e)}, status=500)
    else:
        return JsonResponse({'erro': 'Método não permitido'}, status=405)

def iniciar_extracao(request):
    if request.method == 'POST':
        cias_selected = json.loads(request.POST.get('cias_selected', '[]'))
        competencia = request.POST.get('mes', '')
        
        if not cias_selected or not competencia:
            return JsonResponse({'status': 'error', 'message': 'Selecione pelo menos uma CIA e informe a competência'})

        competencia_id = competencia.replace("-", "")
        unique_id = f"conta_virtual_{competencia_id}"

        try:
            runner = BatchRunner()

            resultados = runner.executar_combinacoes(cias_selected, competencia)

            if resultados.get("status") not in ["completed", "success"]:
                return JsonResponse({
                    'status': 'error',
                    'message': 'Erro durante o processamento',
                    'detalhes': resultados
                })

            xlsx_filename = f"{unique_id}.xlsx"
            txt_filename = f"{unique_id}.txt"
            xlsx_path = os.path.join(settings.MEDIA_ROOT, xlsx_filename)
            txt_path = os.path.join(settings.MEDIA_ROOT, txt_filename)

            if not os.path.exists(xlsx_path) or not os.path.exists(txt_path):
                return JsonResponse({
                    'status': 'error',
                    'message': 'Arquivos de saída não encontrados.'
                })

            return JsonResponse({
                'status': 'success',
                'message': 'Processamento finalizado com sucesso!',
                'id': unique_id,
                'xlsx_url': f"/media/{xlsx_filename}",
                'txt_url': f"/media/{txt_filename}"
            })

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

    return JsonResponse({'status': 'error', 'message': 'Método não permitido'}, status=405)

def baixar_resumo(request):
    unique_id = request.GET.get('id')
    if not unique_id:
        raise Http404("ID não especificado.")

    file_path = os.path.join(settings.MEDIA_ROOT, f"{unique_id}.xlsx")

    if os.path.exists(file_path):
        return FileResponse(open(file_path, 'rb'), as_attachment=True, filename=f"{unique_id}.xlsx")
    else:
        raise Http404("Arquivo não encontrado.")

def atualizar_relatorios(request):
    cias_opt = os.getenv("CIAS_OPT", "")
    cias_list = [cia.strip() for cia in cias_opt.split(",") if cia.strip()]
    
    return render(request, 'atualizar_relatorios.html', {
        'cias_opt': cias_list
    })
    
def atualizar_caixa(request):
    cias_raw = os.getenv("CIAS_OPT", "")
    cias_opt = [cia.strip() for cia in cias_raw.split(",") if cia.strip()]
    return render(request, 'atualizar_caixa.html', {'cias_opt': cias_opt})

def executar_atualizar_caixa(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Método não permitido"}, status=405)

    try:
        mes = (request.POST.get("mes") or "").strip()
        if not mes:
            return JsonResponse({"status": "error", "message": "Informe a competência (MM-AAAA)."}, status=400)

        cias_selected_raw = request.POST.get("cias_selected", "").strip()
        if cias_selected_raw:
            try:
                cias = json.loads(cias_selected_raw)
            except json.JSONDecodeError:
                return JsonResponse({"status": "error", "message": "Formato inválido em 'cias_selected'."}, status=400)

            if not isinstance(cias, list) or not cias:
                return JsonResponse({"status": "error", "message": "Selecione ao menos uma CIA."}, status=400)

            dba = DBA()
            processed, not_found = [], []

            for cia in cias:
                id_cia = dba.get_id_cia(cia)
                if not id_cia:
                    not_found.append(cia)
                    continue

                vb_raw = (request.POST.get(f"valor_bruto_{cia}", "0") or "").replace(".", "").replace(",", ".")
                vl_raw = (request.POST.get(f"valor_liquido_{cia}", "0") or "").replace(".", "").replace(",", ".")

                dba.inserir_ou_atualizar_caixa(
                    id_cia=id_cia,
                    cia=cia,
                    competencia=mes,
                    valor_bruto=vb_raw,
                    valor_liquido=vl_raw,
                    update=True
                )
                processed.append({"cia": cia, "id_cia": id_cia, "valor_bruto": vb_raw, "valor_liquido": vl_raw})

            msg = f"{len(processed)} CIAs atualizadas para {mes}."
            if not_found:
                msg += f" Não encontradas: {', '.join(not_found)}."
            return JsonResponse({"status": "success", "message": msg, "processed": processed, "not_found": not_found})

        cia = (request.POST.get("cia") or "").strip()
        valor = (request.POST.get("valor") or "").strip()
        if not cia or not valor:
            return JsonResponse({"status": "error", "message": "Informe 'cia' e 'valor' ou use o modal com múltiplas CIAs."}, status=400)

        dba = DBA()
        id_cia = dba.get_id_cia(cia)
        if not id_cia:
            return JsonResponse({"status": "error", "message": f"CIA '{cia}' não encontrada."}, status=404)

        valor_norm = valor.replace(".", "").replace(",", ".")
        dba.inserir_ou_atualizar_caixa(
            id_cia=id_cia, cia=cia, competencia=mes,
            valor_bruto=valor_norm, valor_liquido=valor_norm, update=True
        )
        return JsonResponse({"status": "success", "message": f"Atualizado {cia} para {mes}."})

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
def verificar_relatorios_view(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            cias = data.get("cias", [])
            competencia = data.get("competencia", "")

            for cia in cias:
                processar_automaticamente(cia, competencia)

            dba = DBA()
            resultados = []
            for cia in cias:
                existe = dba.relatorio_existente_para_competencia(cia, competencia)
                resultados.append({"cia": cia, "existe": existe})

            return JsonResponse({"status": "success", "dados": resultados})
        
        except Exception as e:
            return JsonResponse({"status": "error", "mensagem": str(e)}, status=500)

@csrf_exempt
def buscar_cias_api(request):
    if request.method != "POST":
        return JsonResponse({"error": "Método não permitido"}, status=405)

    try:
        cias_raw = request.POST.get("cias", "[]")
        mes = request.POST.get("mes")
        forcar_update = request.POST.get("forcar_update", "false") == "true"

        audit_event_id = request.POST.get("audit_event_id") or str(uuid.uuid4())
        user_name = "bruno.cassio"

        cias = json.loads(cias_raw) if isinstance(cias_raw, str) else cias_raw
        if not isinstance(cias, list) or not cias:
            return JsonResponse({"status": "error", "message": "Selecione ao menos uma CIA."}, status=400)
        if not mes:
            return JsonResponse({"status": "error", "message": "Informe a competência (MM-AAAA)."}, status=400)

        dba = DBA()

        ja_existem = []
        nao_existem = []
        for cia in cias:
            if dba.caixa_declarado_existe(cia, mes):
                ja_existem.append(cia)
            else:
                nao_existem.append(cia)

        try:
            dba.registrar_auditoria(
                payload={
                    "event": "caixa.precheck",
                    "event_id": audit_event_id,
                    "user": user_name,
                    "mes": mes,
                    "cias": cias,
                    "resultado": {"ja_existem": ja_existem, "nao_existem": nao_existem}
                },
                summary=f"[precheck] user={user_name} mes={mes} ja_existem={len(ja_existem)} nao_existem={len(nao_existem)}"
            )
        except Exception as _:
            pass

        cias_inseridas = []
        insercoes_payload = []

        for cia in nao_existem:
            id_cia = dba.get_id_cia(cia)
            if not id_cia:
                continue

            valor_bruto = request.POST.get(f"valor_bruto_{cia}", "0")
            valor_liquido = request.POST.get(f"valor_liquido_{cia}", "0")

            dba.inserir_ou_atualizar_caixa(
                id_cia=id_cia,
                cia=cia,
                competencia=mes,
                valor_bruto=valor_bruto,
                valor_liquido=valor_liquido,
                update=False
            )
            cias_inseridas.append(cia)
            insercoes_payload.append({
                "cia": cia,
                "competencia": mes,
                "novos_valores": {
                    "valor_bruto": valor_bruto,
                    "valor_liquido": valor_liquido
                }
            })

        if insercoes_payload:
            try:
                dba.registrar_auditoria(
                    payload={
                        "event": "caixa.insert",
                        "event_id": audit_event_id,
                        "user": user_name,
                        "mes": mes,
                        "insercoes": insercoes_payload
                    },
                    summary=f"[insert] user={user_name} mes={mes} inseridas={len(insercoes_payload)}"
                )
            except Exception as _:
                pass

        if ja_existem and not forcar_update:
            return JsonResponse({
                "status": "existe",
                "message": "Já existe valor para algumas CIAs. Deseja atualizar esses registros?",
                "cias_existentes": ja_existem,
                "cias_inseridas": cias_inseridas
            })

        if ja_existem and forcar_update:
            changes = []
            for cia in ja_existem:
                id_cia = dba.get_id_cia(cia)
                if not id_cia:
                    continue

                antes = dba.obter_caixa_declarado(cia, mes)

                valor_bruto = request.POST.get(f"valor_bruto_{cia}", "0")
                valor_liquido = request.POST.get(f"valor_liquido_{cia}", "0")

                dba.inserir_ou_atualizar_caixa(
                    id_cia=id_cia,
                    cia=cia,
                    competencia=mes,
                    valor_bruto=valor_bruto,
                    valor_liquido=valor_liquido,
                    update=True
                )

                changes.append({
                    "cia": cia,
                    "antes": antes,
                    "depois": {
                        "id_seguradora_quiver": id_cia,
                        "cia": cia,
                        "competencia": mes,
                        "valor_bruto_declarado": valor_bruto,
                        "valor_liq_declarado": valor_liquido
                    }
                })

            try:
                dba.registrar_auditoria(
                    payload={
                        "event": "caixa.update",
                        "event_id": audit_event_id,
                        "user": user_name,
                        "mes": mes,
                        "changes": changes
                    },
                    summary=f"[update] user={user_name} mes={mes} atualizadas={len(changes)}"
                )
            except Exception as _:
                pass

        return JsonResponse({
            "status": "ok",
            "message": "Dados atualizados.",
            "cias_inseridas": cias_inseridas,
            "cias_atualizadas": ja_existem if forcar_update else []
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
def consultar_caixa_api(request):
    if request.method != "POST":
        return JsonResponse({"error": "Método não permitido"}, status=405)

    try:
        mes = (request.POST.get("mes") or "").strip()
        if not mes:
            return JsonResponse({"status": "error", "message": "Informe a competência (MM-AAAA)."}, status=400)

        dba = DBA()
        dados = dba.consultar_caixa_por_competencia(mes)

        try:
            dba.registrar_auditoria(
                payload={
                    "event": "caixa.consulta",
                    "event_id": str(uuid.uuid4()),
                    "user": "bruno.cassio",
                    "mes": mes,
                    "result_count": len(dados or [])
                },
                summary=f"[consulta] user=bruno.cassio mes={mes} rows={len(dados or [])}"
            )
        except Exception as _:
            pass

        return JsonResponse({"status": "ok", "dados": dados})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

