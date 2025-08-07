import os
import json
import threading
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

            resultados = runner.executar_combinações(cias_selected, competencia)

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
    if request.method == "POST":
        cia = request.POST.get("cia")
        mes = request.POST.get("mes")
        valor = request.POST.get("valor")

        config_path = os.path.join(os.getcwd(), "config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump({
                "cia_corresp": cia,
                "competencia": mes
            }, f, indent=2, ensure_ascii=False)

        dba = DBA()
        dba.cia_corresp = cia
        existing, non_existing, lista_cias, id_cia = dba.get_and_compare_cias()

        print("✅ Cias existentes:", existing)
        print("❌ Cias não encontradas:", non_existing)
        print("📄 Lista final de cias:", lista_cias)
        print("🆔 ID da CIA:", id_cia)
        print("💰 Valor a atualizar:", valor)

        return render(request, 'extrato_app/caixa_resultado.html', {
            'existing': existing,
            'non_existing': non_existing,
            'id_cia': id_cia,
            'valor': valor,
            'mes': mes,
        })

    return HttpResponse("Método não permitido", status=405)

@csrf_exempt
def verificar_relatorios_view(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            cias = data.get("cias", [])
            competencia = data.get("competencia", "")

            # Atualiza config.json ANTES de chamar qualquer coisa
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
    if request.method == "POST":
        cia = request.POST.get("cia")
        mes = request.POST.get("mes")
        valor_bruto = request.POST.get("valor_bruto")
        valor_liquido = request.POST.get("valor_liquido")
        forcar_update = request.POST.get("forcar_update", "false") == "true"

        processar_automaticamente(cia, mes)

        dba = DBA()
        existing, non_existing, lista_cias, id_cia = dba.get_and_compare_cias()

        ja_existe = dba.caixa_declarado_existe(cia, mes)

        if ja_existe and not forcar_update:
            return JsonResponse({
                "status": "existe",
                "message": "Já existe valor para esta CIA e competência. Deseja atualizar?",
                "id_cia": id_cia
            })

        if id_cia:
            dba.inserir_ou_atualizar_caixa(
                id_cia=id_cia,
                cia=cia,
                competencia=mes,
                valor_bruto=valor_bruto,
                valor_liquido=valor_liquido,
                update=ja_existe
            )

        return JsonResponse({
            "status": "ok",
            "message": "Dados processados com sucesso.",
            "id_cia": id_cia
        })

    return JsonResponse({"error": "Método não permitido"}, status=405)

    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            cias = data.get("cias", [])
            competencia = data.get("competencia", "")
            dba = DBA()

            resultados = []
            for cia in cias:
                existe = dba.relatorio_existente_para_competencia(cia, competencia)
                resultados.append({
                    "cia": cia,
                    "existe": existe
                })

            return JsonResponse({"status": "success", "dados": resultados})

        except Exception as e:
            return JsonResponse({"status": "error", "mensagem": str(e)}, status=500)