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

arquivos_em_memoria = {} 

def index(request):
    cias_opt = os.getenv("CIAS_OPT", "")
    cias_list = [cia.strip() for cia in cias_opt.split(",") if cia.strip()]
    
    return render(request, 'index.html', {
        'cias_opt': cias_list
    })

def limpar_arquivos(request):
    if request.method == "POST":
        unique_id = request.GET.get('id')
        if not unique_id:
            return JsonResponse({'erro': 'ID inválido'}, status=400)

        txt_path = os.path.join('media', f'finished_{unique_id}.txt')
        xlsx_path = os.path.join('media', f'resumo_{unique_id}.xlsx')

        for path in [txt_path, xlsx_path]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                return JsonResponse({'erro': str(e)}, status=500)

        return JsonResponse({'status': 'ok'})

def iniciar_extracao(request):
    if request.method == 'POST':
        cias_selected = json.loads(request.POST.get('cias_selected', '[]'))
        competencia = request.POST.get('mes', '')
        
        if not cias_selected or not competencia:
            return JsonResponse({'status': 'error', 'message': 'Selecione pelo menos uma CIA e informe a competência'})

        unique_id = "_".join(sorted(cias_selected)) + "_" + competencia.replace("-", "")
        def run_batch():
            try:
                runner = BatchRunner()
                resultados = runner.executar_combinações(cias_selected, competencia)

                resumo = resultados.get('resumo')
                df = pd.DataFrame(resumo) if not isinstance(resumo, pd.DataFrame) else resumo

                output = BytesIO()
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='ResumoFinal')

                output.seek(0)

                arquivos_em_memoria[unique_id] = output.read()

                flag_path = os.path.join(settings.MEDIA_ROOT, f'finished_{unique_id}.txt')
                with open(flag_path, 'w') as f:
                    f.write('ok')

                print("✅ Processo finalizado com arquivo em memória para download")

            except Exception as e:
                print(f"❌ Erro no batch: {str(e)}")

        thread = threading.Thread(target=run_batch)
        thread.start()

        return JsonResponse({
            'status': 'success',
            'message': 'Extração iniciada em segundo plano! Você será notificado quando concluir.',
            'id': unique_id
        })

    return JsonResponse({'status': 'error', 'message': 'Método não permitido'}, status=405)


def baixar_resumo(request):
    unique_id = request.GET.get('id')
    if not unique_id:
        raise Http404("ID não especificado.")

    file_path = os.path.join(
        settings.MEDIA_ROOT,
        f'resumo_{unique_id}.xlsx'
    )

    if os.path.exists(file_path):
        return FileResponse(open(file_path, 'rb'), as_attachment=True, filename=f'resumo_{unique_id}.xlsx')
    else:
        raise Http404("Arquivo não encontrado.")