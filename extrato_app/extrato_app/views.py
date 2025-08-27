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
from dotenv import dotenv_values
from django.shortcuts import render, redirect
from extrato_app.CoreData.dba import DBA, DatabaseManager
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.hashers import check_password, make_password


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
    env = dotenv_values()
    cias_opt_raw = env.get("CIAS_OPT", "")
    cias_opt = [c.strip() for c in cias_opt_raw.split(",") if c.strip()]
    return render(request, "atualizar_relatorios.html", {"cias_opt": cias_opt})
    
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

@csrf_exempt
def api_atualizar_relatorios(request):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'mensagem': 'Método não permitido'}, status=405)
    try:
        payload = json.loads(request.body.decode('utf-8'))
        cias = payload.get('cias', [])
        competencia = payload.get('competencia')
        audit_event_id = payload.get('audit_event_id')

        if not cias or not competencia:
            return JsonResponse({'status': 'error', 'mensagem': 'Campos obrigatórios ausentes'}, status=400)

        user_name = getattr(request.user, "username", None) or "bruno.cassio"

        runner = BatchRunner()
        result = runner.executar_atualizacao_relatorios(
            cias, competencia,
            user_name=user_name,
            audit_event_id=audit_event_id
        )
        return JsonResponse(result, status=200)
    except Exception as e:
        return JsonResponse({'status': 'error', 'mensagem': str(e)}, status=500)
    
def login_page(request):
    if request.session.get("user_id"):
        return redirect("/")
    return render(request, "login.html")

@require_POST
@csrf_protect
def auth_login(request):
    """
    Processa o login:
      - busca usuário por username (case-insensitive)
      - compara hash de senha (PBKDF2 do Django)
      - atualiza last_login_at e zera failed_attempts em caso de sucesso
      - grava auditoria em extrato_audit
      - cria sessão (request.session['user_*'])
    """
    dba = DBA()
    try:
        data = json.loads(request.body.decode("utf-8"))
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""
        audit_event_id = data.get("audit_event_id")

        if not username or not password:
            return JsonResponse({"status": "error", "mensagem": "Informe usuário e senha."}, status=400)

        conn = DatabaseManager.get_connection()
        if not conn:
            return JsonResponse({"status": "error", "mensagem": "Banco indisponível."}, status=503)

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, username, email, full_name, password_hash, is_active, failed_attempts
                    FROM public.app_users
                    WHERE LOWER(username) = LOWER(%s)
                    LIMIT 1
                """, (username,))
                row = cur.fetchone()

                if not row or not row[5]:
                    # auditoria de tentativa inválida
                    dba.registrar_auditoria(
                        payload={
                            "action": "user_login",
                            "audit_event_id": audit_event_id,
                            "user_name_attempt": username,
                            "status": "error",
                            "reason": "user_not_found_or_inactive",
                            "ip": request.META.get("REMOTE_ADDR"),
                            "ua": request.META.get("HTTP_USER_AGENT"),
                        },
                        summary=f"Login • {username} • ERRO (user not found or inactive)",
                        user_name=username or "anon",
                    )
                    return JsonResponse({"status": "error", "mensagem": "Credenciais inválidas."}, status=401)

                user_id, db_username, email, full_name, pwd_hash, is_active, failed_attempts = row

                if not check_password(password, pwd_hash):
                    # incrementa failed_attempts
                    cur.execute("""
                        UPDATE public.app_users
                           SET failed_attempts = COALESCE(failed_attempts,0) + 1,
                               updated_at = NOW()
                         WHERE id = %s
                    """, (user_id,))
                    conn.commit()

                    dba.registrar_auditoria(
                        payload={
                            "action": "user_login",
                            "audit_event_id": audit_event_id,
                            "user_id": user_id,
                            "user_name": db_username,
                            "status": "error",
                            "reason": "invalid_password",
                            "ip": request.META.get("REMOTE_ADDR"),
                            "ua": request.META.get("HTTP_USER_AGENT"),
                        },
                        summary=f"Login • {db_username} • ERRO (invalid password)",
                        user_name=db_username,
                    )
                    return JsonResponse({"status": "error", "mensagem": "Usuário ou senha inválidos."}, status=401)

                # sucesso: atualiza last_login, zera failed_attempts
                cur.execute("""
                    UPDATE public.app_users
                       SET last_login_at = NOW(),
                           failed_attempts = 0,
                           updated_at = NOW()
                     WHERE id = %s
                """, (user_id,))
                conn.commit()

            request.session["user_id"] = user_id
            request.session["user_name"] = db_username
            request.session["full_name"] = full_name
            request.session.set_expiry(60 * 60 * 8)

            dba.registrar_auditoria(
                payload={
                    "action": "user_login",
                    "audit_event_id": audit_event_id,
                    "user_id": user_id,
                    "user_name": db_username,
                    "status": "success",
                    "ip": request.META.get("REMOTE_ADDR"),
                    "ua": request.META.get("HTTP_USER_AGENT"),
                },
                summary=f"Login • {db_username} • SUCESSO",
                user_name=db_username,
            )

            return JsonResponse({"status": "ok", "redirect": "/"}, status=200)
        finally:
            DatabaseManager.return_connection(conn)

    except Exception as e:
        dba.registrar_auditoria(
            payload={
                "action": "user_login",
                "status": "error",
                "error": str(e),
            },
            summary="Login • EXCEPTION",
            user_name="system",
        )
        return JsonResponse({"status": "error", "mensagem": "Erro inesperado."}, status=500)

@require_POST
@csrf_protect
def auth_request_reset(request):
    """
    Recebe e-mail, gera reset_token + reset_expires_at (~2h),
    registra auditoria. (Envio de e-mail fica a seu critério.)
    """
    dba = DBA()
    try:
        data = json.loads(request.body.decode("utf-8"))
        email = (data.get("email") or "").strip()
        audit_event_id = data.get("audit_event_id")

        if not email:
            return JsonResponse({"status": "error", "mensagem": "Informe o e-mail."}, status=400)

        conn = DatabaseManager.get_connection()
        if not conn:
            return JsonResponse({"status": "error", "mensagem": "Banco indisponível."}, status=503)

        try:
            token = str(uuid.uuid4())
            expires = timezone.now() + timedelta(hours=2)
            updated = 0

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.app_users
                       SET reset_token = %s,
                           reset_expires_at = %s,
                           updated_at = NOW()
                     WHERE LOWER(email) = LOWER(%s)
                     RETURNING id, username
                """, (token, expires, email))
                row = cur.fetchone()
                if row:
                    updated = 1
                    user_id, username = row

            conn.commit()

            # auditoria (não revele se o e-mail existe)
            dba.registrar_auditoria(
                payload={
                    "action": "user_reset_request",
                    "audit_event_id": audit_event_id,
                    "email": email,
                    "updated": bool(updated),
                    "status": "success" if updated else "noop",
                },
                summary=f"Reset Request • {email} • {'ENVIADO' if updated else 'NOOP'}",
                user_name=email,
            )

            # resposta neutra (evita enumeração)
            return JsonResponse({"status": "ok"}, status=200)
        finally:
            DatabaseManager.return_connection(conn)

    except Exception as e:
        dba.registrar_auditoria(
            payload={"action": "user_reset_request", "status": "error", "error": str(e)},
            summary="Reset Request • EXCEPTION",
            user_name="system",
        )
        return JsonResponse({"status": "error", "mensagem": "Erro inesperado."}, status=500)

def auth_logout(request):
    # limpa sessão e redireciona para login
    request.session.flush()
    return redirect("/login")