import os, json, uuid, threading, time
from datetime import timedelta
from functools import wraps

from django.conf import settings
from django.shortcuts import render, redirect
from django.urls import reverse
from django.http import (
    JsonResponse, HttpResponse, FileResponse, Http404, HttpResponseNotAllowed
)
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.views.decorators.http import require_POST, require_http_methods
from django.utils import timezone
from django.contrib.auth.hashers import check_password, make_password
from django.template.loader import render_to_string
from django.core.mail import send_mail
from dotenv import dotenv_values
from extrato_app.CoreData.dba import DBA, audit_event
from extrato_app.CoreData.grande_conn import DatabaseManager
from extrato_app.CoreData.batch_runner import BatchRunner
from extrato_app.CoreData.ds4 import processar_automaticamente
from django.core import signing


def _load_user(username: str):
    if not username:
        return None
    conn = DatabaseManager.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, email, is_active
                  FROM public.app_users
                 WHERE LOWER(username)=LOWER(%s)
                 LIMIT 1
            """, (username,))
            row = cur.fetchone()
            if not row or not row[3]:
                return None
            return {"id": row[0], "username": row[1], "email": row[2]}
    finally:
        DatabaseManager.return_connection(conn)

def login_required_view(view=None, *, allow_json=False):
    """
    Exige cookie 'auth_user' válido nas views protegidas.
    Se allow_json=True, retorna 401 JSON; senão, redireciona para LOGIN_URL com ?next=.
    """
    def decorator(fn):
        @wraps(fn)
        def _wrapped(request, *args, **kwargs):
            user_login = request.COOKIES.get("auth_user")
            user = _load_user(user_login)
            if not user:
                if allow_json or request.headers.get("Accept","").startswith("application/json"):
                    return JsonResponse({"status":"unauthorized","message":"Faça login."}, status=401)
                login_url = getattr(settings, "LOGIN_URL", "/login")
                return redirect(f"{login_url}?next={request.get_full_path()}")
            request.app_user = user
            return fn(request, *args, **kwargs)
        return _wrapped
    return decorator(view) if view else decorator

arquivos_em_memoria = {} 

@login_required_view
def index(request):
    cias_opt = os.getenv("CIAS_OPT", "")
    cias_list = [cia.strip() for cia in cias_opt.split(",") if cia.strip()]
    return render(request, 'index.html', {'cias_opt': cias_list})

@login_required_view(allow_json=True)
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

@login_required_view(allow_json=True)
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

@login_required_view
def baixar_resumo(request):
    unique_id = request.GET.get('id')
    if not unique_id:
        raise Http404("ID não especificado.")

    file_path = os.path.join(settings.MEDIA_ROOT, f"{unique_id}.xlsx")

    if os.path.exists(file_path):
        return FileResponse(open(file_path, 'rb'), as_attachment=True, filename=f"{unique_id}.xlsx")
    else:
        raise Http404("Arquivo não encontrado.")

@login_required_view
def atualizar_relatorios(request):
    env = dotenv_values()
    cias_opt_raw = env.get("CIAS_OPT", "")
    cias_opt = [c.strip() for c in cias_opt_raw.split(",") if c.strip()]
    return render(request, "atualizar_relatorios.html", {"cias_opt": cias_opt})

@login_required_view
def atualizar_caixa(request):
    cias_raw = os.getenv("CIAS_OPT", "")
    cias_opt = [cia.strip() for cia in cias_raw.split(",") if cia.strip()]
    return render(request, 'atualizar_caixa.html', {'cias_opt': cias_opt})

@login_required_view(allow_json=True)
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
@login_required_view(allow_json=True)
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
    return JsonResponse({"status": "error", "mensagem": "Método não permitido"}, status=405)

@csrf_exempt
@login_required_view(allow_json=True)
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
@login_required_view(allow_json=True)
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
@login_required_view(allow_json=True)
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
    
# ============ (sem login required) ============
def login_page(request):
    if request.session.get("user_id"):
        return redirect("/")
    return render(request, "login.html")

def auth_logout(request):
    request.session.flush()
    resp = redirect("/login")
    resp.delete_cookie("auth_user", path="/", samesite="Lax")
    return resp

RESET_TTL_MIN = 30

def _client_meta(request):
    ip = (request.META.get('HTTP_X_FORWARDED_FOR') or '').split(',')[0].strip() or request.META.get('REMOTE_ADDR')
    ua = request.META.get('HTTP_USER_AGENT', '')
    return ip, ua

def _mask_email(email: str) -> str:
    if not email or '@' not in email:
        return email or ''
    name, dom = email.split('@', 1)
    masked = (name[0] + "***") if name else "***"
    return f"{masked}@{dom}"

def _get_user_by_token(token):
    conn = DatabaseManager.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, email, reset_expires_at
                  FROM public.app_users
                 WHERE reset_token = %s
                 LIMIT 1
            """, (str(token),))
            row = cur.fetchone()
            if not row:
                return None
            uid, username, email, exp = row
            if not exp or exp < timezone.now():
                return None
            return {"id": uid, "username": username, "email": email, "expires": exp}
    finally:
        DatabaseManager.return_connection(conn)

def reset_password_page(request, token):
    """Exibe o formulário de nova senha (se token válido)."""
    user = _get_user_by_token(token)
    if not user:
        return render(request, "reset_invalid.html", status=400)
    return render(request, "reset_password.html", {"token": token})

@require_POST
def auth_login(request):
    try:
        data = request.JSON if hasattr(request, "JSON") else None
    except Exception:
        data = None
    if not data:
        import json
        try:
            data = json.loads(request.body.decode('utf-8'))
        except Exception:
            data = {}

    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    audit_event_id = data.get("audit_event_id")

    if not username or not password:
        return JsonResponse({"status": "error", "mensagem": "Informe usuário e senha."}, status=400)

    ip, ua = _client_meta(request)

    conn = DatabaseManager.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, password_hash, is_active
                  FROM public.app_users
                 WHERE LOWER(username) = LOWER(%s)
                 LIMIT 1
            """, (username,))
            row = cur.fetchone()

            if not row:
                audit_event(
                    action="user_login",
                    user_name=username,
                    status="error",
                    audit_event_id=audit_event_id,
                    reason="user_not_found",
                    ip=ip, ua=ua
                )
                return JsonResponse({"status": "error", "mensagem": "Usuário ou senha inválidos."}, status=401)

            user_id, user_login, pwd_hash, is_active = row

            if not is_active:
                audit_event(
                    action="user_login",
                    user_name=user_login,
                    status="error",
                    audit_event_id=audit_event_id,
                    reason="inactive_user",
                    user_id=user_id, ip=ip, ua=ua
                )
                return JsonResponse({"status": "error", "mensagem": "Usuário inativo."}, status=403)

            if not check_password(password, pwd_hash):
                cur.execute("""
                    UPDATE public.app_users
                       SET failed_attempts = COALESCE(failed_attempts, 0) + 1,
                           updated_at = NOW()
                     WHERE id = %s
                """, (user_id,))
                conn.commit()

                audit_event(
                    action="user_login",
                    user_name=user_login,
                    status="error",
                    audit_event_id=audit_event_id,
                    reason="invalid_password",
                    user_id=user_id, ip=ip, ua=ua
                )
                return JsonResponse({"status": "error", "mensagem": "Usuário ou senha inválidos."}, status=401)

            cur.execute("""
                UPDATE public.app_users
                   SET failed_attempts = 0,
                       last_login_at = NOW(),
                       updated_at = NOW()
                 WHERE id = %s
            """, (user_id,))
            conn.commit()

        audit_event(
            action="user_login",
            user_name=user_login,
            status="success",
            audit_event_id=audit_event_id,
            user_id=user_id, ip=ip, ua=ua
        )

        resp = JsonResponse({"status": "ok", "redirect": "/"})
        resp.set_cookie("auth_user", user_login, httponly=True, samesite="Lax")
        return resp

    except Exception as e:
        audit_event(
            action="user_login",
            user_name=username,
            status="error",
            audit_event_id=audit_event_id,
            reason="internal_error",
            error=str(e)
        )
        return JsonResponse({"status": "error", "mensagem": "Erro interno."}, status=500)
    finally:
        DatabaseManager.return_connection(conn)

@require_POST
def auth_request_reset(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
    except Exception:
        data = {}

    email = (data.get("email") or "").strip()
    audit_event_id = data.get("audit_event_id")
    ip, ua = _client_meta(request)

    if not email:
        return JsonResponse({"status": "error", "mensagem": "Informe o e-mail."}, status=400)

    conn = DatabaseManager.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, email, is_active
                  FROM public.app_users
                 WHERE LOWER(email) = LOWER(%s) OR LOWER(username) = LOWER(%s)
                 LIMIT 1
            """, (email, email))
            row = cur.fetchone()

            if not row:
                audit_event(
                    action="password_reset_request",
                    user_name="unknown",
                    status="error",
                    audit_event_id=audit_event_id,
                    email_masked=_mask_email(email),
                    reason="user_not_found",
                    ip=ip, ua=ua
                )
                return JsonResponse({"status": "ok"})

            user_id, user_login, user_email, is_active = row
            if not is_active:
                audit_event(
                    action="password_reset_request",
                    user_name=user_login,
                    status="error",
                    audit_event_id=audit_event_id,
                    user_id=user_id,
                    email_masked=_mask_email(user_email),
                    reason="inactive_user",
                    ip=ip, ua=ua
                )
                return JsonResponse({"status": "ok"})

            token = uuid.uuid4()
            expires = timezone.now() + timedelta(hours=2)

            cur.execute("""
                UPDATE public.app_users
                   SET reset_token = %s,
                       reset_expires_at = %s,
                       updated_at = NOW()
                 WHERE id = %s
            """, (str(token), expires, user_id))
            conn.commit()

        reset_link = request.build_absolute_uri(f"/auth/reset/{token}")

        subject = "Redefinição de senha - Extrato App"
        body = (
            f"Olá {user_login},\n\n"
            f"Recebemos uma solicitação de redefinição de senha.\n"
            f"Use o link abaixo (válido por 2 horas):\n{reset_link}\n\n"
            f"Se não foi você, ignore este e-mail."
        )

        send_mail(
            subject,
            body,
            settings.DEFAULT_FROM_EMAIL,
            [user_email],
            fail_silently=False,
        )

        audit_event(
            action="password_reset_request",
            user_name=user_login,
            status="success",
            audit_event_id=audit_event_id,
            user_id=user_id,
            email_masked=_mask_email(user_email),
            expires_at=expires.isoformat(),
            ip=ip, ua=ua
        )
        return JsonResponse({"status": "ok"})
    except Exception as e:
        audit_event(
            action="password_reset_request",
            user_name=email,
            status="error",
            audit_event_id=audit_event_id,
            reason="internal_error",
            error=str(e),
            ip=ip, ua=ua
        )
        return JsonResponse({"status": "error", "mensagem": "Erro ao processar a solicitação."}, status=500)
    finally:
        DatabaseManager.return_connection(conn)

def auth_reset_confirm(request, token: uuid.UUID):
    """
    GET: mostra formulário
    POST: salva nova senha
    """
    if request.method == "GET":
        return render(request, "auth_reset_confirm.html", {"token": str(token)})

    if request.method != "POST":
        return HttpResponseNotAllowed(["GET", "POST"])

    new_password = (request.POST.get("new_password") or "").strip()
    confirm_password = (request.POST.get("confirm_password") or "").strip()
    ip, ua = _client_meta(request)

    if not new_password or new_password != confirm_password:
        return render(
            request,
            "auth_reset_confirm.html",
            {"token": str(token), "error": "Senha e confirmação não conferem."},
            status=400
        )

    conn = DatabaseManager.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, reset_expires_at
                  FROM public.app_users
                 WHERE reset_token = %s
                 LIMIT 1
            """, (str(token),))
            row = cur.fetchone()

            if not row:
                audit_event(
                    action="password_reset_apply",
                    user_name="unknown",
                    status="error",
                    reason="invalid_token",
                    token=str(token),
                    ip=ip, ua=ua
                )
                return render(request, "auth_reset_confirm.html", {"error": "Token inválido ou expirado."}, status=400)

            user_id, user_login, expires_at = row
            if not expires_at or timezone.now() > expires_at:
                audit_event(
                    action="password_reset_apply",
                    user_name=user_login,
                    status="error",
                    user_id=user_id,
                    reason="expired_token",
                    token=str(token),
                    ip=ip, ua=ua
                )
                return render(request, "auth_reset_confirm.html", {"error": "Token expirado."}, status=400)

            pwd_hash = make_password(new_password)
            cur.execute("""
                UPDATE public.app_users
                   SET password_hash = %s,
                       reset_token = NULL,
                       reset_expires_at = NULL,
                       updated_at = NOW()
                 WHERE id = %s
            """, (pwd_hash, user_id))
            conn.commit()

        audit_event(
            action="password_reset_apply",
            user_name=user_login,
            status="success",
            user_id=user_id,
            ip=ip, ua=ua
        )

        return redirect("/login")
    except Exception as e:
        audit_event(
            action="password_reset_apply",
            user_name="unknown",
            status="error",
            reason="internal_error",
            error=str(e),
            token=str(token),
            ip=ip, ua=ua
        )
        return render(request, "auth_reset_confirm.html", {"error": "Erro interno."}, status=500)
    finally:
        DatabaseManager.return_connection(conn)

