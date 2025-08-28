from functools import wraps
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import redirect
from extrato_app.CoreData.grande_conn import DatabaseManager

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
    Exige login por cookie 'auth_user' OU por sessão.
    Retorna 401 JSON quando allow_json=True; caso contrário, redireciona para LOGIN_URL com ?next=.
    Também injeta cabeçalhos no-cache no response final para evitar back-cache após logout.
    """
    def decorator(fn):
        @wraps(fn)
        def _wrapped(request, *args, **kwargs):
            user_login = (
                request.COOKIES.get("auth_user")
                or request.session.get("username")
            )
            user = _load_user(user_login)
            if not user:
                if allow_json or request.headers.get("Accept","").startswith("application/json"):
                    return JsonResponse({"status":"unauthorized","message":"Faça login."}, status=401)
                login_url = getattr(settings, "LOGIN_URL", "/login")
                return redirect(f"{login_url}?next={request.get_full_path()}")

            request.app_user = user
            resp = fn(request, *args, **kwargs)

            try:
                resp["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                resp["Pragma"] = "no-cache"
                resp["Expires"] = "0"
            except Exception:
                pass
            return resp
        return _wrapped
    return decorator(view) if view else decorator
