let AUDIT_EVENT_ID = null;
function ensureAuditEventId(){
  if(!AUDIT_EVENT_ID){
    AUDIT_EVENT_ID = (crypto?.randomUUID?.() || Math.random().toString(36).slice(2));
  }
  return AUDIT_EVENT_ID;
}

document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("formLogin");
  const btnSubmit = document.getElementById("btnSubmit");
  const btnForgot = document.getElementById("btnForgot");
  const username = document.getElementById("username");
  const password = document.getElementById("password");
  const togglePass = document.getElementById("togglePass");

  const errBox = document.getElementById("loginError");
  const showInlineError = (msg) => {
    if (!errBox) return;
    errBox.textContent = msg || "Usuário ou senha inválidos.";
    errBox.classList.add("show");
  };
  const clearInlineError = () => {
    if (!errBox) return;
    errBox.textContent = "";
    errBox.classList.remove("show");
  };

  const downloadPopup = document.getElementById("download-popup");
  const popupTitle = document.getElementById("popup-title");
  const notification = document.getElementById("notification");
  const notifIcon = document.getElementById("notification-icon");
  const notifMsg = document.getElementById("notification-message");

  const toastEl = document.getElementById("resultToast");
  const toastTitle = document.getElementById("toastTitle");
  const toastMsg = document.getElementById("toastMsg");
  const toastIconSuccess = document.getElementById("toastIconSuccess");
  const toastIconError = document.getElementById("toastIconError");
  const toastClose = document.getElementById("toastClose");

  const forgotBackdrop = document.getElementById("forgotBackdrop");
  const forgotClose = document.getElementById("forgotClose");
  const forgotCancel = document.getElementById("forgotCancel");
  const forgotSend = document.getElementById("forgotSend");
  const forgotEmail = document.getElementById("forgotEmail");

  const csrf = window.csrfToken || (document.cookie.split("; ").find(x=>x.startsWith("csrftoken="))?.split("=")[1] || "");

  function showPopup(msg){
    if(popupTitle) popupTitle.textContent = msg || "Processando...";
    downloadPopup?.classList.remove("popup-hidden");
  }
  function hidePopup(){
    downloadPopup?.classList.add("popup-hidden");
  }
  function showNotification(message, type="info"){
    if(!notification||!notifIcon||!notifMsg) return;
    notification.className="notification"; notifIcon.className=""; notifIcon.innerHTML="";
    if(type==="success"){notification.classList.add("success"); notifIcon.classList.add("bi","bi-check-circle-fill");}
    else if(type==="error"){notification.classList.add("error"); notifIcon.classList.add("bi","bi-x-circle-fill");}
    else if(type==="loading"){notification.classList.add("info"); notifIcon.innerHTML='<div class="spinner" style="display:inline-block;width:1em;height:1em;"></div>';}
    else {notification.classList.add("info"); notifIcon.classList.add("bi","bi-info-circle-fill");}
    notifMsg.textContent = message;
    notification.classList.remove("popup-hidden");
    notification.classList.add("show");
    if(type!=="loading") setTimeout(()=>notification.classList.remove("show"), 5000);
  }
  function showToast(type,message){
    if(!toastEl) return;
    toastIconSuccess.style.display = (type==="success") ? "block" : "none";
    toastIconError.style.display   = (type==="error") ? "block" : "none";
    toastTitle.textContent = (type==="success") ? "Sucesso!" : "Ops...";
    toastMsg.textContent = message || "";
    toastEl.classList.add("open"); toastEl.setAttribute("aria-hidden","false");
  }
  function hideToast(){
    toastEl?.classList.remove("open");
    toastEl?.setAttribute("aria-hidden","true");
  }
  toastClose?.addEventListener("click", hideToast);
  toastEl?.addEventListener("click",(e)=>{ if(e.target===toastEl) hideToast(); });

  // ---------- Mostrar/ocultar senha ----------
  togglePass?.addEventListener("click", ()=>{
    if(password.type==="password"){
      password.type="text";
      togglePass.innerHTML='<i class="bi bi-eye"></i>';
    } else {
      password.type="password";
      togglePass.innerHTML='<i class="bi bi-eye-slash"></i>';
    }
  });



  // ---------- Esqueci minha senha ----------
  function openForgot(){ forgotBackdrop.classList.add("open"); forgotBackdrop.setAttribute("aria-hidden","false"); forgotEmail?.focus(); }
  function closeForgot(){ forgotBackdrop.classList.remove("open"); forgotBackdrop.setAttribute("aria-hidden","true"); }
  btnForgot?.addEventListener("click", openForgot);
  forgotClose?.addEventListener("click", closeForgot);
  forgotCancel?.addEventListener("click", closeForgot);
  forgotBackdrop?.addEventListener("click",(e)=>{ if(e.target===forgotBackdrop) closeForgot(); });

  forgotSend?.addEventListener("click", async ()=>{
    const email = (forgotEmail?.value || "").trim();
    if(!email){ forgotEmail?.reportValidity(); return; }
    try{
      showPopup("Enviando instruções...");
      const resp = await fetch("/auth/request-reset", {
        method:"POST",
        headers:{ "Content-Type":"application/json", "X-CSRFToken": csrf },
        body: JSON.stringify({ email, audit_event_id: ensureAuditEventId() })
      });
      hidePopup();
      const data = await resp.json().catch(()=>({}));
      if(resp.ok && data?.status==="ok"){
        closeForgot();
        showToast("success","Enviamos um link para o seu e-mail.");
      }else{
        showToast("error", data?.mensagem || "Não foi possível enviar o e-mail de reset.");
      }
    }catch(err){
      hidePopup();
      showToast("error","Erro ao contatar o servidor.");
    }
  });

  if (form) {
    form.action = 'javascript:void(0)';
    form.setAttribute('novalidate','true');
  }

  const canSubmit = () =>
    (username?.value || "").trim().length > 0 &&
    (password?.value || "").trim().length > 0;

  function updateSubmitState(){
    const enabled = canSubmit();
    if (btnSubmit){
      btnSubmit.disabled = !enabled;
      btnSubmit.setAttribute("aria-disabled", String(!enabled));
    }
  }

  username?.addEventListener("input", updateSubmitState);
  password?.addEventListener("input", updateSubmitState);
  username?.addEventListener("paste", () => setTimeout(updateSubmitState, 0));
  password?.addEventListener("paste", () => setTimeout(updateSubmitState, 0));

  updateSubmitState();

  async function doLogin(){
    if (btnSubmit?.disabled || !canSubmit()){
      showNotification("Informe usuário e senha.", "error");
      return;
    }

    clearInlineError();

    try{
      showPopup("Autenticando... aguarde.");
      showNotification("Validando credenciais...", "loading");

      const auditEventId = ensureAuditEventId();
      const resp = await fetch("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRFToken": csrf },
        body: JSON.stringify({
          username: username.value.trim(),
          password: password.value,
          audit_event_id: auditEventId
        })
      });

      let data = {};
      try { data = await resp.json(); } catch { data = {}; }

      hidePopup();
      notification?.classList.remove("show");

      if(resp.ok && data?.status==="ok"){
        showToast("success","Login realizado com sucesso.");
        AUDIT_EVENT_ID = null;
        setTimeout(()=>{ window.location.href = data?.redirect || "/"; }, 900);
        return;
      }

      const msg = data?.mensagem ||
                  (resp.status === 401 ? "Usuário ou senha inválidos." :
                   `Falha no login (HTTP ${resp.status}).`);
      showInlineError(msg);
      showToast("error", msg);

    }catch(err){
      hidePopup();
      notification?.classList.remove("show");
      showInlineError("Erro ao contatar o servidor.");
      showToast("error","Erro ao contatar o servidor.");
      console.error("Login ► erro:", err);
    }
  }

  btnSubmit?.addEventListener("click", (e)=>{ e.preventDefault(); e.stopPropagation(); doLogin(); return false; });

  [username, password].forEach(el => {
    el?.addEventListener("keydown", (e)=>{
      if (e.key === "Enter") {
        e.preventDefault();
        if (canSubmit()) doLogin();
        else showNotification("Informe usuário e senha.", "error");
      }
    });
  });
});
