let AUDIT_EVENT_ID = null;
function ensureAuditEventId() {
  if (!AUDIT_EVENT_ID) {
    AUDIT_EVENT_ID = (crypto?.randomUUID?.() || Math.random().toString(36).slice(2));
  }
  return AUDIT_EVENT_ID;
}

document.addEventListener("DOMContentLoaded", () => {
  const ciaItems = document.querySelectorAll(".cia-item");
  const selectedCount = document.getElementById("selected-count");
  const ciasSelectedInput = document.getElementById("cias-selected");
  const selectAllBtn = document.getElementById("select-all");
  const deselectAllBtn = document.getElementById("deselect-all");
  const form = document.querySelector("form");
  const submitBtn = form ? form.querySelector('button[type="submit"]') : null;

  const downloadPopup = document.getElementById("download-popup");
  const popupTitle = document.getElementById("popup-title");
  const notification = document.getElementById("notification");
  const notifIcon = document.getElementById("notification-icon");
  const notifMsg = document.getElementById("notification-message");

  const toastEl = document.getElementById('resultToast');
  const toastTitle = document.getElementById('toastTitle');
  const toastMsg = document.getElementById('toastMsg');
  const toastIconSuccess = document.getElementById('toastIconSuccess');
  const toastIconError = document.getElementById('toastIconError');
  const toastClose = document.getElementById('toastClose');

  const originalBtnHTML = submitBtn ? submitBtn.innerHTML : null;
  const csrfToken =
    window.csrfToken ||
    (document.cookie.split("; ").find((x) => x.startsWith("csrftoken="))?.split("=")[1] || "");

  let selectedCias = [];

  function showDownloadPopup(message = "Processando atualização dos relatórios... aguarde.") {
    if (popupTitle) popupTitle.textContent = message;
    if (downloadPopup) downloadPopup.classList.remove("popup-hidden");
  }
  function hideDownloadPopup() {
    if (downloadPopup) downloadPopup.classList.add("popup-hidden");
  }
  function showNotification(message, type = "info") {
    if (!notification || !notifIcon || !notifMsg) return;
    notification.className = "notification";
    notifIcon.className = "";
    notifIcon.innerHTML = "";

    if (type === "success") { notification.classList.add("success"); notifIcon.classList.add("bi","bi-check-circle-fill"); }
    else if (type === "error") { notification.classList.add("error"); notifIcon.classList.add("bi","bi-x-circle-fill"); }
    else if (type === "loading") { notification.classList.add("info"); notifIcon.innerHTML = '<div class="spinner" style="display:inline-block;width:1em;height:1em;"></div>'; }
    else { notification.classList.add("info"); notifIcon.classList.add("bi","bi-info-circle-fill"); }

    notifMsg.textContent = message;
    notification.classList.remove("popup-hidden");
    notification.classList.add("show");
    if (type !== "loading") setTimeout(() => notification.classList.remove("show"), 5000);
  }
  function setLoadingState(isLoading) {
    if (!submitBtn) return;
    if (isLoading) {
      submitBtn.disabled = true;
      submitBtn.innerHTML = '<i class="bi bi-hourglass-split"></i> Processando...';
    } else {
      submitBtn.disabled = false;
      submitBtn.innerHTML = originalBtnHTML;
    }
  }

  function showToast(type, message) {
    if (!toastEl) return;
    toastIconSuccess.style.display = type === 'success' ? 'block' : 'none';
    toastIconError.style.display   = type === 'error'   ? 'block' : 'none';
    toastTitle.textContent = type === 'success' ? 'Sucesso!' : 'Ops...';
    toastMsg.textContent = message || '';
    toastEl.classList.add('open');
    toastEl.setAttribute('aria-hidden', 'false');
  }
  function hideToast(){
    if (!toastEl) return;
    toastEl.classList.remove('open');
    toastEl.setAttribute('aria-hidden', 'true');
  }
  toastClose?.addEventListener('click', hideToast);
  toastEl?.addEventListener('click', (e) => { if (e.target === toastEl) hideToast(); });


  function updateSelections() {
    if (selectedCount) selectedCount.textContent = selectedCias.length;
    if (ciasSelectedInput) ciasSelectedInput.value = JSON.stringify(selectedCias);
    ciaItems.forEach((item) => {
      const ciaName = item.dataset.cia;
      item.classList.toggle("selected", selectedCias.includes(ciaName));
    });
  }
  ciaItems.forEach((item) => {
    item.addEventListener("click", () => {
      const ciaName = item.dataset.cia;
      const index = selectedCias.indexOf(ciaName);
      index === -1 ? selectedCias.push(ciaName) : selectedCias.splice(index, 1);
      updateSelections();
    });
  });
  if (selectAllBtn) selectAllBtn.addEventListener("click", () => {
    selectedCias = Array.from(ciaItems).map((item) => item.dataset.cia);
    updateSelections();
  });
  if (deselectAllBtn) deselectAllBtn.addEventListener("click", () => {
    selectedCias = [];
    updateSelections();
  });


  document.querySelectorAll('input[name="mes"]').forEach((competenciaInput) => {
    competenciaInput.addEventListener("input", function (e) {
      let value = e.target.value.replace(/\D/g, "");
      if (value.length > 2) value = value.substring(0, 2) + "-" + value.substring(2, 6);
      e.target.value = value;
    });
  });


  if (form) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();

      const competencia = document.querySelector('input[name="mes"]').value;

      if (!/^(0[1-9]|1[0-2])-[0-9]{4}$/.test(competencia)) {
        showNotification("Formato de competência inválido! Use MM-AAAA.", "error");
        return;
      }
      if (selectedCias.length === 0) {
        showNotification("Selecione pelo menos uma CIA.", "error");
        return;
      }

      try {
        setLoadingState(true);
        showDownloadPopup();
        // showNotification("Atualizando relatórios, aguarde…", "loading");

        const auditEventId = ensureAuditEventId();
        console.debug("[AUDIT] atualizar_relatorios • event_id=", auditEventId, "cias=", selectedCias, "competencia=", competencia);

        const res = await fetch("/api/atualizar-relatorios", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-CSRFToken": csrfToken },
          body: JSON.stringify({ cias: selectedCias, competencia, audit_event_id: auditEventId })
        });

        const json = await res.json();

        hideDownloadPopup();
        setLoadingState(false);

        if (res.ok && (json.status === "success" || json.status === "partial")) {
          const total = json.resultados ? Object.keys(json.resultados).length : selectedCias.length;
          const okCount = json.resultados ? Object.values(json.resultados).filter(r => r && r.success).length : total;

          notification?.classList.remove('show');

          const suffix = json.resultados
            ? Object.entries(json.resultados).map(([k,v]) => v?.version_id ? `${k} (v${v.version_id})` : k).join(', ')
            : null;

          showToast('success', `Atualização concluída (${okCount}/${total})${suffix ? ` • ${suffix}` : ''}.`);
          console.table(json.resultados || {});

          AUDIT_EVENT_ID = null;
        } else {
          const msg = (json && json.mensagem) ? json.mensagem : "Erro ao atualizar relatórios.";
          showToast('error', msg);
          console.error("❌ Atualizar Relatórios - erro:", json);
        }
      } catch (err) {
        hideDownloadPopup();
        setLoadingState(false);
        console.error("Erro ao atualizar relatórios:", err);
        showToast('error', 'Erro ao conectar com o servidor.');
      }
    });
  }

});

function handleLogoutClick(btn, evt){
  evt?.preventDefault?.();
  if (!btn || btn.dataset.busy === '1') return;
  btn.dataset.busy = '1';
  btn.disabled = true;
  btn.innerHTML = '<i class="bi bi-box-arrow-right"></i> Saindo...';
  window.location.href = '/logout';
}

document.addEventListener('DOMContentLoaded', () => {
  const btnLogout = document.getElementById('btnLogout');
  if (btnLogout) {
    btnLogout.addEventListener('click', (e) => handleLogoutClick(btnLogout, e));
  }
});

document.addEventListener('click', (e) => {
  const btn = e.target?.closest?.('#btnLogout');
  if (btn) handleLogoutClick(btn, e);
});
