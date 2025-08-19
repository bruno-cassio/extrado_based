document.addEventListener('DOMContentLoaded', function () {
  const list = document.getElementById('cias-list');
  const selectedCount = document.getElementById('selected-count');
  const hiddenSelected = document.getElementById('cias-selected');
  const btnSelectAll = document.getElementById('select-all');
  const btnDeselectAll = document.getElementById('deselect-all');
  const btnProsseguir = document.getElementById('btnProsseguir');

  const modalBackdrop = document.getElementById('modalBackdrop');
  const modalClose = document.getElementById('modalClose');
  const modalCancelar = document.getElementById('modalCancelar');
  const modalAtualizar = document.getElementById('modalAtualizar');
  const modalCamposCias = document.getElementById('modalCamposCias');
  const inputCompetencia = document.getElementById('inputCompetencia');

  // Popup resultado
  const toastEl = document.getElementById('resultToast');
  const toastTitle = document.getElementById('toastTitle');
  const toastMsg = document.getElementById('toastMsg');
  const toastIconSuccess = document.getElementById('toastIconSuccess');
  const toastIconError = document.getElementById('toastIconError');
  const toastClose = document.getElementById('toastClose');

  // Confirm existe
  const confirmEl = document.getElementById('confirmBackdrop');
  const confirmList = document.getElementById('confirmList');
  const confirmYes = document.getElementById('confirmYes');
  const confirmNo = document.getElementById('confirmNo');

  const form = document.getElementById('formAtualizarCaixa');
  const ENDPOINT_BUSCAR_CIAS = form?.dataset?.buscarCiasUrl || '/api/buscar-cias/';

  // === Máscara MM-AAAA mínima: só insere "-" após 2 dígitos ===
  (function attachMaskMMYYYY() {
    const el = inputCompetencia;
    if (!el) return;

    function formatValueKeepCaret(e) {
      const input = e.target;
      const digitsBefore = input.value.slice(0, input.selectionStart).replace(/\D/g, '').length;
      let digits = input.value.replace(/\D/g, '').slice(0, 6); // MM + AAAA

      const newVal = digits.length <= 2 ? digits : digits.slice(0, 2) + '-' + digits.slice(2);
      input.value = newVal;

      const newPos = digitsBefore <= 2 ? digitsBefore : digitsBefore + 1; // +1 pelo '-'
      input.setSelectionRange(newPos, newPos);
    }

    el.addEventListener('input', formatValueKeepCaret);
    el.addEventListener('paste', (e) => {
      e.preventDefault();
      const txt = (e.clipboardData || window.clipboardData).getData('text');
      const digits = txt.replace(/\D/g, '').slice(0, 6);
      el.value = digits.length <= 2 ? digits : digits.slice(0, 2) + '-' + digits.slice(2);
      el.setSelectionRange(el.value.length, el.value.length);
    });
  })();

  // Seleção de cias
  function toggleItem(el) { el.classList.toggle('selected'); updateSelected(); }
  function updateSelected() {
    const active = [...list.querySelectorAll('.cia-item.selected')].map(n => n.dataset.cia);
    selectedCount.textContent = active.length;
    hiddenSelected.value = JSON.stringify(active);
  }
  btnSelectAll?.addEventListener('click', () => {
    list.querySelectorAll('.cia-item').forEach(el => el.classList.add('selected'));
    updateSelected();
  });
  btnDeselectAll?.addEventListener('click', () => {
    list.querySelectorAll('.cia-item').forEach(el => el.classList.remove('selected'));
    updateSelected();
  });
  list?.addEventListener('click', (e) => {
    const item = e.target.closest('.cia-item');
    if (item) toggleItem(item);
  });

  // Abre modal e cria inputs de valores
  btnProsseguir?.addEventListener('click', () => {
    const selected = JSON.parse(hiddenSelected.value || '[]');
    if (selected.length === 0) { alert('Selecione ao menos uma seguradora (CIA).'); return; }

    modalCamposCias.innerHTML = '';
    selected.forEach(cia => {
      const row = document.createElement('div');
      row.className = 'field-row';
      row.innerHTML = `
        <div><strong>${cia}</strong></div>
        <div>
          <label style="display:block;font-size:.85rem;margin-bottom:4px;">Valor Bruto</label>
          <input type="text" class="input-text moeda" name="valor_bruto_${cia}" form="formAtualizarCaixa" placeholder="0,00" required />
        </div>
        <div>
          <label style="display:block;font-size:.85rem;margin-bottom:4px;">Valor Líquido</label>
          <input type="text" class="input-text moeda" name="valor_liquido_${cia}" form="formAtualizarCaixa" placeholder="0,00" required />
        </div>
      `;
      modalCamposCias.appendChild(row);
    });

    // Máscara moeda (pt-BR) + colagem em massa
    modalCamposCias.querySelectorAll('input.moeda').forEach(input => {
      input.addEventListener('input', (e) => {
        let txt = e.target.value.replace(/[^\d,.-]/g, '').replace(/\./g, '').replace(',', '.');
        const num = parseFloat(txt);
        if (isNaN(num)) { e.target.value = ''; return; }
        e.target.value = num.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      });
      input.addEventListener('paste', (e) => {
        const focused = e.target;
        const inputs = [...modalCamposCias.querySelectorAll('input.moeda')];
        const startIndex = inputs.indexOf(focused);
        const text = e.clipboardData.getData('text/plain');
        const rows = text.split(/\r?\n/).filter(r => r.trim() !== '');
        if (rows.length <= 1 && !rows[0].includes('\t')) return;
        e.preventDefault();
        const flat = [];
        for (const r of rows) { flat.push(...r.split('\t')); }
        for (let i = 0; i < flat.length && (startIndex + i) < inputs.length; i++) {
          const raw = flat[i].replace(/R\$/g, '').replace(/\./g, '').replace(',', '.').trim();
          const v = parseFloat(raw);
          if (!isNaN(v)) {
            inputs[startIndex + i].value = v.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
          }
        }
      });
    });

    openModal();
  });

  function openModal(){ modalBackdrop.classList.add('open'); modalBackdrop.setAttribute('aria-hidden','false'); inputCompetencia.focus(); }
  function closeModal(){ modalBackdrop.classList.remove('open'); modalBackdrop.setAttribute('aria-hidden','true'); }
  modalClose?.addEventListener('click', closeModal);
  modalCancelar?.addEventListener('click', closeModal);
  modalBackdrop?.addEventListener('click', (e) => { if (e.target === modalBackdrop) closeModal(); });

  // Toast helpers
  function showToast(type, message) {
    toastIconSuccess.style.display = type === 'success' ? 'block' : 'none';
    toastIconError.style.display   = type === 'error'   ? 'block' : 'none';
    toastTitle.textContent = type === 'success' ? 'Sucesso!' : 'Ops...';
    toastMsg.textContent = message || '';
    toastEl.classList.add('open'); toastEl.setAttribute('aria-hidden','false');
  }
  function hideToast(){ toastEl.classList.remove('open'); toastEl.setAttribute('aria-hidden','true'); }
  toastClose?.addEventListener('click', hideToast);
  toastEl?.addEventListener('click', (e)=>{ if(e.target===toastEl) hideToast(); });

  // Confirm helpers (mostra inseridas e existentes)
  function openConfirm(ciasExistentes = [], ciasInseridas = []) {
    let html = '';
    if (ciasInseridas.length) {
      html += `<div style="margin-bottom:8px;"><strong>Inseridas agora:</strong></div>`;
      html += `<div style="margin-left:8px; margin-bottom:10px;">${ciasInseridas.map(c=>`• ${c}`).join('<br>')}</div>`;
    }
    if (ciasExistentes.length) {
      html += `<div style="margin-bottom:8px;"><strong>Já existiam (aguardam sua confirmação para atualizar):</strong></div>`;
      html += `<div style="margin-left:8px;">${ciasExistentes.map(c=>`• ${c}`).join('<br>')}</div>`;
    }
    confirmList.innerHTML = html || '—';
    confirmEl.classList.add('open'); confirmEl.setAttribute('aria-hidden','false');
  }
  function closeConfirm(){
    confirmEl.classList.remove('open'); confirmEl.setAttribute('aria-hidden','true');
  }
  confirmNo?.addEventListener('click', closeConfirm);
  confirmEl?.addEventListener('click', (e)=>{ if(e.target===confirmEl) closeConfirm(); });

  // Chamar buscar_cias_api (que chama inserir_ou_atualizar_caixa no DBA)
  async function postCaixa(forcarUpdate=false){
    const selected = JSON.parse(hiddenSelected.value || '[]');
    const fd = new FormData(form);
    fd.set('cias', JSON.stringify(selected));     // backend espera 'cias'
    if (forcarUpdate) fd.set('forcar_update', 'true');

    const csrftoken = document.querySelector('input[name=csrfmiddlewaretoken]')?.value || '';

    const originalText = modalAtualizar.innerHTML;
    modalAtualizar.disabled = true;
    modalAtualizar.innerHTML = '<i class="bi bi-arrow-repeat"></i> Processando...';

    try{
      const resp = await fetch(ENDPOINT_BUSCAR_CIAS, {
        method: 'POST',
        headers: {
          'X-Requested-With': 'XMLHttpRequest',
          'X-CSRFToken': csrftoken
        },
        body: fd
      });
      const data = await resp.json().catch(()=>null);

      // Fecha o modal de edição (independente do resultado)
      closeModal();

      if (resp.ok && data){
        if (data.status === 'ok'){
          showToast('success', data.message || 'Processo concluído.');
        } else if (data.status === 'existe'){
          // Inseridas as que não existiam; as existentes aguardam confirmação
          openConfirm(data.cias_existentes || [], data.cias_inseridas || []);
        } else {
          showToast('error', data.message || 'Falha ao processar.');
        }
      } else {
        showToast('error', 'Erro no servidor.');
      }
    }catch(err){
      showToast('error', err?.message || 'Erro inesperado.');
    }finally{
      modalAtualizar.disabled = false;
      modalAtualizar.innerHTML = originalText;
    }
  }

  // Botão "Atualizar Caixa" -> envia: insere novas; se houver existentes, abre confirmação
  modalAtualizar?.addEventListener('click', async () => {
    if (!form.reportValidity()) return;
    await postCaixa(false);
  });

  // Confirmar atualização dos que já existiam
  confirmYes?.addEventListener('click', async () => {
    closeConfirm();
    await postCaixa(true);
  });

})();
