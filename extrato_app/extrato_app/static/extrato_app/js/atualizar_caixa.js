document.addEventListener('DOMContentLoaded', function () {
  const list = document.getElementById('cias-list');
  const selectedCount = document.getElementById('selected-count');
  const hiddenSelected = document.getElementById('cias-selected');
  const btnSelectAll = document.getElementById('select-all');
  const btnDeselectAll = document.getElementById('deselect-all');
  const btnProsseguir = document.getElementById('btnProsseguir');

  const form = document.getElementById('formAtualizarCaixa');
  const ENDPOINT_BUSCAR_CIAS = form?.dataset?.buscarCiasUrl || '/api/buscar-cias/';

  // ===== Modal Atualizar
  const modalBackdrop = document.getElementById('modalBackdrop');
  const modalClose = document.getElementById('modalClose');
  const modalCancelar = document.getElementById('modalCancelar');
  const modalAtualizar = document.getElementById('modalAtualizar');
  const modalCamposCias = document.getElementById('modalCamposCias');
  const inputCompetencia = document.getElementById('inputCompetencia');

  // ===== NOVO Modal Consultar
  const btnOpenConsulta = document.getElementById('btnOpenConsulta');
  const consultaBackdrop = document.getElementById('consultaBackdrop');
  const consultaClose = document.getElementById('consultaClose');
  const consultaCancelar = document.getElementById('consultaCancelar');
  const consultaExecutar = document.getElementById('consultaExecutar');
  const inputCompetenciaConsulta = document.getElementById('inputCompetenciaConsulta');
  const consultaResultado = document.getElementById('consultaResultado');
  const CONSULTAR_CAIXA_URL = form?.dataset?.consultarCaixaUrl || '/consultar-caixa';

  // ===== Toast
  const toastEl = document.getElementById('resultToast');
  const toastTitle = document.getElementById('toastTitle');
  const toastMsg = document.getElementById('toastMsg');
  const toastIconSuccess = document.getElementById('toastIconSuccess');
  const toastIconError = document.getElementById('toastIconError');
  const toastClose = document.getElementById('toastClose');

  // ===== Confirm
  const confirmEl = document.getElementById('confirmBackdrop');
  const confirmList = document.getElementById('confirmList');
  const confirmYes = document.getElementById('confirmYes');
  const confirmNo = document.getElementById('confirmNo');

  // ---------- Helpers ----------
  function attachMaskMMYYYYTo(el){
    if(!el) return;
    function formatValueKeepCaret(e) {
      const input = e.target;
      const digitsBefore = input.value.slice(0, input.selectionStart).replace(/\D/g, '').length;
      let digits = input.value.replace(/\D/g, '').slice(0, 6);
      const newVal = digits.length <= 2 ? digits : digits.slice(0, 2) + '-' + digits.slice(2);
      input.value = newVal;
      const newPos = digitsBefore <= 2 ? digitsBefore : digitsBefore + 1;
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
  }
  attachMaskMMYYYYTo(inputCompetencia);
  attachMaskMMYYYYTo(inputCompetenciaConsulta);

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

  function openModal(){ modalBackdrop.classList.add('open'); modalBackdrop.setAttribute('aria-hidden','false'); inputCompetencia?.focus(); }
  function closeModal(){ modalBackdrop.classList.remove('open'); modalBackdrop.setAttribute('aria-hidden','true'); }
  modalClose?.addEventListener('click', closeModal);
  modalCancelar?.addEventListener('click', closeModal);
  modalBackdrop?.addEventListener('click', (e) => { if (e.target === modalBackdrop) closeModal(); });

  function openConsulta(){
    consultaBackdrop.classList.add('open');
    consultaBackdrop.setAttribute('aria-hidden','false');
    inputCompetenciaConsulta?.focus();
    if (consultaResultado) {
      consultaResultado.innerHTML = 'Informe a competência e clique em Consultar.';
      consultaResultado.classList.add('consulta-empty');
    }
  }
  function closeConsulta(){ consultaBackdrop.classList.remove('open'); consultaBackdrop.setAttribute('aria-hidden','true'); }
  btnOpenConsulta?.addEventListener('click', openConsulta);
  consultaClose?.addEventListener('click', closeConsulta);
  consultaCancelar?.addEventListener('click', closeConsulta);
  consultaBackdrop?.addEventListener('click', (e)=>{ if(e.target===consultaBackdrop) closeConsulta(); });

  // Renderiza a tabela de consulta dentro do modal de consulta
  function renderConsultaTabela(rows, competencia) {
    if (!consultaResultado) return;

    const fmtBRL = v => (v == null || v === '' || isNaN(Number(v)))
      ? '—'
      : Number(v).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

    if (!Array.isArray(rows) || rows.length === 0) {
      consultaResultado.innerHTML = `Nenhum registro para <strong>${competencia}</strong>.`;
      consultaResultado.classList.add('consulta-empty');
      return;
    }

    const body = rows.map(r => `
      <tr>
        <td>${r.cia ?? ''}</td>
        <td>${fmtBRL(r.valor_bruto_declarado)}</td>
        <td>${fmtBRL(r.valor_liq_declarado)}</td>
        <td>${r.competencia ?? ''}</td>
      </tr>
    `).join('');

    consultaResultado.classList.remove('consulta-empty');
    consultaResultado.innerHTML = `
      <div class="table-wrap">
        <table class="consulta-table">
          <thead>
            <tr>
              <th>CIA</th>
              <th>Valor Bruto</th>
              <th>Valor Líquido</th>
              <th>Competência</th>
            </tr>
          </thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    `;
  }

  // Consultar (não fecha o modal; mostra tabela + console)
  consultaExecutar?.addEventListener('click', async () => {
    const competencia = (inputCompetenciaConsulta?.value || '').trim();
    if (!competencia || !inputCompetenciaConsulta.checkValidity()) {
      inputCompetenciaConsulta?.reportValidity();
      return;
    }

    const fd = new FormData();
    fd.set('mes', competencia);
    const csrftoken = document.querySelector('input[name=csrfmiddlewaretoken]')?.value || '';

    const original = consultaExecutar.innerHTML;
    consultaExecutar.disabled = true;
    consultaExecutar.innerHTML = '<i class="bi bi-arrow-repeat"></i> Consultando...';

    try {
      const resp = await fetch(CONSULTAR_CAIXA_URL, {
        method: 'POST',
        headers: { 'X-Requested-With': 'XMLHttpRequest', 'X-CSRFToken': csrftoken },
        body: fd
      });
      const data = await resp.json();

      console.group(`Consulta Caixa • Competência: ${competencia}`);
      if (resp.ok && data?.status === 'ok') {
        console.table(data.dados || []);
        renderConsultaTabela(data.dados || [], competencia);
      } else {
        console.error('Erro na consulta:', data?.message || data);
        renderConsultaTabela([], competencia);
      }
      console.groupEnd();

      showToast(resp.ok && data.status === 'ok' ? 'success' : 'error',
                resp.ok ? 'Consulta executada.' : (data?.message || 'Erro ao consultar.'));
    } catch (err) {
      console.error('Consulta Caixa ► erro', err);
      renderConsultaTabela([], competencia);
      showToast('error', 'Erro inesperado ao consultar.');
    } finally {
      consultaExecutar.disabled = false;
      consultaExecutar.innerHTML = original;
    }
  });

  // ---------- Seleção de CIAs ----------
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

  // ---------- Prosseguir -> Monta inputs de valores ----------
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

    // === MÁSCARA DE MOEDA: live format + caret estável + colagem em massa ===
    modalCamposCias.querySelectorAll('input.moeda').forEach(input => {
      // Helpers locais
      const sanitize = raw => raw.replace(/\./g, '').replace(/[^\d,]/g, '');
      const keepSingleComma = s => {
        const i = s.indexOf(',');
        return i === -1 ? s : s.slice(0, i + 1) + s.slice(i + 1).replace(/,/g, '');
      };
      const addThousands = intStr =>
        intStr.replace(/^0+(?=\d)/, '')
              .replace(/\B(?=(\d{3})+(?!\d))/g, '.');

      const formatLive = (raw) => {
        let s = keepSingleComma(sanitize(raw));
        if (s === '') return '';
        const parts = s.split(',');
        let intPart = parts[0] || '';
        let fracPart = parts[1] || '';

        if (raw.trim().startsWith(',') || (intPart === '' && fracPart !== '')) {
          intPart = '0';
        }
        fracPart = fracPart.replace(/\D/g, '').slice(0, 2);

        const intFormatted = intPart === '' ? '' : addThousands(intPart);
        return parts.length > 1 ? `${intFormatted},${fracPart}` : intFormatted;
      };

      const tokenCountAt = (str, pos) => {
        let count = 0;
        for (let i = 0; i < Math.min(pos, str.length); i++) {
          if (/\d|,/.test(str[i])) count++;
        }
        return count;
      };
      const caretFromTokenCount = (str, logical) => {
        if (logical <= 0) return 0;
        let seen = 0;
        for (let i = 0; i < str.length; i++) {
          if (/\d|,/.test(str[i])) {
            seen++;
            if (seen === logical) return i + 1;
          }
        }
        return str.length;
      };

      input.addEventListener('input', () => {
        const before = input.value;
        const caretLogical = tokenCountAt(before, input.selectionStart ?? before.length);
        const formatted = formatLive(before);
        input.value = formatted;
        const newPos = caretFromTokenCount(formatted, caretLogical);
        input.setSelectionRange(newPos, newPos);
      });

      input.addEventListener('blur', () => {
        const raw = input.value;
        if (!raw) return;
        let s = keepSingleComma(sanitize(raw));
        if (s === '') { input.value = ''; return; }

        let [intPart = '', fracPart = ''] = s.split(',');
        if (raw.trim().startsWith(',') || (intPart === '' && fracPart !== '')) {
          intPart = '0';
        }
        intPart = intPart === '' ? '0' : intPart.replace(/^0+(?=\d)/, '');
        fracPart = (fracPart || '').replace(/\D/g, '').slice(0, 2).padEnd(2, '0');

        const intFormatted = addThousands(intPart);
        input.value = `${intFormatted},${fracPart}`;
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
        for (const r of rows) flat.push(...r.split('\t'));

        for (let i = 0; i < flat.length && (startIndex + i) < inputs.length; i++) {
          const raw = flat[i].replace(/R\$/g, '').replace(/\./g, '').replace(',', '.').trim();
          const v = parseFloat(raw);
          if (!isNaN(v)) {
            inputs[startIndex + i].value = v.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
          }
        }
      });

      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          input.blur();
          const inputs = [...modalCamposCias.querySelectorAll('input.moeda')];
          const idx = inputs.indexOf(input);
          if (idx > -1 && idx + 1 < inputs.length) inputs[idx + 1].focus();
        }
      });
    });

    openModal();
  });

  // ---------- Envio para buscar_cias_api (insere e depois confirma updates) ----------
  async function postCaixa(forcarUpdate=false){
    const selected = JSON.parse(hiddenSelected.value || '[]');
    const fd = new FormData(form);
    fd.set('cias', JSON.stringify(selected));
    if (forcarUpdate) fd.set('forcar_update', 'true');

    const csrftoken = document.querySelector('input[name=csrfmiddlewaretoken]')?.value || '';

    const originalText = modalAtualizar.innerHTML;
    modalAtualizar.disabled = true;
    modalAtualizar.innerHTML = '<i class="bi bi-arrow-repeat"></i> Processando...';

    try{
      const resp = await fetch(ENDPOINT_BUSCAR_CIAS, {
        method: 'POST',
        headers: {'X-Requested-With': 'XMLHttpRequest', 'X-CSRFToken': csrftoken},
        body: fd
      });
      const data = await resp.json().catch(()=>null);

      closeModal();

      if (resp.ok && data){
        if (data.status === 'ok'){
          showToast('success', data.message || 'Processo concluído.');
        } else if (data.status === 'existe'){
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

  modalAtualizar?.addEventListener('click', async () => {
    if (!form.reportValidity()) return;
    await postCaixa(false);
  });
  confirmYes?.addEventListener('click', async () => {
    closeConfirm();
    await postCaixa(true);
  });
});
