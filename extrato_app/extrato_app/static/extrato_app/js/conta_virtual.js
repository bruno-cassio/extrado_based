
document.addEventListener('DOMContentLoaded', () => {

    const ciaItems = document.querySelectorAll('.cia-item');
    const selectedCount = document.getElementById('selected-count');
    const ciasSelectedInput = document.getElementById('cias-selected');
    const selectAllBtn = document.getElementById('select-all');
    const deselectAllBtn = document.getElementById('deselect-all');

    let selectedCias = [];

    function updateSelections() {
        if (selectedCount) selectedCount.textContent = selectedCias.length;
        if (ciasSelectedInput) ciasSelectedInput.value = JSON.stringify(selectedCias);
        ciaItems.forEach(item => {
            const ciaName = item.dataset.cia;
            item.classList.toggle('selected', selectedCias.includes(ciaName));
        });
    }

    ciaItems.forEach(item => {
        item.addEventListener('click', () => {
            const ciaName = item.dataset.cia;
            const index = selectedCias.indexOf(ciaName);
            index === -1 ? selectedCias.push(ciaName) : selectedCias.splice(index, 1);
            updateSelections();
        });
    });

    if (selectAllBtn) {
        selectAllBtn.addEventListener('click', () => {
            selectedCias = Array.from(ciaItems).map(item => item.dataset.cia);
            updateSelections();
        });
    }

    if (deselectAllBtn) {
        deselectAllBtn.addEventListener('click', () => {
            selectedCias = [];
            updateSelections();
        });
    }

    document.querySelectorAll('input[name="mes"]').forEach((competenciaInput) => {
        competenciaInput.addEventListener('input', function (e) {
            let value = e.target.value.replace(/\D/g, '');
            if (value.length > 2) {
                value = value.substring(0, 2) + '-' + value.substring(2, 6);
            }
            e.target.value = value;
        });
    });

    document.querySelectorAll('input[name="valor"]').forEach((input) => {
        input.addEventListener('input', function (e) {
            let value = e.target.value.replace(/\D/g, '');
            if (value.length === 0) {
                e.target.value = '';
                return;
            }
            let numericValue = parseFloat(value) / 100;
            e.target.value = numericValue.toLocaleString('pt-BR', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            });
        });
    });

    function getCookie(name) {
        let cookieValue = null;
        if (document.cookie && document.cookie !== '') {
            const cookies = document.cookie.split(';');
            for (let cookie of cookies) {
                cookie = cookie.trim();
                if (cookie.startsWith(name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    }

    function showNotification(message, type) {
        const notification = document.getElementById('notification');
        const icon = document.getElementById('notification-icon');
        const messageEl = document.getElementById('notification-message');

        notification.className = 'notification';
        icon.className = '';

        if (type === 'success') {
            notification.classList.add('success');
            icon.classList.add('bi', 'bi-check-circle-fill');
        } else if (type === 'error') {
            notification.classList.add('error');
            icon.classList.add('bi', 'bi-x-circle-fill');
        } else if (type === 'loading') {
            notification.classList.add('info');
            icon.innerHTML = '<div class="spinner"></div>';
        } else {
            notification.classList.add('info');
            icon.classList.add('bi', 'bi-info-circle-fill');
        }

        messageEl.textContent = message;
        notification.classList.add('show');

        if (type !== 'loading') {
            setTimeout(() => {
                notification.classList.remove('show');
            }, 5000);
        }
    }

    function iniciarDownloadQuandoArquivosEstiveremProntos(uniqueId) {
        const verificarArquivos = setInterval(() => {
            fetch(`/media/finished_${uniqueId}.txt`, { method: 'HEAD' })
                .then(async (res) => {
                    if (res.ok) {
                        clearInterval(verificarArquivos);
                        hideDownloadPopup();

                        const link = document.createElement('a');
                        link.href = `/baixar_resumo?id=${uniqueId}`;
                        link.download = `${uniqueId}.xlsx`;
                        document.body.appendChild(link);
                        link.click();
                        link.remove();

                        setTimeout(() => {
                            fetch(`/media/${uniqueId}.txt`, { method: 'HEAD' })
                                .then(r => {
                                    if (r.ok) {
                                        const txtLink = document.createElement('a');
                                        txtLink.href = `/media/${uniqueId}.txt`;
                                        txtLink.download = `${uniqueId}.txt`;
                                        document.body.appendChild(txtLink);
                                        txtLink.click();
                                        txtLink.remove();
                                    } else {
                                        console.warn("Arquivo .txt não encontrado ainda.");
                                    }
                                });
                        }, 1000);

                        showNotification('✅ Extração concluída! Download iniciado.', 'success');

                        setTimeout(() => {
                            fetch(`/limpar_arquivos?id=${uniqueId}`, {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'X-CSRFToken': getCookie('csrftoken')
                                }
                            });
                        }, 5000);
                    }
                })
                .catch(err => console.error('Erro ao verificar arquivos:', err));
        }, 2000);
    }

    async function submitForm(event) {
        event.preventDefault();

        const competenciaInput = event.target.querySelector('input[name="mes"]');

        if (ciaItems.length > 0 && selectedCias.length === 0) {
            showNotification('Por favor, selecione pelo menos uma seguradora!', 'error');
            return;
        }

        if (!competenciaInput.value.match(/^(0[1-9]|1[0-2])-[0-9]{4}$/)) {
            showNotification('Formato de competência inválido! Use o formato MM-AAAA.', 'error');
            return;
        }

        // showNotification('Iniciando extração, aguarde...', 'loading');
        showDownloadPopup();

        try {
            const formData = new FormData(event.target);
            const response = await fetch(window.djangoURL, {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': window.csrfToken
                }
            });

            const data = await response.json();

            if (data.status === 'success') {
                showNotification(data.message, 'success');
                showDownloadPopup();
                iniciarDownloadQuandoArquivosEstiveremProntos(data.id);
            } else {
                showNotification(data.message, 'error');
            }
        } catch (error) {
            showNotification('Erro ao conectar com o servidor', 'error');
            console.error('Erro:', error);
        }
    }

    const form = document.querySelector('form');
    if (form) form.addEventListener('submit', submitForm);

    updateSelections();
});

function showDownloadPopup() {
    const popup = document.getElementById('download-popup');
    popup.classList.remove('popup-hidden');
}

function hideDownloadPopup() {
    const popup = document.getElementById('download-popup');
    popup.classList.add('popup-hidden');
}
