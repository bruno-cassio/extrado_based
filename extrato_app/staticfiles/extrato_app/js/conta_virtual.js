document.addEventListener('DOMContentLoaded', () => {

    const ciaItems = document.querySelectorAll('.cia-item');
    const selectedCount = document.getElementById('selected-count');
    const ciasSelectedInput = document.getElementById('cias-selected');
    const selectAllBtn = document.getElementById('select-all');
    const deselectAllBtn = document.getElementById('deselect-all');
    const competenciaInput = document.querySelector('input[name="mes"]');

    let selectedCias = [];

    function updateSelections() {
        selectedCount.textContent = selectedCias.length;
        ciasSelectedInput.value = JSON.stringify(selectedCias);
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

    selectAllBtn.addEventListener('click', () => {
        selectedCias = Array.from(ciaItems).map(item => item.dataset.cia);
        updateSelections();
    });

    deselectAllBtn.addEventListener('click', () => {
        selectedCias = [];
        updateSelections();
    });

    competenciaInput.addEventListener('input', function (e) {
        let value = e.target.value.replace(/\D/g, '');
        if (value.length > 2) {
            value = value.substring(0, 2) + '-' + value.substring(2, 6);
        }
        e.target.value = value;
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

    function iniciarDownloadQuandoPronto(uniqueId) {
        const intervalo = setInterval(() => {
            fetch(`/media/finished_${uniqueId}.txt`)
                .then(response => {
                    if (response.ok) {
                        clearInterval(intervalo);
                        const link = document.createElement('a');
                        link.href = `/baixar_resumo?id=${uniqueId}`;
                        link.download = `resumo_${uniqueId}.xlsx`;
                        document.body.appendChild(link);
                        link.click();
                        link.remove();

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
                .catch(() => {});
        }, 3000);
    }

    async function submitForm(event) {
        event.preventDefault();

        if (selectedCias.length === 0) {
            showNotification('Por favor, selecione pelo menos uma seguradora!', 'error');
            return;
        }

        if (!competenciaInput.value.match(/^(0[1-9]|1[0-2])-[0-9]{4}$/)) {
            showNotification('Formato de competência inválido! Use o formato MM-AAAA.', 'error');
            return;
        }

        showNotification('Iniciando extração, aguarde...', 'loading');

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
                iniciarDownloadQuandoPronto(data.id);
            } else {
                showNotification(data.message, 'error');
            }
        } catch (error) {
            showNotification('Erro ao conectar com o servidor', 'error');
            console.error('Erro:', error);
        }
    }

    document.querySelector('form').addEventListener('submit', submitForm);
    updateSelections();
});
