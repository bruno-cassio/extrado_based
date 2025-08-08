
document.addEventListener("DOMContentLoaded", function () {
  const form = document.querySelector("form");
  const btn = document.getElementById("btnAtualizarCaixa");

  document.querySelectorAll('input[name="mes"]').forEach((competenciaInput) => {
    competenciaInput.addEventListener('input', function (e) {
      let value = e.target.value.replace(/\D/g, '');
      if (value.length > 2) {
        value = value.substring(0, 2) + '-' + value.substring(2, 6);
      }
      e.target.value = value;
    });
  });

  document.querySelectorAll('input[name="valor_bruto"], input[name="valor_liquido"]').forEach((input) => {
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

  function mostrarAlerta(mensagem, tempo = 5000) {
    const alerta = document.getElementById("custom-alert");
    const conteudo = document.getElementById("custom-alert-content");

    conteudo.innerHTML = mensagem;
    alerta.style.display = "flex";

    setTimeout(() => {
      alerta.style.display = "none";
    }, tempo);
  }

  window.fecharAlerta = function () {
    document.getElementById("custom-alert").style.display = "none";
  };

  function mostrarConfirmacao(mensagem, aoConfirmar, aoCancelar) {
    const box = document.getElementById("custom-confirm");
    const content = document.getElementById("custom-confirm-content");
    const btnSim = document.getElementById("btnConfirmar");
    const btnNao = document.getElementById("btnCancelar");

    content.innerHTML = mensagem;
    box.style.display = "flex";

    btnSim.onclick = () => {
      box.style.display = "none";
      if (aoConfirmar) aoConfirmar();
    };

    btnNao.onclick = () => {
      box.style.display = "none";
      if (aoCancelar) aoCancelar();
    };
  }

  form.addEventListener("submit", async function (e) {
    e.preventDefault();

    const cia = form.querySelector("select[name='cia']").value;
    const mes = form.querySelector("input[name='mes']").value;
    const valorBruto = form.querySelector("input[name='valor_bruto']").value;
    const valorLiquido = form.querySelector("input[name='valor_liquido']").value;

    const formData = new FormData();
    formData.append("cia", cia);
    formData.append("mes", mes);
    formData.append("valor_bruto", valorBruto);
    formData.append("valor_liquido", valorLiquido);

    try {
      const response = await fetch("/api/buscar-cias/", {
        method: "POST",
        body: formData,
      });

      const data = await response.json();

      if (data.status === "existe") {
        mostrarConfirmacao(data.message, async () => {
          formData.append("forcar_update", "true");
          const responseUpdate = await fetch("/api/buscar-cias/", {
            method: "POST",
            body: formData,
          });
          const dataUpdate = await responseUpdate.json();
          mostrarAlerta(dataUpdate.message || "Atualizado com sucesso.");
        }, () => {
          mostrarAlerta("Atualização cancelada.");
        });
      } else {
        mostrarAlerta(data.message || "Processado com sucesso.");
      }

    } catch (error) {
      console.error("Erro ao buscar cias:", error);
      mostrarAlerta("Erro de conexão com o servidor.");
    }
  });
});
