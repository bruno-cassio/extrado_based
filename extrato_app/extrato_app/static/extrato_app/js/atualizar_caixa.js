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
        const confirmar = confirm(data.message);
        if (confirmar) {
          formData.append("forcar_update", "true");
          const responseUpdate = await fetch("/api/buscar-cias/", {
            method: "POST",
            body: formData,
          });

          const dataUpdate = await responseUpdate.json();
          alert(dataUpdate.message || "Atualizado com sucesso.");
        } else {
          alert("Atualização cancelada.");
        }
      } else {
        alert(data.message || "Processado com sucesso.");
      }

    } catch (error) {
      console.error("Erro ao buscar cias:", error);
    }
  });
});