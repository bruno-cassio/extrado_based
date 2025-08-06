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

  form.addEventListener("submit", async function (e) {
    e.preventDefault();

    const cia = form.querySelector("select[name='cia']").value;
    const mes = form.querySelector("input[name='mes']").value;
    const valor = form.querySelector("input[name='valor']").value;

    const formData = new FormData();
    formData.append("cia", cia);
    formData.append("mes", mes);

    try {
      const response = await fetch("/api/buscar-cias/", {
        method: "POST",
        body: formData
      });

      const data = await response.json();

      console.log("✅ Cias existentes:", data.existing);
      console.log("❌ Cias não encontradas:", data.non_existing);
      console.log("📄 Lista final:", data.lista_cias);
      console.log("🆔 ID da CIA:", data.id_cia);
      console.log("💰 Valor digitado:", valor);

    //   form.submit();

    } catch (error) {
      console.error("Erro ao buscar cias:", error);
    }
  });
});
