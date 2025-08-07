document.addEventListener("DOMContentLoaded", () => {
  const ciaItems = document.querySelectorAll(".cia-item");
  const selectedCount = document.getElementById("selected-count");
  const ciasSelectedInput = document.getElementById("cias-selected");
  const selectAllBtn = document.getElementById("select-all");
  const deselectAllBtn = document.getElementById("deselect-all");
  const form = document.querySelector("form");

  let selectedCias = [];

  function updateSelections() {
    if (selectedCount) selectedCount.textContent = selectedCias.length;
    if (ciasSelectedInput) ciasSelectedInput.value = JSON.stringify(selectedCias);
    ciaItems.forEach(item => {
      const ciaName = item.dataset.cia;
      item.classList.toggle("selected", selectedCias.includes(ciaName));
    });
  }

  ciaItems.forEach(item => {
    item.addEventListener("click", () => {
      const ciaName = item.dataset.cia;
      const index = selectedCias.indexOf(ciaName);
      index === -1 ? selectedCias.push(ciaName) : selectedCias.splice(index, 1);
      updateSelections();
    });
  });

  if (selectAllBtn) {
    selectAllBtn.addEventListener("click", () => {
      selectedCias = Array.from(ciaItems).map(item => item.dataset.cia);
      updateSelections();
    });
  }

  if (deselectAllBtn) {
    deselectAllBtn.addEventListener("click", () => {
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

  if (form) {
    form.addEventListener("submit", (e) => {
      console.log("🔄 Executando atualização de relatórios...");
    });
  }

  updateSelections();
});
