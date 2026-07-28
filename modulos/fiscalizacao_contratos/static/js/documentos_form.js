(() => {
    const contrato = document.getElementById("contrato_id");
    const aditivo = document.getElementById("aditivo_id");

    function filtrarAditivos() {
        const contratoId = contrato.value;
        for (const opcao of aditivo.options) {
            if (!opcao.value) continue;
            opcao.hidden = Boolean(contratoId)
                && opcao.dataset.contrato !== contratoId;
            if (opcao.selected && opcao.hidden) aditivo.value = "";
        }
    }

    contrato.addEventListener("change", filtrarAditivos);
    filtrarAditivos();
})();
