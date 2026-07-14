(() => {
    const form = document.getElementById("empresaForm");
    if (!form) return;

    const somenteNumeros = (valor) => (valor || "").replace(/\D/g, "");
    const aviso = document.getElementById("consultaAviso");

    const mostrarAviso = (mensagem, tipo) => {
        aviso.textContent = mensagem;
        aviso.className = `alert alert-${tipo}`;
    };

    const consultar = async (url, botao) => {
        const conteudoOriginal = botao.innerHTML;
        botao.disabled = true;
        botao.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        try {
            const resposta = await fetch(url, { headers: { Accept: "application/json" } });
            const dados = await resposta.json();
            if (!resposta.ok) throw new Error(dados.erro || "Consulta indisponível.");
            aviso.className = "alert d-none";
            return dados;
        } catch (erro) {
            mostrarAviso(`${erro.message} Continue preenchendo manualmente.`, "warning");
            return null;
        } finally {
            botao.disabled = false;
            botao.innerHTML = conteudoOriginal;
        }
    };

    document.getElementById("consultarCnpj").addEventListener("click", async (evento) => {
        const cnpj = somenteNumeros(document.getElementById("cnpj").value);
        if (cnpj.length !== 14) {
            mostrarAviso("Digite os 14 dígitos do CNPJ antes de consultar.", "warning");
            return;
        }
        const url = form.dataset.consultaCnpj.replace("CNPJ_VALOR", cnpj);
        const dados = await consultar(url, evento.currentTarget);
        if (!dados) return;
        Object.entries(dados).forEach(([campo, valor]) => {
            const input = document.getElementById(campo);
            if (input && valor) input.value = valor;
        });
        mostrarAviso("Dados encontrados. Confira as informações antes de salvar.", "success");
    });

    document.getElementById("consultarCep").addEventListener("click", async (evento) => {
        const cep = somenteNumeros(document.getElementById("cep").value);
        if (cep.length !== 8) {
            mostrarAviso("Digite os 8 dígitos do CEP antes de consultar.", "warning");
            return;
        }
        const url = form.dataset.consultaCep.replace("CEP_VALOR", cep);
        const dados = await consultar(url, evento.currentTarget);
        if (!dados) return;
        Object.entries(dados).forEach(([campo, valor]) => {
            const input = document.getElementById(campo);
            if (input && valor) input.value = valor;
        });
        mostrarAviso("Endereço encontrado. Confira as informações antes de salvar.", "success");
    });

    form.addEventListener("submit", () => {
        document.getElementById("cnpj").value = somenteNumeros(document.getElementById("cnpj").value);
        document.getElementById("cep").value = somenteNumeros(document.getElementById("cep").value);
        document.getElementById("uf").value = document.getElementById("uf").value.trim().toUpperCase();
    });
})();
