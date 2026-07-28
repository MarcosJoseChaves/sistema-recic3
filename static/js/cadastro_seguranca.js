function escaparHtml(valor) {
    return String(valor ?? "").replace(/[&<>"']/g, (caractere) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
    })[caractere]);
}

function idNumericoSeguro(valor) {
    if (valor === null || valor === undefined || valor === "") return null;
    const numero = Number(valor);
    return Number.isSafeInteger(numero) && numero >= 0 ? numero : null;
}

let redirecionandoParaLogin = false;
function tratarSessaoExpirada(statusHttp) {
    if (statusHttp !== 401 || redirecionandoParaLogin) return false;
    redirecionandoParaLogin = true;
    alert("Sua sessão terminou. Entre novamente para continuar.");
    window.location.assign("/login");
    return true;
}

let avisoLimiteExcedidoAtivo = false;
function tratarLimiteExcedido(statusHttp) {
    if (statusHttp !== 429 || avisoLimiteExcedidoAtivo) return false;
    avisoLimiteExcedidoAtivo = true;
    alert("Muitas solicitações. Aguarde um pouco e tente novamente.");
    window.setTimeout(() => {
        avisoLimiteExcedidoAtivo = false;
    }, 1000);
    return true;
}

$(document).ajaxSend(function (_evento, xhr, configuracao) {
    const metodo = (configuracao.type || "GET").toUpperCase();
    const destino = new URL(configuracao.url, window.location.href);
    if (
        ["POST", "PUT", "PATCH", "DELETE"].includes(metodo)
        && destino.origin === window.location.origin
    ) {
        xhr.setRequestHeader(
            "X-CSRFToken",
            document.querySelector('meta[name="csrf-token"]').content,
        );
    }
});

$(document).ajaxError(function (_evento, xhr) {
    if (!tratarSessaoExpirada(xhr.status)) {
        tratarLimiteExcedido(xhr.status);
    }
});
