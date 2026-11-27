// Prévia local aproximada. Persistência e validação pertencem ao servidor.
(() => {
  const form = document.querySelector('#trcs-form');
  if (!form) return;
  const output = document.querySelector('#trcs-preview');
  function preview() {
    const raw = name => form.elements[name].value.trim();
    if (!raw('entrada') || !raw('rejeitos')) {
      output.textContent = 'Prévia indisponível: informe ambas as massas.';
      return;
    }
    const decimal = /^[+]?(?:\d+(?:\.\d*)?|\.\d+)$/;
    if (![raw('entrada'), raw('rejeitos')].every(x => decimal.test(x))) {
      output.textContent = 'Use números decimais com ponto. O servidor validará os valores.';
      return;
    }
    const kg = name => Number(raw(name)) * (raw('unidade_' + name) === 't' ? 1000 : 1);
    const input = kg('entrada'), rejects = kg('rejeitos');
    if (!Number.isFinite(input) || !Number.isFinite(rejects) || input <= 0 || rejects < 0) {
      output.textContent = 'Prévia indisponível para esses valores; confira as massas.';
      return;
    }
    const result = 100 * rejects / input;
    output.textContent = Number.isFinite(result)
      ? 'Prévia aproximada da TRCS: ' + result.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '% · bruta e provisória.'
      : 'Prévia indisponível; confira as massas.';
  }
  form.addEventListener('input', preview);
  form.addEventListener('change', preview);
  preview();
})();
