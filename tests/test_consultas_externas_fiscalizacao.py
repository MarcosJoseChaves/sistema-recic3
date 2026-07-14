"""Testes isolados das consultas externas, sem chamadas reais de rede."""

import unittest
from unittest.mock import MagicMock, patch

import requests

from modulos.fiscalizacao_contratos.services.consultas_externas import (
    ConsultaExternaError,
    consultar_cep,
    consultar_cnpj,
)


CNPJ_VALIDO = "04252011000110"


def resposta_json(dados, status=200):
    resposta = MagicMock()
    resposta.status_code = status
    resposta.json.return_value = dados
    if status >= 400:
        resposta.raise_for_status.side_effect = requests.HTTPError(response=resposta)
    return resposta


class TestConsultasExternasFiscalizacao(unittest.TestCase):
    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_brasilapi_funciona_sem_chamar_opencnpja(self, get_mock):
        get_mock.return_value = resposta_json(
            {
                "razao_social": "Empresa Brasil Ltda",
                "nome_fantasia": "Empresa Brasil",
                "cep": "01310-100",
                "logradouro": "Avenida Paulista",
                "numero": "1000",
                "bairro": "Bela Vista",
                "municipio": "São Paulo",
                "uf": "sp",
                "ddd_telefone_1": "11",
                "telefone_1": "30004000",
                "email": "contato@empresa.test",
            }
        )

        dados = consultar_cnpj(CNPJ_VALIDO)

        self.assertEqual(get_mock.call_count, 1)
        self.assertIn("brasilapi.com.br", get_mock.call_args.args[0])
        self.assertEqual(dados["razao_social"], "Empresa Brasil Ltda")
        self.assertEqual(dados["nome_fantasia"], "Empresa Brasil")
        self.assertEqual(dados["cep"], "01310100")
        self.assertEqual(dados["uf"], "SP")
        self.assertEqual(dados["telefone"], "(11) 30004000")
        self.assertEqual(dados["email"], "contato@empresa.test")

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_brasilapi_falha_e_opencnpja_funciona(self, get_mock):
        brasil = resposta_json({}, status=500)
        opencnpj = resposta_json(
            {
                "company": {"name": "Empresa Alternativa SA"},
                "alias": "Alternativa",
                "address": {
                    "zip": "20040-020",
                    "street": "Avenida Central",
                    "number": "20",
                    "district": "Centro",
                    "city": "Rio de Janeiro",
                    "state": "rj",
                },
                "phones": [{"area": "21", "number": "22223333"}],
                "emails": [{"address": "contato@alternativa.test"}],
            }
        )
        get_mock.side_effect = [brasil, opencnpj]

        with self.assertLogs(
            "modulos.fiscalizacao_contratos.services.consultas_externas", level="INFO"
        ) as logs:
            dados = consultar_cnpj(CNPJ_VALIDO)

        self.assertEqual(get_mock.call_count, 2)
        self.assertIn("open.cnpja.com", get_mock.call_args_list[1].args[0])
        self.assertEqual(dados["razao_social"], "Empresa Alternativa SA")
        self.assertEqual(dados["nome_fantasia"], "Alternativa")
        self.assertEqual(dados["telefone"], "(21) 22223333")
        self.assertEqual(dados["email"], "contato@alternativa.test")
        registro = " ".join(logs.output)
        self.assertIn("status_http=500", registro)
        self.assertIn("erro_tipo=resposta_http_invalida", registro)
        self.assertIn("segunda_opcao_acionada=True", registro)
        self.assertNotIn("https://", registro)

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_duas_apis_falham_com_mensagem_amigavel(self, get_mock):
        get_mock.side_effect = [
            requests.Timeout("falha simulada"),
            requests.ConnectionError("falha simulada"),
        ]

        with self.assertRaisesRegex(ConsultaExternaError, "Preencha os dados manualmente"):
            consultar_cnpj(CNPJ_VALIDO)

        self.assertEqual(get_mock.call_count, 2)

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_cnpj_invalido_nao_chama_api(self, get_mock):
        with self.assertRaisesRegex(ValueError, "CNPJ inválido"):
            consultar_cnpj("123")

        get_mock.assert_not_called()

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_json_invalido_aciona_opencnpja(self, get_mock):
        brasil = resposta_json({})
        brasil.json.side_effect = ValueError("JSON inválido simulado")
        get_mock.side_effect = [
            brasil,
            resposta_json({"company": {"name": "Empresa Recuperada"}}),
        ]

        dados = consultar_cnpj(CNPJ_VALIDO)

        self.assertEqual(get_mock.call_count, 2)
        self.assertEqual(dados["razao_social"], "Empresa Recuperada")

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_json_incompleto_nao_causa_erro(self, get_mock):
        get_mock.return_value = resposta_json({})

        dados = consultar_cnpj(CNPJ_VALIDO)

        self.assertTrue(all(valor == "" for valor in dados.values()))

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_telefone_brasilapi_usa_segunda_opcao_e_ausente_fica_vazio(self, get_mock):
        get_mock.side_effect = [
            resposta_json(
                {
                    "ddd_telefone_1": None,
                    "telefone_1": None,
                    "ddd_telefone_2": "31",
                    "telefone_2": "33334444",
                }
            ),
            resposta_json({}),
        ]

        com_segundo_telefone = consultar_cnpj(CNPJ_VALIDO)
        sem_telefone = consultar_cnpj(CNPJ_VALIDO)

        self.assertEqual(com_segundo_telefone["telefone"], "(31) 33334444")
        self.assertEqual(sem_telefone["telefone"], "")
        self.assertNotIn("None", com_segundo_telefone["telefone"])

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_opencnpja_sem_nome_fantasia_email_ou_telefone(self, get_mock):
        get_mock.side_effect = [
            requests.Timeout("falha simulada"),
            resposta_json({"company": {"name": "Empresa Sem Opcionais"}, "address": {}}),
        ]

        dados = consultar_cnpj(CNPJ_VALIDO)

        self.assertEqual(dados["razao_social"], "Empresa Sem Opcionais")
        self.assertEqual(dados["nome_fantasia"], "")
        self.assertEqual(dados["email"], "")
        self.assertEqual(dados["telefone"], "")

    @patch("modulos.fiscalizacao_contratos.services.consultas_externas.requests.get")
    def test_consulta_cep_continua_funcionando(self, get_mock):
        get_mock.return_value = resposta_json(
            {
                "street": "Praça da Sé",
                "neighborhood": "Sé",
                "city": "São Paulo",
                "state": "sp",
            }
        )

        dados = consultar_cep("01001-000")

        self.assertEqual(
            dados,
            {
                "logradouro": "Praça da Sé",
                "bairro": "Sé",
                "cidade": "São Paulo",
                "uf": "SP",
            },
        )
        self.assertEqual(get_mock.call_count, 1)
        self.assertIn("brasilapi.com.br", get_mock.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
