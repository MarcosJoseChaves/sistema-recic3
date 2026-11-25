"""Testes unitários/simulados; nenhuma conexão é criada por descoberta."""
import hashlib
from decimal import Decimal, localcontext
from unittest import TestCase
from uuid import uuid4
from modulos.esg.contratos import FonteDocumental,Periodo,Perimetro
from modulos.esg.maz.modelos import ComponenteMassa,UnidadeMassa,OrigemValor
from modulos.esg.maz.persistencia import (AdaptadorMAZ,normalizar,validar_especificacao,
    ContratoESGInvalido,ConfirmacaoIncertaMAZ,PersistenciaMAZError)
RAW=b'{"formula":"TRCS_BRUTA_100_REJEITOS_ENTRADA","contrato_numerico":"R2-kg-1e9-6"}'
class TestContratoPersistencia(TestCase):
    def comp(self,v,unit=UnidadeMassa.KG):
        return ComponenteMassa(None if v is None else Decimal(v),unit,Periodo(2048,2),Perimetro('p','P'),FonteDocumental('f','F'),OrigemValor.DECLARADO)
    def test_utf8_hash(self):self.assertEqual(validar_especificacao(RAW,hashlib.sha256(RAW).hexdigest()),RAW.decode())
    def test_hash_incompativel(self):
        with self.assertRaises(ContratoESGInvalido):validar_especificacao(RAW,'0'*64)
    def test_duplicatas(self):
        raw=RAW[:-1]+b',"formula":"TRCS_BRUTA_100_REJEITOS_ENTRADA"}'
        with self.assertRaises(ContratoESGInvalido):validar_especificacao(raw,hashlib.sha256(raw).hexdigest())
    def test_unicode_numero_sem_float(self):
        raw=RAW[:-1]+',"unicode":"ação 😀","numero":1.123456789012345678901234567890}'.encode()
        self.assertEqual(validar_especificacao(raw,hashlib.sha256(raw).hexdigest()).encode(),raw)
    def test_nan_json(self):
        raw=RAW[:-1]+b',"n":NaN}'
        with self.assertRaises(ContratoESGInvalido):validar_especificacao(raw,hashlib.sha256(raw).hexdigest())
    def test_ausencia_zero(self):
        self.assertIsNone(normalizar(self.comp(None)).massa)
        self.assertEqual(normalizar(self.comp('0')).massa,Decimal(0))
    def test_teto_inclusivo(self):self.assertEqual(normalizar(self.comp('1000000000')).massa,Decimal('1e9'))
    def test_excesso_magnitude(self):
        with self.assertRaises(ContratoESGInvalido):normalizar(self.comp('1000000000.000001'))
    def test_excesso_escala(self):
        with self.assertRaises(ContratoESGInvalido):normalizar(self.comp('0.0000001'))
    def test_tonelada_exata(self):self.assertEqual(normalizar(self.comp('0.000000001',UnidadeMassa.TONELADA)).massa,Decimal('0.000001'))
    def test_contexto_decimal_externo(self):
        with localcontext() as ctx:
            ctx.prec=2
            self.assertEqual(normalizar(self.comp('1234.567891')).massa,Decimal('1234.567891'))
    def test_validacao_antes_conexao(self):
        def denied():raise AssertionError('Factory must not be called')
        with self.assertRaises(ContratoESGInvalido):AdaptadorMAZ(denied).registrar(self.comp('1'),self.comp('0.0000001'),'v',ator=1,request_id=uuid4(),motivo='teste')
    def test_periodo_incompativel(self):
        from dataclasses import replace
        with self.assertRaises(ContratoESGInvalido):AdaptadorMAZ(lambda:None).registrar(self.comp('1'),replace(self.comp('1'),periodo=Periodo(2049)), 'v',ator=1,request_id=uuid4(),motivo='teste')
    def test_fabrica_obrigatoria(self):
        with self.assertRaises(ContratoESGInvalido):AdaptadorMAZ(None)
    def test_conexao_compartilhada_invalida(self):
        class C:closed=False;autocommit=True
        with self.assertRaises(PersistenciaMAZError):
            with AdaptadorMAZ(lambda:C())._tx():pass
    def test_commit_incerto_sem_retry_simulado(self):
        class Q:
            def __enter__(self):return self
            def __exit__(self,*a):pass
        class C:
            closed=False;autocommit=False;calls=0
            def get_transaction_status(self):return 0
            def set_session(self,**kw):pass
            def cursor(self):return Q()
            def commit(self):self.calls+=1;raise OSError('synthetic')
            def rollback(self):pass
            def close(self):pass
        c=C()
        with self.assertRaises(ConfirmacaoIncertaMAZ):
            with AdaptadorMAZ(lambda:c)._tx():pass
        self.assertEqual(c.calls,1)

    def test_mensagem_unicode_corrigida(self):
        with self.assertRaisesRegex(ContratoESGInvalido,'Componente R1 obrigatório'):
            normalizar(None)
