from datetime import datetime
from typing import Any, List
from io import BytesIO
import json

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import openpyxl
import pandas

from airflow.providers.tesouro_gerencial.hooks.tesouro_gerencial \
    import TesouroGerencialHook
from airflow.providers.tesouro_gerencial.utils.excel import consolidar_tabela


class RelatorioParaMongo(BaseOperator):
    '''Realiza o download de um relatório do Tesouro Gerencial para um
    coleção de banco MongoDB.

    :param id_conexao_siafi: ID de conta do SIAFI cadastrada no Airflow
    :type id_conexao_siafi: str
    :param id_relatorio: ID de relatório existente no Tesouro Gerencial
    :type id_relatorio:
    :param id_conexao_mongo: ID de conexão ao MongoDB cadastrada no Airflow
    :type id_conexao_mongo: str
    :param banco: Nome do banco
    :type banco: str
    :param colecao: Nome da coleção
    :type colecao: str
    :param respostas_prompts_valor: lista com respostas de prompts de valor,
    respeitando sua ordem conforme consta no relatório
    :type respostas_prompts_valor: List[str]
    :param truncar_colecao: `True` se coleção deve ser truncada antes da
    inserção e `False` caso contrário
    '''
    template_fields = [
        'id_relatorio', 'respostas_prompts_valor', 'banco', 'colecao'
    ]

    id_conexao_siafi: str
    id_relatorio: str
    respostas_prompts_valor: List[str]

    id_conexao_mongo: str
    banco: str
    colecao: str
    truncar_colecao: bool

    def __init__(
        self,
        id_conexao_siafi: str,
        id_relatorio: str,
        id_conexao_mongo: str,
        respostas_prompts_valor: List[str] = None,
        banco: str = None,
        colecao: str = 'teste',
        truncar_colecao: bool = False,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.id_conexao_siafi = id_conexao_siafi
        self.id_relatorio = id_relatorio
        self.respostas_prompts_valor = respostas_prompts_valor
        self.id_conexao_mongo = id_conexao_mongo
        self.banco = banco
        self.colecao = colecao
        self.truncar_colecao = truncar_colecao

    def execute(self, context: Any) -> dict:
        self.log.info(
            'Transferindo relatório "%s" para coleção do Mongo "%s" com as '
            'seguintes respostas para prompts: "%s"%s',
            self.id_relatorio, self.colecao,
            self.respostas_prompts_valor,
            '. Truncando coleção' if self.truncar_colecao else ''
        )

        if isinstance(self.respostas_prompts_valor, str):
            respostas_prompts_valor = json.loads(self.respostas_prompts_valor)
        else:
            respostas_prompts_valor = self.respostas_prompts_valor

        with TesouroGerencialHook(self.id_conexao_siafi) as hook:
            instante = datetime.now()

            try:
                relatorio = hook.retorna_relatorio(
                    id_relatorio=self.id_relatorio,
                    formato='excel',
                    respostas_prompts_valor=respostas_prompts_valor
                )
            except AirflowException as excecao:
                resposta = excecao.args[0]

                if resposta.status_code == 500:
                    raise AirflowException(
                        'Erro 500 no servidor. Provável que o relatório ainda '
                        'não esteja pronto para ser exportado. Por favor, '
                        'tente novamente.'
                    ) from None

                raise

        with BytesIO() as arquivo:
            arquivo.write(relatorio)
            arquivo.seek(0)

            livro_excel = openpyxl.load_workbook(arquivo)

        livro_excel = consolidar_tabela(livro_excel)

        with BytesIO() as arquivo:
            livro_excel.save(arquivo)
            dados = pandas.read_excel(arquivo)

        dados.columns = dados.columns.str.replace('.', '', regex=False)
        dados['Timestamp'] = instante

        with MongoHook(self.id_conexao_mongo) as hook:
            if self.truncar_colecao:
                hook.delete_many(self.colecao, {}, self.banco)

            inseridos = hook.insert_many(
                self.colecao, dados.to_dict('records'), self.banco
            ).inserted_ids

        self.log.info(
            'Relatório transferido com sucesso, tendo produzido %s registros',
            len(inseridos)
        )

        self.xcom_push(context, 'registros_inseridos', len(inseridos))
