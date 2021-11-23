from datetime import datetime
from io import BytesIO
from typing import Any, List
import json
import os

from airflow.models.baseoperator import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import humanize
import pandas

from airflow.providers.tesouro_gerencial.hooks.tesouro_gerencial \
    import TesouroGerencialHook


class RelatorioParaArquivo(BaseOperator):
    '''Realiza o download de um relatório do Tesouro Gerencial para um
    arquivo local.

    :param id_conta_siafi: ID de conta do SIAFI cadastrada no Airflow
    :type id_conta_siafi: str
    :param id_relatorio: ID de relatório existente no Tesouro Gerencial
    :type id_relatorio:
    :param caminho_arquivo: caminho do arquivo onde relatório será salvo. Sua
    extensão determinará como o arquivo será gerado no Tesouro Gerencial, sendo
    possível configurar arquivos do tipo ".csv", ".xls", ".xlsx" ou ".pdf"
    :type caminho_arquivo: str
    :param respostas_prompts_valor: lista com respostas de prompts de valor,
    respeitando sua ordem conforme consta no relatório
    :type respostas_prompts_valor: List[str]
    '''
    template_fields = [
        'id_relatorio', 'caminho_arquivo', 'respostas_prompts_valor'
    ]

    id_conta_siafi: str
    id_relatorio: str
    respostas_prompts_valor: List[str]
    caminho_arquivo: str

    def __init__(
        self,
        id_conta_siafi: str,
        id_relatorio: str,
        caminho_arquivo: str,
        respostas_prompts_valor: List[str] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.id_conta_siafi = id_conta_siafi
        self.id_relatorio = id_relatorio
        self.caminho_arquivo = caminho_arquivo
        self.respostas_prompts_valor = respostas_prompts_valor

    def execute(self, context: Any) -> None:
        extensao = os.path.splitext(self.caminho_arquivo)[1]

        if extensao == '.csv':
            formato = TesouroGerencialHook.FORMATO.CSV
        elif extensao == '.xlsx' or extensao == '.xls':
            formato = TesouroGerencialHook.FORMATO.EXCEL
        elif extensao == '.pdf':
            formato = TesouroGerencialHook.FORMATO.PDF
        else:
            self.log.warning(
                'Extensão "%s" inválida, salvando relatório no formato CSV, '
                'sem alterar o nome do arquivo', extensao
            )
            formato = TesouroGerencialHook.FORMATO.CSV

        if isinstance(self.respostas_prompts_valor, str):
            respostas_prompts_valor = json.loads(self.respostas_prompts_valor)
        else:
            respostas_prompts_valor = self.respostas_prompts_valor

        with TesouroGerencialHook(self.id_conta_siafi) as hook:
            relatorio = hook.retorna_relatorio(
                id_relatorio=self.id_relatorio,
                formato=formato,
                respostas_prompts_valor=respostas_prompts_valor
            )

        with open(self.caminho_arquivo, 'wb') as arquivo:
            arquivo.write(relatorio)

        self.xcom_push(
            context, 'caminho', os.path.abspath(self.caminho_arquivo)
        )
        self.xcom_push(
            context, 'tamanho', humanize.naturalsize(len(relatorio))
        )


class RelatorioParaMongo(BaseOperator):
    '''Realiza o download de um relatório do Tesouro Gerencial para um
    coleção de banco MongoDB.

    :param id_conta_siafi: ID de conta do SIAFI cadastrada no Airflow
    :type id_conta_siafi: str
    :param id_relatorio: ID de relatório existente no Tesouro Gerencial
    :type id_relatorio:
    :param id_conexao_mongo: ID de conexão ao MongoDB cadastrada no Airflow
    :type id_conexao_mongo: str
    :param respostas_prompts_valor: lista com respostas de prompts de valor,
    respeitando sua ordem conforme consta no relatório
    :type respostas_prompts_valor: List[str]
    :return: caminho absoluto de arquivo salvo e seu tamanho
    :rtype: Tuple[str, int]
    '''
    template_fields = [
        'id_relatorio', 'respostas_prompts_valor', 'nome_colecao'
    ]

    id_conta_siafi: str
    id_relatorio: str
    respostas_prompts_valor: List[str]

    id_conexao_mongo: str
    nome_colecao: str
    truncar_colecao: bool

    def __init__(
        self,
        id_conta_siafi: str,
        id_relatorio: str,
        id_conexao_mongo: str,
        nome_colecao: str,
        respostas_prompts_valor: List[str] = None,
        truncar_colecao: bool = False,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.id_conta_siafi = id_conta_siafi
        self.id_relatorio = id_relatorio
        self.respostas_prompts_valor = respostas_prompts_valor
        self.id_conexao_mongo = id_conexao_mongo
        self.nome_colecao = nome_colecao
        self.truncar_colecao = truncar_colecao

    def execute(self, context: Any) -> dict:
        if isinstance(self.respostas_prompts_valor, str):
            respostas_prompts_valor = json.loads(self.respostas_prompts_valor)
        else:
            respostas_prompts_valor = self.respostas_prompts_valor

        with TesouroGerencialHook(self.id_conta_siafi) as hook:
            instante = datetime.now()

            relatorio = hook.retorna_relatorio(
                id_relatorio=self.id_relatorio,
                formato='excel',
                respostas_prompts_valor=respostas_prompts_valor
            )

        with BytesIO() as arquivo:
            arquivo.write(relatorio)
            arquivo.seek(0)

            tabela = pandas.read_excel(arquivo, header=None)

        # Separa metadados de registros
        inicio = tabela.head(10)
        indice_cabecalho = inicio.isnull().all(axis=1).iloc[::-1].idxmax() + 1
        metadado = tabela.loc[:indice_cabecalho - 1]
        cabecalho = tabela.loc[indice_cabecalho:indice_cabecalho + 1]
        dado = tabela.loc[indice_cabecalho + 2:]

        # Prepara metadados
        metadado = metadado.fillna('')
        metadado = metadado.apply(' '.join, axis=1).str.strip()
        metadado = '\n'.join(metadado.loc[metadado != ''])

        # Prepara cabeçalho
        cabecalho = cabecalho.fillna('')
        cabecalho = cabecalho.apply(
            lambda coluna: ' - '.join(filter(None, coluna))
            or f'Coluna {coluna.name}'
        )

        # Prepara dados
        dado.columns = cabecalho
        dado['Metadado'] = metadado
        dado['Timestamp'] = instante

        with MongoHook(self.id_conexao_mongo) as hook:
            if self.truncar_colecao:
                hook.delete_many(self.nome_colecao, {})

            inseridos = hook.insert_many(
                self.nome_colecao, dado.to_dict('records')
            ).inserted_ids

        self.xcom_push(context, 'registros_inseridos', len(inseridos))
