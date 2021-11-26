from typing import Any, List
import json
import os
from airflow.exceptions import AirflowException

from airflow.models.baseoperator import BaseOperator
import humanize

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
        self.log.info(
            'Transferindo relatório "%s" para "%s" com as seguintes respostas '
            'para prompts: "%s"',
            self.id_relatorio, self.caminho_arquivo,
            self.respostas_prompts_valor
        )

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
            try:
                relatorio = hook.retorna_relatorio(
                    id_relatorio=self.id_relatorio,
                    formato=formato,
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

        with open(self.caminho_arquivo, 'wb') as arquivo:
            arquivo.write(relatorio)

        self.log.info('Transferência realizada com sucesso')

        self.xcom_push(
            context, 'caminho', os.path.abspath(self.caminho_arquivo)
        )
        self.xcom_push(
            context, 'tamanho', humanize.naturalsize(len(relatorio))
        )
