from typing import Any, List, Tuple
import json
import os

from airflow.models.baseoperator import BaseOperator

from airflow.providers.tesouro_gerencial.hooks.tesouro_gerencial \
    import TesouroGerencialHook


class RelatorioParaArquivo(BaseOperator):
    '''Realiza o download de um relatório do Tesouro Gerencial para um
    arquivo local.

    :param id_conexao: ID de conta do SIAFI cadastrada no Airflow
    :type id_conexao: str
    :param id_relatorio: ID de relatório existente no Tesouro Gerencial
    :type id_relatorio:
    :param caminho_arquivo: caminho do arquivo onde relatório será salvo. Sua
    extensão determinará como o arquivo será gerado no Tesouro Gerencial, sendo
    possível configurar arquivos do tipo ".csv", ".xls", ".xlsx" ou ".pdf"
    :type caminho_arquivo: str
    :param respostas_prompts_valor: lista com respostas de prompts de valor,
    respeitando sua ordem conforme consta no relatório
    :type respostas_prompts_valor: List[str]
    :return: caminho absoluto de arquivo salvo e seu tamanho
    :rtype: Tuple[str, int]
    '''
    template_fields = [
        'id_relatorio', 'caminho_arquivo', 'respostas_prompts_valor'
    ]

    id_conexao: str
    id_relatorio: str
    caminho_arquivo: str
    respostas_prompts_valor: List[str]

    def __init__(
        self,
        id_conexao: str,
        id_relatorio: str,
        caminho_arquivo: str,
        respostas_prompts_valor: List[str] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.id_conexao = id_conexao
        self.id_relatorio = id_relatorio
        self.caminho_arquivo = caminho_arquivo
        self.respostas_prompts_valor = respostas_prompts_valor

    def execute(self, context: Any) -> Tuple[str, int]:
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

        with TesouroGerencialHook(self.id_conexao) as hook:
            relatorio = hook.retorna_relatorio(
                id_relatorio=self.id_relatorio,
                formato=formato,
                respostas_prompts_valor=respostas_prompts_valor
            )

            with open(self.caminho_arquivo, 'wb') as arquivo:
                arquivo.write(relatorio)
