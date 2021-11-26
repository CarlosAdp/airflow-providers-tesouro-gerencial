from enum import Enum
from typing import List, Union
from urllib.parse import urljoin
import warnings

from airflow.exceptions import AirflowException
from airflow.providers.siafi.hooks.siafi import SIAFIHook
import requests

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


class TesouroGerencialHook(SIAFIHook):
    '''Hook para interação com Tesouro Gerencial.

    Classe herdada de :class:`airflow.providers.siafi.hooks.siafi.SIAFIHook`
    '''
    class FORMATO(Enum):
        PDF = 'pdf'
        CSV = 'csv'
        EXCEL = 'excel'

        def __str__(self) -> str:
            return self.value

    URL = 'https://tesourogerencial.tesouro.gov.br/'

    string_sessao: str

    def __enter__(self) -> 'TesouroGerencialHook':
        '''Inicia sessão.'''
        super().__enter__()

        cpf = self.cpf
        senha = self.senha

        self.log.info('Iniciando sessão com usuário "%s"', self.cpf)

        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {
            'taskId': 'senhaMstrSSOTask',
            'taskEnv': 'xhr',
            'taskContentType': 'json',
            'cpf': cpf,
            'token': '',
            'server': '',
            'project': 'TESOURO%20GERENCIAL%20-%20DES',
            'senha': senha,
            'novaSenha': '',
        }

        resposta = requests.get(url, params=params, verify=False)

        try:
            resposta_json = resposta.json()
            self.string_sessao = resposta_json['sessionState']
        except Exception:
            raise AirflowException(resposta)

        self.log.info('Sessão iniciado com sucesso')
        return self

    def __exit__(self, *args, **kwargs) -> None:
        '''Encerra sessão.'''
        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {'taskId': 'logout', 'sessionState': self.string_sessao}
        requests.get(url, params=params, verify=False)

        self.log.info('Sessão encerrada com sucesso')

    def retorna_relatorio(
        self,
        id_relatorio: str,
        formato: Union[str, FORMATO] = FORMATO.CSV,
        respostas_prompts_valor: List[str] = None,
    ) -> bytes:
        '''Retorna um relatório do Tesouro Gerencial.

        :param id_relatorio: ID do relatório
        :type id_relatorio: str
        :param formato: formato do relatório a ser buscado no Tesouro
        Gerencial, podendo ser "csv", "excel" ou "pdf". O atributo
        :attr:`~TesouroGerencialHook.FORMATO` também pode ser utilizado.
        :type formato: Union[str, TesouroGerencialHook.FORMATO]
        :param respostas_prompts_valor: lista com respostas de prompts de
        valor, respeitando sua ordem conforme consta no relatório
        :type respostas_prompts_valor: List[str]
        :return: conteúdo do relatório, em cadeia de caracteres binários
        :rtype: bytes
        '''
        self.log.info(
            'Solicitando relatório "%s" no formato "%s" com as seguintes '
            'respostas para prompts: "%s"',
            id_relatorio, formato, respostas_prompts_valor
        )

        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {
            'taskId': 'exportReport',
            'taskEnv': 'juil_iframe',
            'taskContent': 'json',
            'expandPageBy': True,
        }

        params.update({
            'sessionState': self.string_sessao,
            'reportID': id_relatorio,
            'valuePromptAnswers': '^'.join(respostas_prompts_valor or [])
        })

        try:
            formato = self.FORMATO(formato)
        except ValueError:
            raise AirflowException(f'"{formato}" não é um formato válido')

        if formato == self.FORMATO.CSV:
            params.update({'executionMode': 4, 'plainTextDelimiter': ','})
        elif formato == self.FORMATO.EXCEL:
            params.update({'executionMode': 3, 'excelVersion': 4})
        elif formato == self.FORMATO.PDF:
            params.update({'executionMode': 2})

        requisicao = requests.Request('GET', url, params=params)
        requisicao_preparada = requisicao.prepare()
        self.log.info('Solicitando URL "%s"', requisicao_preparada.url)

        resposta = requests.get(requisicao_preparada.url, verify=False)

        if resposta.ok:
            self.log.info('Relatório gerado com sucesso')
            return resposta.content
        else:
            raise AirflowException(resposta)
