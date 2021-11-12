from io import BytesIO
from urllib.parse import urljoin
import logging

from airflow.exceptions import AirflowException
from eb.airflow.providers.siafi.hooks.siafi import SIAFIHook
import pandas
import requests


logger = logging.getLogger(__name__)


class TesouroGerencialHook(SIAFIHook):
    '''Hook para interação com Tesouro Gerencial.'''
    URL = 'https://tesourogerencial.tesouro.gov.br/'

    string_sessao: str

    def __enter__(self) -> 'TesouroGerencialHook':
        '''Inicia sessão.'''
        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {
            'taskId': 'senhaMstrSSOTask',
            'taskEnv': 'xhr',
            'taskContentType': 'json',
            'cpf': self.cpf,
            'token': '',
            'server': '',
            'project': 'TESOURO%20GERENCIAL%20-%20DES',
            'senha': self.senha,
            'novaSenha': '',
        }

        resposta = requests.get(url, params=params, verify=False)
        resposta_json = resposta.json()
        self.string_sessao = resposta_json.get('sessionState')

        if self.string_sessao is None:
            raise AirflowException(
                'Erro ao iniciar sessão no Tesouro Gerencial. Retorno: '
                f'{resposta.text}'
            )

        return self

    def __exit__(self, *args, **kwargs) -> None:
        '''Encerra sessão.'''
        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {'taskId': 'logout', 'sessionState': self.string_sessao}
        requests.get(url, params=params, verify=False)

    def retorna_relatorio(self, id_relatorio: str) -> pandas.DataFrame:
        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {
            'taskId': 'exportReport',
            'taskEnv': 'juil_iframe',
            'taskContent': 'json',
            'sessionState': self.string_sessao,
            'executionMode': 4,
            'expandPageBy': True,
            'reportID': id_relatorio
        }
        resposta = requests.get(url, params=params, verify=False)
        print(resposta.content)

        with BytesIO() as arquivo:
            arquivo.write(resposta.content)
            arquivo.seek(0)

            return pandas.read_csv(arquivo)
