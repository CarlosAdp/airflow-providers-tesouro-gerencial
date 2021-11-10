from urllib.parse import urljoin
import logging

from airflow.exceptions import AirflowException
from siafi.hooks.siafi import SIAFIHook
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
