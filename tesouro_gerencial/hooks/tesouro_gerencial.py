from urllib.parse import urljoin

from airflow.exceptions import AirflowException
from siafi.hooks.siafi import SIAFIHook
import requests


class TesouroGerencialHook(SIAFIHook):
    '''Hook para interação com Tesouro Gerencial.'''
    URL = 'https://tesourogerencial.tesouro.gov.br/'

    def inicia_sessao(self) -> str:
        '''Solicita início de sessão e retorna identificador.

        :return: string de sessão :rtype: str
        '''
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
        sessao = resposta_json.get('sessionState')

        if sessao is None:
            raise AirflowException(
                'Erro ao iniciar sessão no Tesouro Gerencial. Retorno: '
                f'{resposta.text}'
            )

        return sessao
