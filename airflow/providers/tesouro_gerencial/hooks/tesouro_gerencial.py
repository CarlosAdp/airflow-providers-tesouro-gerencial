from io import StringIO
from typing import Dict, List
from urllib.parse import urljoin
import logging
import re
import warnings

from airflow.exceptions import AirflowException
from airflow.providers.siafi.hooks.siafi import SIAFIHook
import pandas
import requests

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logger = logging.getLogger(__name__)


class TesouroGerencialHook(SIAFIHook):
    '''Hook para interação com Tesouro Gerencial.

    Classe herdada de :class:`airflow.providers.siafi.hooks.siafi.SIAFIHook`
    '''
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

    def retorna_relatorio(
        self,
        id_relatorio: str,
        respostas_prompts_valor: List[str] = None,
        respostas_prompts_selecao: Dict[str, List[str]] = None
    ) -> pandas.DataFrame:
        url = urljoin(self.URL, 'tg/servlet/taskAdmin')
        params = {
            'taskId': 'exportReport',
            'taskEnv': 'juil_iframe',
            'taskContent': 'json',
            'sessionState': self.string_sessao,
            'executionMode': 4,
            'expandPageBy': True,
            'reportID': id_relatorio,
        }
        if respostas_prompts_valor:
            params.update({
                'valuePromptAnswers': '^'.join(respostas_prompts_valor)
            })

        if respostas_prompts_selecao:
            params.update({
                'elementsPromptAnswers': ';'.join(
                    f'{atributo}:{valor}'
                    for atributo, valor in respostas_prompts_selecao.items()
                )
            })

        requisicao = requests.Request('GET', url, params=params)
        requisicao_preparada = requisicao.prepare()
        logger.info(url)

        resposta = requests.get(url, verify=False)
        with open('test.txt', 'wb') as fd:
            fd.write(resposta.content)
        conteudo = resposta.content.decode('utf-16')
        conteudo = re.sub(pattern=r'.*\r\n\r\n', repl='', string=conteudo)

        with StringIO() as arquivo:
            arquivo.write(conteudo)
            arquivo.seek(0)

            return pandas.read_csv(arquivo)


if __name__ == '__main__':
    with TesouroGerencialHook('teste') as hook:
        print(hook.retorna_relatorio(
            id_relatorio='1099EB5111EC4BCCC2FF0080EF2540E7',
            respostas_prompts_selecao={
                '262144037': '1048576'
            }
        ))
