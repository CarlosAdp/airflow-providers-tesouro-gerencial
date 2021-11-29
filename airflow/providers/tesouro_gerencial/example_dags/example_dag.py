from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.tesouro_gerencial.transfers.relatorio_para_arquivo \
    import RelatorioParaArquivo
from airflow.providers.tesouro_gerencial.transfers.relatorio_para_mongo \
    import RelatorioParaMongo


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1))
def teste_tesouro_gerencial():
    teste1 = RelatorioParaArquivo(
        task_id='teste1',
        id_conta_siafi='teste',
        id_relatorio='970D89D511EC423631090080EFA5BFD1',
        caminho_arquivo='/tmp/tg.xlsx',
        respostas_prompts_valor=['622110000', '622120000'],
        retries=10,
        retry_delay=timedelta(minutes=2)
    )

    teste2 = RelatorioParaMongo(
        task_id='teste2',
        id_conta_siafi='teste',
        id_relatorio='970D89D511EC423631090080EFA5BFD1',
        id_conexao_mongo='teste_mongo',
        nome_colecao='teste',
        respostas_prompts_valor=['622110000', '622120000'],
        truncar_colecao=True,
        retries=10,
        retry_delay=timedelta(minutes=2)
    )

    teste1
    teste2


dag = teste_tesouro_gerencial()
