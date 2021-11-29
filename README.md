# airflow-providers-tesouro-gerencial

Provider do Airflow para comunicação com [Tesouro Gerencial](https://tesourogerencial.tesouro.gov.br/).


## Instalação

```shell
pip install airflow-providers-tesouro-gerencial
```

## Conteúdo

- Hook para conexão com Tesouro Gerencial, que contém métodos para:
    - Entrada e saída de contexto (clásula `with`), inicializando e encerrando sessão no Tesouro Gerencial
    - Execução e exportação de relatório

- Transfers que carregam relatórios do Tesouro Gerencial para:
    - Arquivo local;
    - Banco MongoDB

## Exemplo de Uso

Transferência de relatório para arquivo local:

```python
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.tesouro_gerencial.transfers.relatorio_para_arquivo import RelatorioParaArquivo


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1))
def teste_tesouro_gerencial():
    teste = RelatorioParaArquivo(
        task_id='teste1',
        id_conta_siafi='teste',
        id_relatorio='970A89D511EC923631090080EFC5BFD1',
        caminho_arquivo='/tmp/tg.xlsx',
        respostas_prompts_valor=['622110000', '622120000'],
        retries=10,
        retry_delay=timedelta(minutes=2)
    )

minha_dag = teste_tesouro_gerencial()
```

Transferência para banco MongoDB

```python
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.tesouro_gerencial.transfers.relatorio_para_mongo import RelatorioParaMongo


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1))
def teste_tesouro_gerencial():
    teste = RelatorioParaMongo(
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


minha_dag = teste_tesouro_gerencial()
```
