# ============================================================
# TESTES DA DAG: neuro_pipeline_diario
# Neurometric — neuro-pipeline-dev
# ============================================================
# Validam a estrutura da DAG sem executar nada:
#   - carrega sem erros de import
#   - geradores rodam antes do dbt
#   - dbt roda na ordem certa
#   - uma falha nunca fica escondida atrás de um run verde
#
# Uso: pytest tests/
# ============================================================

import os
import pytest
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule

DAGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'dags')
DAG_ID   = 'neuro_pipeline_diario'

GERADORES = [
    'gerar_clientes',
    'gerar_produtos',
    'gerar_pedidos',
    'gerar_linhas_producao',
    'gerar_ordem_producao',
    'gerar_insumos',
    'gerar_entregas',
    'gerar_fretes',
    'gerar_ocorrencias',
]

ORDEM_DBT = ['dbt_deps', 'dbt_raw', 'dbt_staging', 'dbt_marts', 'dbt_test']


@pytest.fixture(scope='session')
def dag():
    dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert dagbag.import_errors == {}, f'Erros de import: {dagbag.import_errors}'
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None, f'DAG {DAG_ID} não encontrada'
    return dag


def test_agenda_diaria_sem_catchup(dag):
    assert dag.schedule_interval == '0 6 * * *'
    assert dag.catchup is False


def test_todos_os_geradores_existem(dag):
    for task_id in GERADORES:
        assert dag.has_task(task_id), f'Tarefa {task_id} não existe'


def test_geradores_rodam_antes_do_dbt(dag):
    antes_do_dbt = dag.get_task('dbt_deps').get_flat_relative_ids(upstream=True)
    for task_id in GERADORES:
        assert task_id in antes_do_dbt, f'{task_id} não roda antes do dbt'


def test_dbt_roda_na_ordem(dag):
    for atual, proxima in zip(ORDEM_DBT, ORDEM_DBT[1:]):
        assert proxima in dag.get_task(atual).downstream_task_ids, \
            f'{proxima} deveria vir logo depois de {atual}'


def test_falha_nao_fica_mascarada(dag):
    # O status do run vem das últimas tarefas: se houvesse uma tarefa
    # final "sempre verde" (ALL_DONE), uma falha apareceria como sucesso
    assert {t.task_id for t in dag.leaves} == {'notificar_sucesso', 'notificar_falha'}
    assert dag.get_task('notificar_falha').trigger_rule == TriggerRule.ONE_FAILED
    assert dag.get_task('notificar_sucesso').trigger_rule == TriggerRule.ALL_SUCCESS


def test_todas_as_tarefas_tem_retry(dag):
    for task in dag.tasks:
        assert task.retries >= 1, f'{task.task_id} sem retry'
