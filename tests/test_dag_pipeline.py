# ============================================================
# TESTES DA DAG: neuro_pipeline_diario
# Neurometric — neuro-pipeline-dev
# ============================================================
# Validam a estrutura da DAG sem executar nada:
#   - carrega sem erros de import
#   - geradores agrupados por domínio e rodando antes do dbt
#   - cada modelo dbt virou uma tarefa (Cosmos), seguindo a linhagem
#   - uma falha nunca fica escondida atrás de um run verde
#
# Uso: pytest tests/
# ============================================================

import glob
import os
import pytest
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule

RAIZ     = os.path.join(os.path.dirname(__file__), '..')
DAGS_DIR = os.path.join(RAIZ, 'dags')
DAG_ID   = 'neuro_pipeline_diario'

GERADORES = {
    'vendas':    ['gerar_clientes', 'gerar_produtos', 'gerar_pedidos'],
    'producao':  ['gerar_linhas_producao', 'gerar_ordem_producao', 'gerar_insumos'],
    'logistica': ['gerar_entregas', 'gerar_fretes', 'gerar_ocorrencias'],
}

# Modelos lidos direto do projeto dbt: um modelo novo já entra no teste
MODELOS = sorted(
    os.path.splitext(os.path.basename(caminho))[0]
    for caminho in glob.glob(os.path.join(RAIZ, 'dbt', 'jornada_financeira', 'models', '*', '*.sql'))
)


@pytest.fixture(scope='session')
def dag():
    dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert dagbag.import_errors == {}, f'Erros de import: {dagbag.import_errors}'
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None, f'DAG {DAG_ID} não encontrada'
    return dag


def tarefa_run(dag, modelo):
    """Tarefa de run do Cosmos: 'modelo.run' (com testes) ou 'modelo_run' (sem testes)."""
    for task_id in (f'transformacao_dbt.{modelo}.run', f'transformacao_dbt.{modelo}_run'):
        if dag.has_task(task_id):
            return dag.get_task(task_id)
    return None


def test_agenda_diaria_sem_catchup(dag):
    assert dag.schedule_interval == '0 6 * * *'
    assert dag.catchup is False


def test_geradores_agrupados_por_dominio(dag):
    for dominio, geradores in GERADORES.items():
        for gerador in geradores:
            assert dag.has_task(f'{dominio}.{gerador}'), f'{dominio}.{gerador} não existe'


def test_geradores_rodam_antes_do_dbt(dag):
    antes_do_dbt = dag.get_task('preparacao_dbt.dbt_deps').get_flat_relative_ids(upstream=True)
    for dominio, geradores in GERADORES.items():
        for gerador in geradores:
            assert f'{dominio}.{gerador}' in antes_do_dbt, f'{gerador} não roda antes do dbt'


def test_projeto_dbt_tem_modelos():
    # Garante que o glob acima achou os modelos (senão os testes abaixo passariam vazios)
    assert len(MODELOS) >= 20


def test_cada_modelo_dbt_virou_tarefa(dag):
    for modelo in MODELOS:
        assert tarefa_run(dag, modelo) is not None, f'Modelo {modelo} sem tarefa no Cosmos'


def test_modelos_rodam_depois_das_sources_externas(dag):
    for modelo in MODELOS:
        antes = tarefa_run(dag, modelo).get_flat_relative_ids(upstream=True)
        assert 'preparacao_dbt.stage_external_sources' in antes, \
            f'{modelo} pode rodar antes das tabelas externas existirem'


def test_linhagem_raw_para_staging(dag):
    # Cada stg_<x> só roda depois do raw_<x> correspondente
    for modelo in MODELOS:
        if modelo.startswith('stg_'):
            raw = tarefa_run(dag, modelo.replace('stg_', 'raw_', 1))
            antes = tarefa_run(dag, modelo).get_flat_relative_ids(upstream=True)
            assert raw.task_id in antes, f'{modelo} não depende de {raw.task_id}'


def test_falha_nao_fica_mascarada(dag):
    # O status do run vem das últimas tarefas: se houvesse uma tarefa
    # final "sempre verde" (ALL_DONE), uma falha apareceria como sucesso
    assert {t.task_id for t in dag.leaves} == {'notificar_sucesso', 'notificar_falha'}
    assert dag.get_task('notificar_falha').trigger_rule == TriggerRule.ONE_FAILED
    assert dag.get_task('notificar_sucesso').trigger_rule == TriggerRule.ALL_SUCCESS


def test_todas_as_tarefas_tem_retry(dag):
    for task in dag.tasks:
        assert task.retries >= 1, f'{task.task_id} sem retry'
