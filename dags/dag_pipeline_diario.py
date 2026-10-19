# ============================================================
# DAG: Pipeline Diário
# Neurometric — neuro-pipeline-dev
# ============================================================
# Executa todo dia automaticamente:
#   1. Gera dados novos com Python
#   2. Envia para o bucket GCP
#   3. Executa o dbt com Cosmos: uma tarefa por modelo
#      (run + testes), seguindo a linhagem do dbt
#   4. Em caso de falha, envia alerta por e-mail
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior
from datetime import datetime, timedelta
import subprocess
import sys
import os

# ── Configurações ────────────────────────────────────────────

# Caminho relativo à DAG: funciona no container (/opt/airflow/dbt/...)
# e no CI do GitHub, onde o repositório fica em outra pasta
DBT_PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'dbt', 'jornada_financeira')
)
DBT_PROFILES_DIR = DBT_PROJECT_DIR
SCRIPTS_DIR = '/opt/airflow/dags/scripts'

# Destinatário do alerta de falha (vazio = alerta só no log)
ALERT_EMAIL = os.getenv('ALERT_EMAIL')

default_args = {
    'owner': 'neurometric',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# ── Funções ──────────────────────────────────────────────────

def rodar_script(nome_script, **context):
    """
    Executa um script Python de geração de dados.
    Se falhar, levanta exceção para o Airflow marcar como erro.
    """
    caminho = f'/opt/airflow/dags/scripts/{nome_script}'
    resultado = subprocess.run(
        [sys.executable, caminho],
        capture_output=True,
        text=True,
        cwd='/opt/airflow'
    )

    print(resultado.stdout)

    if resultado.returncode != 0:
        print(resultado.stderr)
        raise Exception(
            f'Script {nome_script} falhou com código {resultado.returncode}:\n'
            f'{resultado.stderr}'
        )

    print(f'✅ {nome_script} concluído com sucesso')


def notificar_sucesso(**context):
    dag_run = context['dag_run']
    print(f'✅ Pipeline concluído com sucesso!')
    print(f'   DAG: {dag_run.dag_id}')
    print(f'   Execução: {dag_run.execution_date}')


def notificar_falha(**context):
    """
    Envia um único e-mail por execução com as tarefas que falharam
    e o link do log de cada uma.
    """
    dag_run = context['dag_run']
    tarefas = dag_run.get_task_instances()

    # 'failed' = onde o erro aconteceu; 'upstream_failed' = não rodou por causa dele
    falharam   = [ti for ti in tarefas if ti.state == 'failed']
    nao_rodaram = [ti.task_id for ti in tarefas if ti.state == 'upstream_failed']

    print(f'❌ Pipeline falhou!')
    print(f'   DAG: {dag_run.dag_id}')
    print(f'   Execução: {dag_run.run_id}')
    for ti in falharam:
        print(f'   Falhou: {ti.task_id} → {ti.log_url}')

    if not ALERT_EMAIL:
        print('ALERT_EMAIL não configurado — e-mail não enviado.')
        return

    linhas = ''.join(
        f'<tr><td><b>{ti.task_id}</b></td>'
        f'<td>{ti.try_number - 1}</td>'
        f'<td><a href="{ti.log_url}">ver log</a></td></tr>'
        for ti in falharam
    )

    html = f"""
    <h3>❌ Falha no pipeline <code>{dag_run.dag_id}</code></h3>
    <p><b>Execução:</b> {dag_run.run_id}<br>
       <b>Data lógica:</b> {dag_run.logical_date:%d/%m/%Y %H:%M} UTC</p>

    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th>Tarefa com falha</th><th>Tentativas</th><th>Log</th></tr>
      {linhas}
    </table>

    <p><b>Não executadas por causa da falha ({len(nao_rodaram)}):</b>
       {', '.join(nao_rodaram) or '—'}</p>
    """

    send_email(
        to=ALERT_EMAIL,
        subject=f'[Airflow] Falha em {dag_run.dag_id} — {dag_run.logical_date:%d/%m/%Y}',
        html_content=html,
    )
    print(f'📧 Alerta enviado para {ALERT_EMAIL}')


# ── DAG ──────────────────────────────────────────────────────

def gerador(nome):
    """Tarefa que roda o script dags/scripts/<nome>.py"""
    return PythonOperator(
        task_id=nome,
        python_callable=rodar_script,
        op_args=[f'{nome}.py'],
    )


with DAG(
    dag_id='neuro_pipeline_diario',
    description='Pipeline diário: geração de dados + dbt (Cosmos)',
    schedule='0 6 * * *',  # Todo dia às 6h da manhã
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=8,  # limita processos dbt simultâneos no container
    tags=['neurometric', 'pipeline', 'diario'],
) as dag:

    # ── Início ───────────────────────────────────────────────
    inicio = EmptyOperator(task_id='inicio')

    # ── Geração de dados, agrupada por domínio ───────────────
    with TaskGroup('vendas') as vendas:
        gerar_clientes = gerador('gerar_clientes')
        gerar_produtos = gerador('gerar_produtos')
        gerar_pedidos  = gerador('gerar_pedidos')

        # Pedidos apontam para clientes e produtos existentes
        [gerar_clientes, gerar_produtos] >> gerar_pedidos

    with TaskGroup('producao') as producao:
        gerar_linhas         = gerador('gerar_linhas_producao')
        gerar_ordem_producao = gerador('gerar_ordem_producao')
        gerar_insumos        = gerador('gerar_insumos')

        gerar_ordem_producao >> gerar_insumos

    with TaskGroup('logistica') as logistica:
        gerar_entregas    = gerador('gerar_entregas')
        gerar_fretes      = gerador('gerar_fretes')
        gerar_ocorrencias = gerador('gerar_ocorrencias')

        gerar_entregas >> [gerar_fretes, gerar_ocorrencias]

    # ── dbt: preparação ──────────────────────────────────────
    with TaskGroup('preparacao_dbt') as preparacao_dbt:
        # dbt_packages/ não vai para o Git: instala uma vez por execução
        # as versões do package-lock.yml (as tarefas do Cosmos reutilizam)
        dbt_deps = BashOperator(
            task_id='dbt_deps',
            bash_command=(
                f'cd {DBT_PROJECT_DIR} && '
                f'dbt deps --profiles-dir {DBT_PROFILES_DIR}'
            ),
        )

        # Registra os CSVs do bucket como tabelas externas no BigQuery
        stage_external_sources = BashOperator(
            task_id='stage_external_sources',
            bash_command=(
                f'cd {DBT_PROJECT_DIR} && '
                f'dbt run-operation stage_external_sources --profiles-dir {DBT_PROFILES_DIR}'
            ),
        )

        dbt_deps >> stage_external_sources

    # ── dbt: transformação com Cosmos ────────────────────────
    # Cada modelo vira um grupo com run + testes. A ordem segue a
    # linhagem do dbt (ref), então stg_customers pode rodar assim
    # que raw_customers termina, sem esperar a camada inteira.
    # Uma falha afeta só o modelo e o que depende dele.
    transformacao_dbt = DbtTaskGroup(
        group_id='transformacao_dbt',
        project_config=ProjectConfig(
            DBT_PROJECT_DIR,
            install_dbt_deps=False,  # já instalado por dbt_deps
        ),
        profile_config=ProfileConfig(
            profile_name='jornada_financeira',
            target_name=os.getenv('DBT_TARGET', 'dev'),
            profiles_yml_filepath=f'{DBT_PROFILES_DIR}/profiles.yml',
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            # Testes que olham dois modelos (ex.: relationships) viram
            # tarefa própria e só rodam depois dos dois
            should_detach_multiple_parents_tests=True,
        ),
    )

    # ── Notificações ─────────────────────────────────────────
    notificar_ok = PythonOperator(
        task_id='notificar_sucesso',
        python_callable=notificar_sucesso,
    )

    notificar_erro = PythonOperator(
        task_id='notificar_falha',
        python_callable=notificar_falha,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Sem tarefa 'fim' com ALL_DONE: o Airflow define o status do run
    # pelas últimas tarefas, e uma 'fim' sempre verde escondia as falhas.
    # Assim, em caso de erro, notificar_sucesso fica upstream_failed
    # e o run aparece como failed.

    # ── Fluxo ────────────────────────────────────────────────

    # Início → os três domínios (cada tarefa ainda espera suas
    # dependências: linhas de produção não dependem de nada e
    # começam já, em paralelo com clientes e produtos)
    inicio >> [vendas, producao, logistica]

    # Ordens de produção e entregas nascem dos pedidos
    gerar_pedidos >> [gerar_ordem_producao, gerar_entregas]

    # Todos os dados prontos → dbt
    [vendas, producao, logistica] >> preparacao_dbt >> transformacao_dbt

    # Notificações
    transformacao_dbt >> [notificar_ok, notificar_erro]
