# ============================================================
# DAG: Pipeline Diário
# Neurometric — neuro-pipeline-dev
# ============================================================
# Executa todo dia automaticamente:
#   1. Gera dados novos com Python
#   2. Envia para o bucket GCP
#   3. Executa o dbt (raw → staging → marts)
#   4. Em caso de falha, envia alerta por e-mail
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import subprocess
import sys
import os

# ── Configurações ────────────────────────────────────────────

DBT_PROJECT_DIR = '/opt/airflow/dbt/jornada_financeira'
DBT_PROFILES_DIR = '/opt/airflow/dbt/jornada_financeira'
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

with DAG(
    dag_id='neuro_pipeline_diario',
    description='Pipeline diário: geração de dados + dbt',
    schedule='0 6 * * *',  # Todo dia às 6h da manhã
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['neurometric', 'pipeline', 'diario'],
) as dag:

    # ── Início ───────────────────────────────────────────────
    inicio = EmptyOperator(task_id='inicio')

    # ── Geração de dados (em paralelo) ───────────────────────
    gerar_clientes = PythonOperator(
        task_id='gerar_clientes',
        python_callable=rodar_script,
        op_args=['gerar_clientes.py'],
    )

    gerar_produtos = PythonOperator(
        task_id='gerar_produtos',
        python_callable=rodar_script,
        op_args=['gerar_produtos.py'],
    )

    # ── Aguarda clientes e produtos antes dos dependentes ────
    aguardar_base = EmptyOperator(task_id='aguardar_base')

    gerar_pedidos = PythonOperator(
        task_id='gerar_pedidos',
        python_callable=rodar_script,
        op_args=['gerar_pedidos.py'],
    )

    gerar_ordem_producao = PythonOperator(
        task_id='gerar_ordem_producao',
        python_callable=rodar_script,
        op_args=['gerar_ordem_producao.py'],
    )

    gerar_entregas = PythonOperator(
        task_id='gerar_entregas',
        python_callable=rodar_script,
        op_args=['gerar_entregas.py'],
    )

    # ── Aguarda pedidos antes dos dependentes ────────────────
    aguardar_pedidos = EmptyOperator(task_id='aguardar_pedidos')

    gerar_fretes = PythonOperator(
        task_id='gerar_fretes',
        python_callable=rodar_script,
        op_args=['gerar_fretes.py'],
    )

    gerar_insumos = PythonOperator(
        task_id='gerar_insumos',
        python_callable=rodar_script,
        op_args=['gerar_insumos.py'],
    )

    gerar_linhas = PythonOperator(
        task_id='gerar_linhas_producao',
        python_callable=rodar_script,
        op_args=['gerar_linhas_producao.py'],
    )

    gerar_ocorrencias = PythonOperator(
        task_id='gerar_ocorrencias',
        python_callable=rodar_script,
        op_args=['gerar_ocorrencias.py'],
    )

    # ── Aguarda todos os dados antes do dbt ──────────────────
    aguardar_dados = EmptyOperator(task_id='aguardar_todos_os_dados')

    # ── dbt: pacotes ─────────────────────────────────────────
    # dbt_packages/ não vai para o Git: instala as versões do
    # package-lock.yml, necessário num clone novo do projeto
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=(
            'cd /opt/airflow/dbt/jornada_financeira && '
            'dbt deps '
            '--profiles-dir /opt/airflow/dbt/jornada_financeira'
        ),
    )

    # ── dbt: camada RAW ──────────────────────────────────────
    dbt_raw = BashOperator(
        task_id='dbt_raw',
        bash_command=(
            'cd /opt/airflow/dbt/jornada_financeira && '
            'dbt run-operation stage_external_sources '
            '--profiles-dir /opt/airflow/dbt/jornada_financeira && '
            'dbt run --select 01_raw '
            '--profiles-dir /opt/airflow/dbt/jornada_financeira'
        ),
    )

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command=(
            'cd /opt/airflow/dbt/jornada_financeira && '
            'dbt run --select 02_staging '
            '--profiles-dir /opt/airflow/dbt/jornada_financeira'
        ),
    )

    dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command=(
            'cd /opt/airflow/dbt/jornada_financeira && '
            'dbt run --select 03_marts '
            '--profiles-dir /opt/airflow/dbt/jornada_financeira'
        ),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd /opt/airflow/dbt/jornada_financeira && '
            'dbt test '
            '--profiles-dir /opt/airflow/dbt/jornada_financeira'
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

    # Início → geração base em paralelo
    inicio >> [gerar_clientes, gerar_produtos]

    # Base pronta → aguardar sincronização
    [gerar_clientes, gerar_produtos] >> aguardar_base

    # Base → dependentes em paralelo
    aguardar_base >> [gerar_pedidos, gerar_linhas]

    # Pedidos prontos → aguardar
    gerar_pedidos >> aguardar_pedidos

    # Pedidos → dependentes em paralelo
    aguardar_pedidos >> [gerar_ordem_producao, gerar_entregas]

    # Ordens → insumos
    gerar_ordem_producao >> gerar_insumos

    # Entregas → fretes e ocorrências
    gerar_entregas >> [gerar_fretes, gerar_ocorrencias]

    # Todos os dados prontos → dbt em sequência
    [
        gerar_insumos,
        gerar_fretes,
        gerar_ocorrencias,
        gerar_linhas,
    ] >> aguardar_dados

    aguardar_dados >> dbt_deps >> dbt_raw >> dbt_staging >> dbt_marts >> dbt_test

    # Notificações
    dbt_test >> notificar_ok
    dbt_test >> notificar_erro