# ============================================================
# UTILITÁRIOS COMPARTILHADOS
# Neurometric — neuro-pipeline-dev
# ============================================================
# Este arquivo centraliza funções usadas por todos os
# geradores de dados. Qualquer alteração aqui reflete
# automaticamente em todos os scripts.
# ============================================================

import pandas as pd
from google.cloud import storage
from datetime import datetime
import os

# ── Configurações globais ────────────────────────────────────
GCP_PROJECT        = 'neuro-pipeline-dev'
BUCKET_NAME        = 'neuro-pipeline-dev-raw'
CAMINHO_CREDENCIAL = 'secrets/neuro-pipeline-dev.json'
PASTA_TEMP         = os.path.join('data_generator', 'temp')


# ── Funções compartilhadas ───────────────────────────────────

def get_cliente_gcs():
    """
    Retorna um cliente autenticado do Google Cloud Storage.
    Centralizado aqui para não repetir em cada script.
    """
    return storage.Client.from_service_account_json(
        CAMINHO_CREDENCIAL,
        project=GCP_PROJECT
    )


def salvar_csv(dados, nome_entidade):
    """
    Salva uma lista de dicionários em CSV na pasta temp local.

    Parâmetros:
        dados          → lista de dicionários gerados pelo script
        nome_entidade  → nome do arquivo sem data (ex: 'customers')

    Retorna:
        caminho_local  → caminho completo do arquivo salvo
        nome_arquivo   → nome do arquivo com data (ex: 'customers_2026-09-01.csv')
    """
    hoje         = datetime.now().strftime('%Y-%m-%d')
    nome_arquivo = f'{nome_entidade}_{hoje}.csv'
    caminho      = os.path.join(PASTA_TEMP, nome_arquivo)

    os.makedirs(PASTA_TEMP, exist_ok=True)

    df = pd.DataFrame(dados)
    df.to_csv(caminho, index=False, encoding='utf-8')

    print(f'Arquivo salvo localmente: {caminho}')
    print(f'Total de registros: {len(df)}')

    return caminho, nome_arquivo


def enviar_para_gcp(caminho_local, nome_arquivo, pasta_destino):
    """
    Envia um arquivo CSV local para o bucket no GCP.

    Parâmetros:
        caminho_local  → caminho do arquivo na máquina
        nome_arquivo   → nome do arquivo CSV
        pasta_destino  → pasta dentro do bucket (ex: 'vendas/customers')
    """
    print('Enviando para o GCP...')

    cliente_gcs = get_cliente_gcs()
    bucket      = cliente_gcs.bucket(BUCKET_NAME)
    destino     = f'{pasta_destino}/{nome_arquivo}'
    blob        = bucket.blob(destino)

    blob.upload_from_filename(caminho_local)

    print(f'Arquivo enviado para: gs://{BUCKET_NAME}/{destino}')


def data_hoje():
    """Retorna a data atual no formato ISO (YYYY-MM-DD)."""
    return datetime.now().date().isoformat()


def timestamp_agora():
    """Retorna o timestamp atual no formato ISO."""
    return datetime.now().isoformat()


def gerar_id(prefixo, numero):
    """
    Gera um ID único entre execuções, incluindo a data da carga.
    Ex: gerar_id('PED', 1) → 'PED-20261018-000001'

    Usado nas entidades transacionais (pedidos, ordens, entregas...),
    que são novas a cada dia. Cadastros (clientes, produtos, linhas)
    mantêm IDs fixos, pois representam o mesmo registro atualizado.
    """
    data = data_hoje().replace('-', '')
    return f'{prefixo}-{data}-{str(numero).zfill(6)}'