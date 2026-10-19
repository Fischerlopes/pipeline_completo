# ============================================================
# GERADOR DE ORDENS DE PRODUÇÃO
# Neurometric — neuro-pipeline-dev
# ============================================================
# Depende de: orders_*.csv e products_*.csv
# ============================================================

import random
import glob
import pandas as pd
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora, gerar_id

fake = Faker('pt_BR')

QUANTIDADE    = 5500
PASTA_DESTINO = 'producao/order_prod'

STATUS_LIST = [
    'aguardando',
    'em_producao',
    'pausado',
    'finalizado',
    'cancelado',
]

LINHAS_PRODUCAO = [
    'Linha-A', 'Linha-B', 'Linha-C', 'Linha-D'
]

TURNOS = ['Manhã', 'Tarde', 'Noite']


def carregar_ids(entidade, coluna_id):
    padrao   = f'data_generator/temp/{entidade}_*.csv'
    arquivos = sorted(glob.glob(padrao))

    if not arquivos:
        raise FileNotFoundError(
            f'Nenhum arquivo encontrado para {entidade}. '
            f'Rode o gerador de {entidade} primeiro.'
        )

    df  = pd.read_csv(arquivos[-1])
    ids = df[coluna_id].tolist()
    print(f'  {len(ids)} IDs carregados de {arquivos[-1]}')
    return ids


def gerar_ordens(quantidade, ids_pedidos, ids_produtos):
    print(f'Gerando {quantidade} ordens de produção...')
    ordens = []

    for i in range(quantidade):
        id_ordem       = gerar_id('OP', i + 1)
        qtd_planejada  = random.randint(1, 800)
        qtd_produzida  = random.randint(0, qtd_planejada)
        custo_unitario = round(random.uniform(20, 4000), 2)
        custo_total    = round(qtd_produzida * custo_unitario, 2)

        data_inicio = fake.date_between(start_date='-1y', end_date='today')
        data_fim    = fake.date_between(
            start_date=data_inicio,
            end_date='+45d'
        )

        ordens.append({
            'id_ordem':          id_ordem,
            'id_pedido':         random.choice(ids_pedidos),
            'id_produto':        random.choice(ids_produtos),
            'linha_producao':    random.choice(LINHAS_PRODUCAO),
            'turno':             random.choice(TURNOS),
            'qtd_planejada':     qtd_planejada,
            'qtd_produzida':     qtd_produzida,
            'custo_unitario':    custo_unitario,
            'custo_total':       custo_total,
            'status':            random.choice(STATUS_LIST),
            'data_inicio_prev':  data_inicio.isoformat(),
            'data_fim_prev':     data_fim.isoformat(),
            'data_atualizacao':  timestamp_agora(),
            'data_carga':        data_hoje(),
        })

    return ordens


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE ORDENS DE PRODUÇÃO — Neurometric')
    print('=' * 50)

    print('Carregando IDs existentes...')
    ids_pedidos  = carregar_ids('orders',   'id_pedido')
    ids_produtos = carregar_ids('products', 'id_produto')

    dados         = gerar_ordens(QUANTIDADE, ids_pedidos, ids_produtos)
    caminho, nome = salvar_csv(dados, 'order_prod')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)