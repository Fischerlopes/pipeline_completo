# ============================================================
# GERADOR DE ENTREGAS
# Neurometric — neuro-pipeline-dev
# ============================================================
# Depende de: orders_*.csv
# ============================================================

import random
import glob
import pandas as pd
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora, gerar_id

fake = Faker('pt_BR')

QUANTIDADE    = 3800
PASTA_DESTINO = 'logistica/deliveries'

STATUS_LIST = [
    'aguardando_coleta',
    'coletado',
    'em_transito',
    'saiu_para_entrega',
    'entregue',
    'tentativa_falha',
    'devolvido',
]

TRANSPORTADORAS = [
    'Correios',
    'Jadlog',
    'Sequoia',
    'Total Express',
    'Braspress',
]


def carregar_dados_pedidos():
    """
    Carrega pedidos com cliente e data — precisamos disso
    para gerar entregas consistentes com os pedidos.
    """
    padrao   = 'data_generator/temp/orders_*.csv'
    arquivos = sorted(glob.glob(padrao))

    if not arquivos:
        raise FileNotFoundError(
            'Nenhum arquivo de pedidos encontrado. '
            'Rode o gerar_pedidos.py primeiro.'
        )

    df = pd.read_csv(arquivos[-1])
    print(f'  {len(df)} pedidos carregados de {arquivos[-1]}')
    return df


def gerar_entregas(quantidade, df_pedidos):
    print(f'Gerando {quantidade} entregas...')
    entregas = []

    # Seleciona pedidos aleatórios para ter entrega
    pedidos_amostra = df_pedidos.sample(
        n=min(quantidade, len(df_pedidos)),
        replace=True
    )

    for i, (_, pedido) in enumerate(pedidos_amostra.iterrows()):
        id_entrega = gerar_id('ENT', i + 1)
        status     = random.choice(STATUS_LIST)

        data_coleta   = fake.date_between(start_date='-1y', end_date='today')
        data_prevista = fake.date_between(
            start_date=data_coleta,
            end_date='+25d'
        )
        data_entrega  = (
            fake.date_between(start_date=data_coleta, end_date='+30d').isoformat()
            if status == 'entregue'
            else None
        )

        entregas.append({
            'id_entrega':        id_entrega,
            'id_pedido':         pedido['id_pedido'],
            'id_cliente':        pedido['id_cliente'],
            'transportadora':    random.choice(TRANSPORTADORAS),
            'codigo_rastreio':   fake.bothify(text='??########BR').upper(),
            'status':            status,
            'data_coleta_prev':  data_coleta.isoformat(),
            'data_entrega_prev': data_prevista.isoformat(),
            'data_entrega_real': data_entrega,
            'tentativas':        random.randint(1, 3),
            'data_atualizacao':  timestamp_agora(),
            'data_carga':        data_hoje(),
        })

    return entregas


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE ENTREGAS — Neurometric')
    print('=' * 50)

    print('Carregando pedidos...')
    df_pedidos    = carregar_dados_pedidos()

    dados         = gerar_entregas(QUANTIDADE, df_pedidos)
    caminho, nome = salvar_csv(dados, 'deliveries')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)