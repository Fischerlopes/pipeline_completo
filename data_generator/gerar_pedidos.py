# ============================================================
# GERADOR DE PEDIDOS DE VENDA
# Neurometric — neuro-pipeline-dev
# ============================================================
# Depende de: customers_*.csv e products_*.csv
# ============================================================

import random
import glob
import pandas as pd
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora, gerar_id

fake = Faker('pt_BR')

QUANTIDADE    = 4000
PASTA_DESTINO = 'vendas/orders'

STATUS_LIST = [
    'rascunho',
    'confirmado',
    'em_producao',
    'faturado',
    'entregue',
    'cancelado',
]

CANAIS = ['Representante', 'E-commerce', 'Televendas', 'Loja física', 'Marketplace']


def carregar_ids(entidade, coluna_id):
    """
    Lê o CSV mais recente de uma entidade e retorna lista de IDs.
    Assim os pedidos apontam para clientes e produtos reais.
    """
    padrao  = f'data_generator/temp/{entidade}_*.csv'
    arquivos = sorted(glob.glob(padrao))

    if not arquivos:
        raise FileNotFoundError(
            f'Nenhum arquivo encontrado para {entidade}. '
            f'Rode o gerador de {entidade} primeiro.'
        )

    arquivo_mais_recente = arquivos[-1]
    df = pd.read_csv(arquivo_mais_recente)
    ids = df[coluna_id].tolist()
    print(f'  {len(ids)} IDs carregados de {arquivo_mais_recente}')
    return ids


def gerar_pedidos(quantidade, ids_clientes, ids_produtos):
    print(f'Gerando {quantidade} pedidos...')
    pedidos = []

    for i in range(quantidade):
        id_pedido  = gerar_id('PED', i + 1)
        qtd_itens  = random.randint(1, 15)
        preco_unit = round(random.uniform(50, 20000), 2)
        desconto   = round(random.uniform(0, 0.20), 4)
        valor_bruto = round(qtd_itens * preco_unit, 2)
        valor_desconto = round(valor_bruto * desconto, 2)
        valor_liquido  = round(valor_bruto - valor_desconto, 2)

        data_pedido   = fake.date_between(start_date='-1y', end_date='today')
        data_entrega  = fake.date_between(
            start_date=data_pedido,
            end_date='+60d'
        )

        pedidos.append({
            'id_pedido':        id_pedido,
            'id_cliente':       random.choice(ids_clientes),
            'id_produto':       random.choice(ids_produtos),
            'canal_venda':      random.choice(CANAIS),
            'quantidade':       qtd_itens,
            'preco_unitario':   preco_unit,
            'desconto_pct':     desconto,
            'valor_bruto':      valor_bruto,
            'valor_desconto':   valor_desconto,
            'valor_liquido':    valor_liquido,
            'status':           random.choice(STATUS_LIST),
            'data_pedido':      data_pedido.isoformat(),
            'data_entrega_prev':data_entrega.isoformat(),
            'data_atualizacao': timestamp_agora(),
            'data_carga':       data_hoje(),
        })

    return pedidos


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE PEDIDOS — Neurometric')
    print('=' * 50)

    print('Carregando IDs existentes...')
    ids_clientes = carregar_ids('customers', 'id_cliente')
    ids_produtos = carregar_ids('products',  'id_produto')

    dados         = gerar_pedidos(QUANTIDADE, ids_clientes, ids_produtos)
    caminho, nome = salvar_csv(dados, 'orders')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)