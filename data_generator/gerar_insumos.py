# ============================================================
# GERADOR DE INSUMOS DE PRODUÇÃO
# Neurometric — neuro-pipeline-dev
# ============================================================
# Depende de: order_prod_*.csv
# ============================================================

import random
import glob
import pandas as pd
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora, gerar_id

fake = Faker('pt_BR')

PASTA_DESTINO = 'producao/production_inputs'

INSUMOS = [
    'Aço',  'Alumínio', 'Plástico ABS', 'Madeira MDF',
    'Cobre', 'Borracha', 'Vidro', 'Tecido', 'Tinta',
    'Parafuso', 'Cola Industrial', 'Espuma',
]

FORNECEDORES = [
    'MetalBras', 'PlastiCo', 'MadeiraFlex',
    'CopperMax', 'BorrachaTec', 'VidroClaro',
]


def carregar_ids_ordens():
    padrao   = 'data_generator/temp/order_prod_*.csv'
    arquivos = sorted(glob.glob(padrao))

    if not arquivos:
        raise FileNotFoundError(
            'Nenhum arquivo de ordens encontrado. '
            'Rode o gerar_ordem_producao.py primeiro.'
        )

    df  = pd.read_csv(arquivos[-1])
    ids = df['id_ordem'].tolist()
    print(f'  {len(ids)} ordens carregadas de {arquivos[-1]}')
    return ids


def gerar_insumos(ids_ordens):
    print('Gerando insumos de produção...')
    insumos = []
    contador = 1

    # Cada ordem usa entre 1 e 4 insumos diferentes
    for id_ordem in ids_ordens:
        qtd_insumos = random.randint(1, 5)
        for _ in range(qtd_insumos):
            id_insumo      = gerar_id('INS', contador)
            qtd_utilizada  = round(random.uniform(0.5, 200), 2)
            custo_unitario = round(random.uniform(2, 700), 2)

            insumos.append({
                'id_insumo':       id_insumo,
                'id_ordem':        id_ordem,
                'nome_insumo':     random.choice(INSUMOS),
                'fornecedor':      random.choice(FORNECEDORES),
                'unidade':         random.choice(['KG', 'MT', 'UN', 'LT']),
                'qtd_planejada':   round(qtd_utilizada * 1.1, 2),
                'qtd_utilizada':   qtd_utilizada,
                'custo_unitario':  custo_unitario,
                'custo_total':     round(qtd_utilizada * custo_unitario, 2),
                'data_atualizacao': timestamp_agora(),
                'data_carga':      data_hoje(),
            })
            contador += 1

    return insumos


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE INSUMOS — Neurometric')
    print('=' * 50)

    ids_ordens    = carregar_ids_ordens()
    dados         = gerar_insumos(ids_ordens)
    caminho, nome = salvar_csv(dados, 'production_inputs')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)