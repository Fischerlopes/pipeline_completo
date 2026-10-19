# ============================================================
# GERADOR DE PRODUTOS
# Neurometric — neuro-pipeline-dev
# ============================================================

import random
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora

fake = Faker('pt_BR')

QUANTIDADE    = 200
PASTA_DESTINO = 'vendas/products'

CATEGORIAS = {
    'Móveis':           {'preco_min': 500,  'preco_max': 8000,  'custo_pct': 0.45},
    'Eletrodomésticos': {'preco_min': 800,  'preco_max': 5000,  'custo_pct': 0.50},
    'Eletrônicos':      {'preco_min': 300,  'preco_max': 10000, 'custo_pct': 0.55},
    'Ferramentas':      {'preco_min': 50,   'preco_max': 2000,  'custo_pct': 0.40},
    'Embalagens':       {'preco_min': 10,   'preco_max': 500,   'custo_pct': 0.30},
    'Matéria-Prima':    {'preco_min': 5,    'preco_max': 300,   'custo_pct': 0.25},
}

UNIDADES    = ['UN', 'CX', 'KG', 'MT', 'PC', 'RL']
STATUS_LIST = ['ativo', 'inativo', 'descontinuado']


def gerar_produtos(quantidade):
    print(f'Gerando {quantidade} produtos...')
    produtos = []

    for i in range(quantidade):
        categoria      = random.choice(list(CATEGORIAS.keys()))
        config         = CATEGORIAS[categoria]
        preco_venda    = round(random.uniform(config['preco_min'], config['preco_max']), 2)
        custo_producao = round(preco_venda * config['custo_pct'], 2)

        produtos.append({
            'id_produto':       f'PRD-{str(i + 1).zfill(5)}',
            'nome':             f'{fake.word().capitalize()} {fake.word().capitalize()}',
            'descricao':        fake.sentence(nb_words=6),
            'categoria':        categoria,
            'unidade_medida':   random.choice(UNIDADES),
            'preco_venda':      preco_venda,
            'custo_producao':   custo_producao,
            'margem_bruta':     round(preco_venda - custo_producao, 2),
            'peso_kg':          round(random.uniform(0.1, 50.0), 2),
            'estoque_minimo':   random.randint(10, 100),
            'estoque_atual':    random.randint(0, 500),
            'status':           random.choice(STATUS_LIST),
            'data_cadastro':    fake.date_between(start_date='-5y', end_date='today').isoformat(),
            'data_atualizacao': timestamp_agora(),
            'data_carga':       data_hoje(),
        })

    return produtos


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE PRODUTOS — Neurometric')
    print('=' * 50)

    dados         = gerar_produtos(QUANTIDADE)
    caminho, nome = salvar_csv(dados, 'products')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)