# ============================================================
# GERADOR DE CUSTOS DE FRETE
# Neurometric — neuro-pipeline-dev
# ============================================================
# Depende de: deliveries_*.csv
# Enriquece com: API Banco Central (taxa de câmbio do dia)
# ============================================================

import random
import glob
import requests
import pandas as pd
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora, gerar_id

fake = Faker('pt_BR')

PASTA_DESTINO = 'logistica/freight_costs'

MODALIDADES = ['Rodoviário', 'Aéreo', 'Fluvial']

FAIXAS_PESO = {
    'até 1kg':    {'min': 8,   'max': 25},
    '1kg a 5kg':  {'min': 15,  'max': 60},
    '5kg a 20kg': {'min': 40,  'max': 150},
    'acima 20kg': {'min': 100, 'max': 800},
}


def buscar_dolar():
    """
    Consulta a API do Banco Central para pegar a cotação
    do dólar do dia. Usado para registrar o câmbio no momento
    do frete — útil para análises de custo em moeda estrangeira.
    """
    try:
        url      = 'https://economia.awesomeapi.com.br/json/last/USD-BRL'
        resposta = requests.get(url, timeout=5)
        if resposta.status_code == 200:
            dados = resposta.json()
            cotacao = float(dados['USDBRL']['bid'])
            print(f'  Cotação do dólar consultada: R$ {cotacao:.4f}')
            return cotacao
    except Exception:
        pass

    print('  API de câmbio indisponível — usando valor padrão R$ 5.20')
    return 5.20


def carregar_entregas():
    padrao   = 'data_generator/temp/deliveries_*.csv'
    arquivos = sorted(glob.glob(padrao))

    if not arquivos:
        raise FileNotFoundError(
            'Nenhum arquivo de entregas encontrado. '
            'Rode o gerar_entregas.py primeiro.'
        )

    df = pd.read_csv(arquivos[-1])
    print(f'  {len(df)} entregas carregadas de {arquivos[-1]}')
    return df


def gerar_fretes(df_entregas, cotacao_dolar):
    print(f'Gerando custos de frete para {len(df_entregas)} entregas...')
    fretes = []

    for i, (_, entrega) in enumerate(df_entregas.iterrows()):
        id_frete   = gerar_id('FRT', i + 1)
        faixa      = random.choice(list(FAIXAS_PESO.keys()))
        config     = FAIXAS_PESO[faixa]
        modalidade = random.choice(MODALIDADES)

        # Aéreo é mais caro
        multiplicador = 2.5 if modalidade == 'Aéreo' else 1.0
        custo_brl = round(
            random.uniform(config['min'], config['max']) * multiplicador, 2
        )
        custo_usd = round(custo_brl / cotacao_dolar, 2)

        # Seguro: 0.3% a 1.5% do valor do frete
        seguro = round(custo_brl * random.uniform(0.003, 0.015), 2)

        fretes.append({
            'id_frete':          id_frete,
            'id_entrega':        entrega['id_entrega'],
            'id_pedido':         entrega['id_pedido'],
            'modalidade':        modalidade,
            'faixa_peso':        faixa,
            'custo_frete_brl':   custo_brl,
            'custo_frete_usd':   custo_usd,
            'cotacao_dolar':     cotacao_dolar,
            'seguro_brl':        seguro,
            'custo_total_brl':   round(custo_brl + seguro, 2),
            'data_atualizacao':  timestamp_agora(),
            'data_carga':        data_hoje(),
        })

    return fretes


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE FRETES — Neurometric')
    print('=' * 50)

    print('Consultando cotação do dólar...')
    cotacao_dolar = buscar_dolar()

    print('Carregando entregas...')
    df_entregas   = carregar_entregas()

    dados         = gerar_fretes(df_entregas, cotacao_dolar)
    caminho, nome = salvar_csv(dados, 'freight_costs')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)