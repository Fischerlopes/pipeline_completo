# ============================================================
# GERADOR DE OCORRÊNCIAS LOGÍSTICAS
# Neurometric — neuro-pipeline-dev
# ============================================================
# Depende de: deliveries_*.csv
# ============================================================

import random
import glob
import pandas as pd
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora, gerar_id

fake = Faker('pt_BR')

PASTA_DESTINO = 'logistica/occurrences'

TIPOS_OCORRENCIA = [
    'Atraso na entrega',
    'Avaria na mercadoria',
    'Endereço não encontrado',
    'Cliente ausente',
    'Devolução solicitada',
    'Extravio',
    'Roubo de carga',
]

STATUS_OCORRENCIA = [
    'aberta',
    'em_analise',
    'resolvida',
    'encerrada',
]

RESPONSAVEIS = [
    'Transportadora',
    'Remetente',
    'Destinatário',
    'Força maior',
]


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


def gerar_ocorrencias(df_entregas):
    print('Gerando ocorrências...')

    # Apenas 20% das entregas têm ocorrências — mais realista
    amostra = df_entregas.sample(frac=0.30)
    print(f'  {len(amostra)} entregas selecionadas para ocorrências')

    ocorrencias = []

    for i, (_, entrega) in enumerate(amostra.iterrows()):
        id_ocorrencia  = gerar_id('OCO', i + 1)
        tipo           = random.choice(TIPOS_OCORRENCIA)
        custo_ocorrencia = round(random.uniform(50, 3000), 2)

        data_ocorrencia = fake.date_between(
            start_date='-1y',
            end_date='today'
        )
        data_resolucao  = (
            fake.date_between(
                start_date=data_ocorrencia,
                end_date='+30d'
            ).isoformat()
            if random.random() > 0.4
            else None
        )

        ocorrencias.append({
            'id_ocorrencia':    id_ocorrencia,
            'id_entrega':       entrega['id_entrega'],
            'id_pedido':        entrega['id_pedido'],
            'tipo_ocorrencia':  tipo,
            'descricao':        fake.sentence(nb_words=10),
            'responsavel':      random.choice(RESPONSAVEIS),
            'custo_ocorrencia': custo_ocorrencia,
            'status':           random.choice(STATUS_OCORRENCIA),
            'data_ocorrencia':  data_ocorrencia.isoformat(),
            'data_resolucao':   data_resolucao,
            'data_atualizacao': timestamp_agora(),
            'data_carga':       data_hoje(),
        })

    return ocorrencias


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE OCORRÊNCIAS — Neurometric')
    print('=' * 50)

    print('Carregando entregas...')
    df_entregas   = carregar_entregas()

    dados         = gerar_ocorrencias(df_entregas)
    caminho, nome = salvar_csv(dados, 'occurrences')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)