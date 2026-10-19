# ============================================================
# GERADOR DE LINHAS DE PRODUÇÃO
# Neurometric — neuro-pipeline-dev
# ============================================================

import random
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora

fake = Faker('pt_BR')

PASTA_DESTINO = 'producao/production_lines'

LINHAS = [
    {'id': 'Linha-A', 'descricao': 'Montagem de móveis',        'capacidade': 200},
    {'id': 'Linha-B', 'descricao': 'Eletrodomésticos',          'capacidade': 150},
    {'id': 'Linha-C', 'descricao': 'Eletrônicos e ferramentas', 'capacidade': 100},
    {'id': 'Linha-D', 'descricao': 'Embalagem e acabamento',    'capacidade': 300},
]

TURNOS  = ['Manhã', 'Tarde', 'Noite']
STATUS  = ['ativa', 'manutencao', 'inativa']


def gerar_linhas():
    print('Gerando linhas de produção...')
    registros = []
    contador  = 1

    for linha in LINHAS:
        for turno in TURNOS:
            id_registro = f'LPT-{str(contador).zfill(4)}'

            registros.append({
                'id_linha_turno':    id_registro,
                'id_linha':          linha['id'],
                'descricao':         linha['descricao'],
                'turno':             turno,
                'capacidade_dia':    linha['capacidade'],
                'eficiencia_pct':    round(random.uniform(0.70, 0.98), 4),
                'operadores':        random.randint(3, 20),
                'status':            random.choice(STATUS),
                'data_atualizacao':  timestamp_agora(),
                'data_carga':        data_hoje(),
            })
            contador += 1

    return registros


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE LINHAS DE PRODUÇÃO — Neurometric')
    print('=' * 50)

    dados         = gerar_linhas()
    caminho, nome = salvar_csv(dados, 'production_lines')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)