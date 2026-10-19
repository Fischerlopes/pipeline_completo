# ============================================================
# GERADOR DE CLIENTES
# Neurometric — neuro-pipeline-dev
# ============================================================

import random
import requests
from faker import Faker
from utils import salvar_csv, enviar_para_gcp, data_hoje, timestamp_agora

fake = Faker('pt_BR')

QUANTIDADE    = 1000
PASTA_DESTINO = 'vendas/customers'

CEPS_BASE = [
    '01310-100', '20040-020', '30112-000', '80010-010',
    '90010-150', '40020-010', '60135-235', '50010-010',
    '66010-000', '69010-010',
]


def buscar_cep(cep):
    try:
        cep_limpo = cep.replace('-', '')
        resposta  = requests.get(
            f'https://viacep.com.br/ws/{cep_limpo}/json/',
            timeout=5
        )
        if resposta.status_code == 200:
            dados = resposta.json()
            if 'erro' not in dados:
                return {
                    'cidade':     dados.get('localidade', 'Desconhecido'),
                    'estado':     dados.get('uf', 'XX'),
                    'bairro':     dados.get('bairro', 'Centro'),
                    'logradouro': dados.get('logradouro', ''),
                }
    except Exception:
        pass
    return {'cidade': 'São Paulo', 'estado': 'SP',
            'bairro': 'Centro', 'logradouro': ''}


def gerar_clientes(quantidade):
    print(f'Gerando {quantidade} clientes...')

    print('Consultando CEPs na API ViaCEP...')
    dados_ceps = {}
    for cep in CEPS_BASE:
        dados_ceps[cep] = buscar_cep(cep)
        print(f'  CEP {cep} → {dados_ceps[cep]["cidade"]} - {dados_ceps[cep]["estado"]}')

    segmentos   = ['Varejo', 'Atacado', 'Industrial', 'Governo', 'E-commerce']
    status_list = ['ativo', 'inativo', 'prospecto']

    clientes = []
    for i in range(quantidade):
        cep       = random.choice(CEPS_BASE)
        dados_cep = dados_ceps[cep]

        clientes.append({
            'id_cliente':       f'CLI-{str(i + 1).zfill(5)}',
            'nome':             fake.company(),
            'cnpj':             fake.cnpj(),
            'email':            fake.company_email(),
            'telefone':         fake.phone_number(),
            'segmento':         random.choice(segmentos),
            'cep':              cep,
            'logradouro':       dados_cep['logradouro'] or fake.street_name(),
            'numero':           str(random.randint(1, 9999)),
            'bairro':           dados_cep['bairro'],
            'cidade':           dados_cep['cidade'],
            'estado':           dados_cep['estado'],
            'status':           random.choice(status_list),
            'data_cadastro':    fake.date_between(start_date='-3y', end_date='today').isoformat(),
            'data_atualizacao': timestamp_agora(),
            'data_carga':       data_hoje(),
        })

    return clientes


if __name__ == '__main__':
    print('=' * 50)
    print('GERADOR DE CLIENTES — Neurometric')
    print('=' * 50)

    dados               = gerar_clientes(QUANTIDADE)
    caminho, nome       = salvar_csv(dados, 'customers')
    enviar_para_gcp(caminho, nome, PASTA_DESTINO)

    print('=' * 50)
    print('Concluído com sucesso!')
    print('=' * 50)