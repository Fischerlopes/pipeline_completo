# ============================================================
# LIMPEZA DO BUCKET — arquiva os CSVs com IDs no formato antigo
# Neurometric — neuro-pipeline-dev
# ============================================================
# Move os arquivos das pastas lidas pelo dbt para
# arquivo_ids_antigos/<pasta>/, fora dos caminhos das tabelas
# externas (gs://.../<pasta>/*.csv). Nada é apagado de vez:
# para desfazer, rode com --restaurar.
#
# Uso (a partir da raiz do projeto):
#   python data_generator/limpar_bucket.py               → só mostra o que faria
#   python data_generator/limpar_bucket.py --executar    → move os arquivos
#   python data_generator/limpar_bucket.py --restaurar   → desfaz a limpeza
# ============================================================

import sys
from utils import get_cliente_gcs, BUCKET_NAME

PASTA_ARQUIVO      = 'arquivo_ids_antigos'

# Pastas lidas pelo dbt (models/01_raw/sources.yml)
PASTAS = [
    'vendas/customers',
    'vendas/products',
    'vendas/orders',
    'producao/order_prod',
    'producao/production_inputs',
    'producao/production_lines',
    'logistica/deliveries',
    'logistica/freight_costs',
    'logistica/occurrences',
]


def listar_movimentos(bucket, restaurar):
    """Retorna a lista de (origem, destino) a mover."""
    movimentos = []
    for pasta in PASTAS:
        origem_prefixo  = f'{PASTA_ARQUIVO}/{pasta}/' if restaurar else f'{pasta}/'
        destino_prefixo = f'{pasta}/' if restaurar else f'{PASTA_ARQUIVO}/{pasta}/'

        for blob in bucket.list_blobs(prefix=origem_prefixo):
            if not blob.name.endswith('.csv'):
                continue
            nome_arquivo = blob.name[len(origem_prefixo):]
            movimentos.append((blob, destino_prefixo + nome_arquivo))
    return movimentos


if __name__ == '__main__':
    executar  = '--executar' in sys.argv
    restaurar = '--restaurar' in sys.argv

    print('=' * 50)
    print('RESTAURAÇÃO DO BUCKET' if restaurar else 'LIMPEZA DO BUCKET')
    print('=' * 50)

    bucket = get_cliente_gcs().bucket(BUCKET_NAME)

    movimentos = listar_movimentos(bucket, restaurar)

    if not movimentos:
        print('Nenhum arquivo para mover.')
        sys.exit(0)

    for blob, destino in movimentos:
        print(f'  {blob.name}  ->  {destino}')

    print()
    print(f'Total: {len(movimentos)} arquivo(s)')

    if not (executar or restaurar):
        print()
        print('Modo simulação — nada foi alterado.')
        print('Para mover de verdade, rode com --executar')
        sys.exit(0)

    print()
    print('Movendo...')
    for blob, destino in movimentos:
        bucket.copy_blob(blob, bucket, destino)
        blob.delete()

    print(f'Concluído: {len(movimentos)} arquivo(s) movido(s).')
