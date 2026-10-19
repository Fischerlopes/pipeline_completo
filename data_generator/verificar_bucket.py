from utils import get_cliente_gcs, BUCKET_NAME

bucket = get_cliente_gcs().bucket(BUCKET_NAME)
blobs  = list(bucket.list_blobs())

print(f'Total de arquivos no bucket: {len(blobs)}')
print()

for blob in sorted(blobs, key=lambda x: x.name):
    tamanho = round(blob.size / 1024, 1)
    print(f'  {blob.name}  ({tamanho} KB)')