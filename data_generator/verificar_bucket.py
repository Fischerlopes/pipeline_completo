from google.cloud import storage

cliente = storage.Client.from_service_account_json(
    'secrets/neuro-pipeline-dev.json',
    project='neuro-pipeline-dev'
)

bucket = cliente.bucket('neuro-pipeline-dev-raw')
blobs  = list(bucket.list_blobs())

print(f'Total de arquivos no bucket: {len(blobs)}')
print()

for blob in sorted(blobs, key=lambda x: x.name):
    tamanho = round(blob.size / 1024, 1)
    print(f'  {blob.name}  ({tamanho} KB)')