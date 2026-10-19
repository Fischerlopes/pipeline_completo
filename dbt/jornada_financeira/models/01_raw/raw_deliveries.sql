SELECT
    id_entrega,
    id_pedido,
    id_cliente,
    transportadora,
    codigo_rastreio,
    status,
    data_coleta_prev,
    data_entrega_prev,
    data_entrega_real,
    CAST(tentativas AS INT64)        AS tentativas,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()              AS dbt_loaded_at
FROM {{ source('bucket_logistica', 'deliveries') }}