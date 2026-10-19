SELECT
    id_ocorrencia,
    id_entrega,
    id_pedido,
    tipo_ocorrencia,
    descricao,
    responsavel,
    CAST(custo_ocorrencia AS FLOAT64) AS custo_ocorrencia,
    status,
    data_ocorrencia,
    data_resolucao,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()               AS dbt_loaded_at
FROM {{ source('bucket_logistica', 'occurrences') }}