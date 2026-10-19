SELECT
    id_insumo,
    id_ordem,
    nome_insumo,
    fornecedor,
    unidade,
    CAST(qtd_planejada AS FLOAT64)   AS qtd_planejada,
    CAST(qtd_utilizada AS FLOAT64)   AS qtd_utilizada,
    CAST(custo_unitario AS FLOAT64)  AS custo_unitario,
    CAST(custo_total AS FLOAT64)     AS custo_total,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()              AS dbt_loaded_at
FROM {{ source('bucket_producao', 'production_inputs') }}