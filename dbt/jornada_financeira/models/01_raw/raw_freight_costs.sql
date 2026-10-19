SELECT
    id_frete,
    id_entrega,
    id_pedido,
    modalidade,
    faixa_peso,
    CAST(custo_frete_brl AS FLOAT64) AS custo_frete_brl,
    CAST(custo_frete_usd AS FLOAT64) AS custo_frete_usd,
    CAST(cotacao_dolar AS FLOAT64)   AS cotacao_dolar,
    CAST(seguro_brl AS FLOAT64)      AS seguro_brl,
    CAST(custo_total_brl AS FLOAT64) AS custo_total_brl,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()              AS dbt_loaded_at
FROM {{ source('bucket_logistica', 'freight_costs') }}