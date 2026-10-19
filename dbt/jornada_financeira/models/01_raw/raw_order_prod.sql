SELECT
    id_ordem,
    id_pedido,
    id_produto,
    linha_producao,
    turno,
    CAST(qtd_planejada AS INT64)     AS qtd_planejada,
    CAST(qtd_produzida AS INT64)     AS qtd_produzida,
    CAST(custo_unitario AS FLOAT64)  AS custo_unitario,
    CAST(custo_total AS FLOAT64)     AS custo_total,
    status,
    data_inicio_prev,
    data_fim_prev,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()              AS dbt_loaded_at
FROM {{ source('bucket_producao', 'order_prod') }}