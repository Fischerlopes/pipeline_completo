SELECT
    id_pedido,
    id_cliente,
    id_produto,
    canal_venda,
    CAST(quantidade AS INT64)        AS quantidade,
    CAST(preco_unitario AS FLOAT64)  AS preco_unitario,
    CAST(desconto_pct AS FLOAT64)    AS desconto_pct,
    CAST(valor_bruto AS FLOAT64)     AS valor_bruto,
    CAST(valor_desconto AS FLOAT64)  AS valor_desconto,
    CAST(valor_liquido AS FLOAT64)   AS valor_liquido,
    status,
    data_pedido,
    data_entrega_prev,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()              AS dbt_loaded_at
FROM {{ source('bucket_vendas', 'orders') }}