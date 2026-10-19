{{
    config(
        materialized='incremental',
        unique_key='id_pedido',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_orders') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_pedido,
        id_cliente,
        id_produto,
        COALESCE(canal_venda, 'Não informado') AS canal_venda,
        quantidade,
        ROUND(preco_unitario, 2)               AS preco_unitario,
        ROUND(desconto_pct, 4)                 AS desconto_pct,
        ROUND(valor_bruto, 2)                  AS valor_bruto,
        ROUND(valor_desconto, 2)               AS valor_desconto,
        ROUND(valor_liquido, 2)                AS valor_liquido,
        CASE
            WHEN LOWER(status) = 'rascunho'     THEN 'rascunho'
            WHEN LOWER(status) = 'confirmado'   THEN 'confirmado'
            WHEN LOWER(status) = 'em_producao'  THEN 'em_producao'
            WHEN LOWER(status) = 'faturado'     THEN 'faturado'
            WHEN LOWER(status) = 'entregue'     THEN 'entregue'
            WHEN LOWER(status) = 'cancelado'    THEN 'cancelado'
            ELSE 'desconhecido'
        END                                    AS status,
        CAST(data_pedido AS DATE)              AS data_pedido,
        CAST(data_entrega_prev AS DATE)        AS data_entrega_prev,
        CAST(data_atualizacao AS TIMESTAMP)    AS data_atualizacao,
        CAST(data_carga AS DATE)               AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_pedido
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1