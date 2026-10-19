{{
    config(
        materialized='incremental',
        unique_key='id_frete',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_freight_costs') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_frete,
        id_entrega,
        id_pedido,
        COALESCE(modalidade, 'Não informado')   AS modalidade,
        COALESCE(faixa_peso, 'Não informado')   AS faixa_peso,
        ROUND(custo_frete_brl, 2)               AS custo_frete_brl,
        ROUND(custo_frete_usd, 2)               AS custo_frete_usd,
        ROUND(cotacao_dolar, 4)                 AS cotacao_dolar,
        ROUND(seguro_brl, 2)                    AS seguro_brl,
        ROUND(custo_total_brl, 2)               AS custo_total_brl,
        CAST(data_atualizacao AS TIMESTAMP)     AS data_atualizacao,
        CAST(data_carga AS DATE)                AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_frete
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1