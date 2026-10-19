{{
    config(
        materialized='incremental',
        unique_key='id_insumo',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_production_inputs') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_insumo,
        id_ordem,
        COALESCE(nome_insumo, 'Não informado')  AS nome_insumo,
        COALESCE(fornecedor, 'Não informado')   AS fornecedor,
        COALESCE(unidade, 'UN')                 AS unidade,
        ROUND(qtd_planejada, 3)                 AS qtd_planejada,
        ROUND(qtd_utilizada, 3)                 AS qtd_utilizada,
        ROUND(custo_unitario, 2)                AS custo_unitario,
        ROUND(custo_total, 2)                   AS custo_total,
        -- Variação entre planejado e utilizado
        ROUND(qtd_utilizada - qtd_planejada, 3) AS variacao_qtd,
        CAST(data_atualizacao AS TIMESTAMP)     AS data_atualizacao,
        CAST(data_carga AS DATE)                AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_insumo
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1