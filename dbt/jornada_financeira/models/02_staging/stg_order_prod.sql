{{
    config(
        materialized='incremental',
        unique_key='id_ordem',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_order_prod') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_ordem,
        id_pedido,
        id_produto,
        COALESCE(linha_producao, 'Não informado') AS linha_producao,
        COALESCE(turno, 'Não informado')           AS turno,
        qtd_planejada,
        qtd_produzida,
        ROUND(custo_unitario, 2)                   AS custo_unitario,
        ROUND(custo_total, 2)                      AS custo_total,
        -- Eficiência de produção
        CASE
            WHEN qtd_planejada > 0
            THEN ROUND(SAFE_DIVIDE(qtd_produzida, qtd_planejada), 4)
            ELSE 0
        END                                        AS eficiencia_producao,
        CASE
            WHEN LOWER(status) = 'aguardando'    THEN 'aguardando'
            WHEN LOWER(status) = 'em_producao'   THEN 'em_producao'
            WHEN LOWER(status) = 'pausado'       THEN 'pausado'
            WHEN LOWER(status) = 'finalizado'    THEN 'finalizado'
            WHEN LOWER(status) = 'cancelado'     THEN 'cancelado'
            ELSE 'desconhecido'
        END                                        AS status,
        CAST(data_inicio_prev AS DATE)             AS data_inicio_prev,
        CAST(data_fim_prev AS DATE)                AS data_fim_prev,
        CAST(data_atualizacao AS TIMESTAMP)        AS data_atualizacao,
        CAST(data_carga AS DATE)                   AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_ordem
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1