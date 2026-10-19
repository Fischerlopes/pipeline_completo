{{
    config(
        materialized='incremental',
        unique_key='id_entrega',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_deliveries') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_entrega,
        id_pedido,
        id_cliente,
        COALESCE(transportadora, 'Não informado') AS transportadora,
        COALESCE(codigo_rastreio, 'Sem rastreio') AS codigo_rastreio,
        CASE
            WHEN LOWER(status) = 'aguardando_coleta'  THEN 'aguardando_coleta'
            WHEN LOWER(status) = 'coletado'           THEN 'coletado'
            WHEN LOWER(status) = 'em_transito'        THEN 'em_transito'
            WHEN LOWER(status) = 'saiu_para_entrega'  THEN 'saiu_para_entrega'
            WHEN LOWER(status) = 'entregue'           THEN 'entregue'
            WHEN LOWER(status) = 'tentativa_falha'    THEN 'tentativa_falha'
            WHEN LOWER(status) = 'devolvido'          THEN 'devolvido'
            ELSE 'desconhecido'
        END                                       AS status,
        CAST(data_coleta_prev AS DATE)            AS data_coleta_prev,
        CAST(data_entrega_prev AS DATE)           AS data_entrega_prev,
        CAST(data_entrega_real AS DATE)           AS data_entrega_real,
        tentativas,
        -- Atraso em dias (quando entregue)
        CASE
            WHEN data_entrega_real IS NOT NULL
            AND data_entrega_prev IS NOT NULL
            THEN DATE_DIFF(
                CAST(data_entrega_real AS DATE),
                CAST(data_entrega_prev AS DATE),
                DAY
            )
            ELSE NULL
        END                                       AS dias_atraso,
        CAST(data_atualizacao AS TIMESTAMP)       AS data_atualizacao,
        CAST(data_carga AS DATE)                  AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_entrega
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1