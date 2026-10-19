{{
    config(
        materialized='incremental',
        unique_key='id_ocorrencia',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_occurrences') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_ocorrencia,
        id_entrega,
        id_pedido,
        COALESCE(tipo_ocorrencia, 'Não informado') AS tipo_ocorrencia,
        COALESCE(descricao, 'Sem descrição')       AS descricao,
        COALESCE(responsavel, 'Não informado')     AS responsavel,
        ROUND(custo_ocorrencia, 2)                 AS custo_ocorrencia,
        CASE
            WHEN LOWER(status) = 'aberta'       THEN 'aberta'
            WHEN LOWER(status) = 'em_analise'   THEN 'em_analise'
            WHEN LOWER(status) = 'resolvida'    THEN 'resolvida'
            WHEN LOWER(status) = 'encerrada'    THEN 'encerrada'
            ELSE 'desconhecido'
        END                                        AS status,
        CAST(data_ocorrencia AS DATE)              AS data_ocorrencia,
        CAST(data_resolucao AS DATE)               AS data_resolucao,
        -- Tempo de resolução em dias
        CASE
            WHEN data_resolucao IS NOT NULL
            THEN DATE_DIFF(
                CAST(data_resolucao AS DATE),
                CAST(data_ocorrencia AS DATE),
                DAY
            )
            ELSE NULL
        END                                        AS dias_resolucao,
        CAST(data_atualizacao AS TIMESTAMP)        AS data_atualizacao,
        CAST(data_carga AS DATE)                   AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_ocorrencia
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1