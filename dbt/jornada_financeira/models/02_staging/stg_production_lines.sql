{{
    config(
        materialized='incremental',
        unique_key='id_linha_turno',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_production_lines') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_linha_turno,
        id_linha,
        COALESCE(descricao, 'Não informado')    AS descricao,
        COALESCE(turno, 'Não informado')        AS turno,
        capacidade_dia,
        ROUND(eficiencia_pct, 4)                AS eficiencia_pct,
        operadores,
        -- Capacidade real considerando eficiência
        ROUND(capacidade_dia * eficiencia_pct)  AS capacidade_real,
        CASE
            WHEN LOWER(status) = 'ativa'        THEN 'ativa'
            WHEN LOWER(status) = 'manutencao'   THEN 'manutencao'
            WHEN LOWER(status) = 'inativa'      THEN 'inativa'
            ELSE 'desconhecido'
        END                                     AS status,
        CAST(data_atualizacao AS TIMESTAMP)     AS data_atualizacao,
        CAST(data_carga AS DATE)                AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_linha_turno
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1
