{{
    config(
        materialized='incremental',
        unique_key='id_produto',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_products') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        id_produto,
        TRIM(nome)                          AS nome,
        TRIM(descricao)                     AS descricao,
        COALESCE(categoria, 'Sem categoria') AS categoria,
        COALESCE(unidade_medida, 'UN')      AS unidade_medida,
        ROUND(preco_venda, 2)               AS preco_venda,
        ROUND(custo_producao, 2)            AS custo_producao,
        ROUND(margem_bruta, 2)              AS margem_bruta,
        ROUND(peso_kg, 3)                   AS peso_kg,
        estoque_minimo,
        estoque_atual,
        CASE
            WHEN LOWER(status) = 'ativo'          THEN 'ativo'
            WHEN LOWER(status) = 'inativo'        THEN 'inativo'
            WHEN LOWER(status) = 'descontinuado'  THEN 'descontinuado'
            ELSE 'desconhecido'
        END                                 AS status,
        CAST(data_cadastro AS DATE)         AS data_cadastro,
        CAST(data_atualizacao AS TIMESTAMP) AS data_atualizacao,
        CAST(data_carga AS DATE)            AS data_carga,
        dbt_loaded_at
    FROM source
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_produto
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn) FROM deduplicado WHERE rn = 1