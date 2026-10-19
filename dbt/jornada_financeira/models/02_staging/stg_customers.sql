-- ============================================================
-- STAGING: Clientes
-- Limpa e padroniza os dados brutos de clientes
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='id_cliente',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_customers') }}

    {% if is_incremental() %}
        -- Só processa registros novos ou alterados
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cleaned AS (
    SELECT
        -- Identificação
        id_cliente,

        -- Dados cadastrais limpos
        TRIM(UPPER(nome))                   AS nome,
        REGEXP_REPLACE(cnpj, r'[^0-9]', '') AS cnpj,
        LOWER(TRIM(email))                  AS email,
        REGEXP_REPLACE(telefone, r'[^0-9]', '') AS telefone,

        -- Classificação
        COALESCE(segmento, 'Não informado') AS segmento,

        -- Endereço
        REGEXP_REPLACE(cep, r'[^0-9]', '') AS cep,
        COALESCE(logradouro, 'Não informado') AS logradouro,
        COALESCE(cast(numero as STRING), 'S/N')             AS numero,
        COALESCE(bairro, 'Não informado')   AS bairro,
        UPPER(TRIM(cidade))                 AS cidade,
        UPPER(TRIM(estado))                 AS estado,

        -- Status normalizado
        CASE
            WHEN LOWER(status) = 'ativo'      THEN 'ativo'
            WHEN LOWER(status) = 'inativo'    THEN 'inativo'
            WHEN LOWER(status) = 'prospecto'  THEN 'prospecto'
            ELSE 'desconhecido'
        END                                 AS status,

        -- Datas
        CAST(data_cadastro AS DATE)         AS data_cadastro,
        CAST(data_atualizacao AS TIMESTAMP) AS data_atualizacao,
        CAST(data_carga AS DATE)            AS data_carga,

        -- Controle dbt
        dbt_loaded_at

    FROM source
),

-- Remove duplicatas mantendo o registro mais recente
deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT(rn)
FROM deduplicado
WHERE rn = 1