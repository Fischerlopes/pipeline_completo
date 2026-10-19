-- ============================================================
-- RAW: Clientes
-- Lê o arquivo CSV do bucket e cria tabela bruta no BigQuery
-- ============================================================

SELECT
    id_cliente,
    nome,
    cnpj,
    email,
    telefone,
    segmento,
    cep,
    logradouro,
    numero,
    bairro,
    cidade,
    estado,
    status,
    data_cadastro,
    data_atualizacao,
    data_carga,

    -- Metadados de controle
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM
    {{ source('bucket_vendas', 'customers') }}