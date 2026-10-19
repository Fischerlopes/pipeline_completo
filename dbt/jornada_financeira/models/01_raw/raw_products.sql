SELECT
    id_produto,
    nome,
    descricao,
    categoria,
    unidade_medida,
    CAST(preco_venda AS FLOAT64)    AS preco_venda,
    CAST(custo_producao AS FLOAT64) AS custo_producao,
    CAST(margem_bruta AS FLOAT64)   AS margem_bruta,
    CAST(peso_kg AS FLOAT64)        AS peso_kg,
    CAST(estoque_minimo AS INT64)   AS estoque_minimo,
    CAST(estoque_atual AS INT64)    AS estoque_atual,
    status,
    data_cadastro,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()             AS dbt_loaded_at
FROM {{ source('bucket_vendas', 'products') }}