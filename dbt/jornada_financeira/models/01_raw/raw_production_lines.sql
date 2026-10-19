SELECT
    id_linha_turno,
    id_linha,
    descricao,
    turno,
    CAST(capacidade_dia AS INT64)    AS capacidade_dia,
    CAST(eficiencia_pct AS FLOAT64)  AS eficiencia_pct,
    CAST(operadores AS INT64)        AS operadores,
    status,
    data_atualizacao,
    data_carga,
    CURRENT_TIMESTAMP()              AS dbt_loaded_at
FROM {{ source('bucket_producao', 'production_lines') }}