-- ============================================================
-- MART: Logística por Entrega
-- SLA, fretes, ocorrências e devoluções
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='id_entrega',
        incremental_strategy='merge',
        schema='marts'
    )
}}

WITH entregas AS (
    SELECT
        id_entrega,
        id_pedido,
        id_cliente,
        transportadora,
        status                              AS status_entrega,
        data_coleta_prev,
        data_entrega_prev,
        data_entrega_real,
        dias_atraso,
        tentativas,
        CASE
            WHEN status = 'entregue' AND COALESCE(dias_atraso, 0) <= 0 THEN 'no_prazo'
            WHEN status = 'entregue' AND dias_atraso > 0               THEN 'atrasado'
            WHEN status = 'devolvido'                                   THEN 'devolvido'
            ELSE 'em_andamento'
        END                                 AS sla_status
    FROM {{ ref('stg_deliveries') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

clientes AS (
    SELECT
        id_cliente,
        nome        AS nome_cliente,
        segmento,
        cidade,
        estado
    FROM {{ ref('stg_customers') }}
),

pedidos AS (
    SELECT
        id_pedido,
        id_produto,
        canal_venda,
        valor_liquido,
        data_pedido
    FROM {{ ref('stg_orders') }}
),

produtos AS (
    SELECT
        id_produto,
        nome        AS nome_produto,
        categoria
    FROM {{ ref('stg_products') }}
),

fretes AS (
    SELECT
        id_entrega,
        modalidade,
        faixa_peso,
        custo_frete_brl,
        seguro_brl                          AS custo_seguro,
        custo_total_brl                     AS custo_logistico_total,
        cotacao_dolar
    FROM {{ ref('stg_freight_costs') }}
),

ocorrencias AS (
    SELECT
        id_entrega,
        COUNT(*)                            AS total_ocorrencias,
        SUM(custo_ocorrencia)               AS custo_ocorrencias_total,
        COUNTIF(
            status != 'encerrada'
            AND status != 'resolvida'
        )                                   AS ocorrencias_abertas,
        STRING_AGG(
            DISTINCT tipo_ocorrencia
            ORDER BY tipo_ocorrencia
            LIMIT 3
        )                                   AS tipos_ocorrencia,
        MAX(dias_resolucao)                 AS maior_tempo_resolucao,
        AVG(dias_resolucao)                 AS tempo_medio_resolucao,
        COUNTIF(
            LOWER(tipo_ocorrencia) LIKE '%devolu%'
        )                                   AS total_devolucoes,
        SUM(CASE
            WHEN LOWER(tipo_ocorrencia) LIKE '%devolu%'
            THEN custo_ocorrencia
            ELSE 0
        END)                                AS custo_devolucoes
    FROM {{ ref('stg_occurrences') }}
    GROUP BY id_entrega
)

SELECT
    -- Identificação
    e.id_entrega,
    e.id_pedido,
    e.id_cliente,

    -- Cliente
    c.nome_cliente,
    c.segmento,
    c.cidade,
    c.estado,

    -- Produto
    pr.nome_produto,
    pr.categoria,

    -- Pedido
    p.canal_venda,
    p.valor_liquido                         AS receita_pedido,
    p.data_pedido,

    -- Entrega
    e.transportadora,
    e.status_entrega,
    e.sla_status,
    e.dias_atraso,
    e.tentativas,
    e.data_coleta_prev,
    e.data_entrega_prev,
    e.data_entrega_real,

    -- Frete
    f.modalidade,
    f.faixa_peso,
    COALESCE(f.custo_frete_brl, 0)          AS custo_frete,
    COALESCE(f.custo_seguro, 0)             AS custo_seguro,
    COALESCE(f.custo_logistico_total, 0)    AS custo_logistico_total,
    f.cotacao_dolar,

    -- Ocorrências
    COALESCE(o.total_ocorrencias, 0)        AS total_ocorrencias,
    COALESCE(o.ocorrencias_abertas, 0)      AS ocorrencias_abertas,
    COALESCE(o.custo_ocorrencias_total, 0)  AS custo_ocorrencias_total,
    COALESCE(o.total_devolucoes, 0)         AS total_devolucoes,
    COALESCE(o.custo_devolucoes, 0)         AS custo_devolucoes,
    o.tipos_ocorrencia,
    o.tempo_medio_resolucao,
    o.maior_tempo_resolucao,

    -- Custo total da entrega
    COALESCE(f.custo_logistico_total, 0)
        + COALESCE(o.custo_ocorrencias_total, 0) AS custo_total_entrega,

    -- SLA flags
    CASE WHEN e.sla_status = 'no_prazo'  THEN 1 ELSE 0 END AS flag_no_prazo,
    CASE WHEN e.sla_status = 'atrasado'  THEN 1 ELSE 0 END AS flag_atrasado,
    CASE WHEN e.sla_status = 'devolvido' THEN 1 ELSE 0 END AS flag_devolvido,

    -- Tempos
    DATE_DIFF(
        e.data_coleta_prev,
        p.data_pedido,
        DAY
    )                                       AS dias_pedido_ate_coleta,

    DATE_DIFF(
        e.data_entrega_real,
        p.data_pedido,
        DAY
    )                                       AS dias_pedido_ate_entrega,

    -- Controle
    CURRENT_TIMESTAMP()                     AS dbt_updated_at

FROM entregas e
LEFT JOIN clientes c    ON e.id_cliente = c.id_cliente
LEFT JOIN pedidos p     ON e.id_pedido  = p.id_pedido
LEFT JOIN produtos pr   ON p.id_produto = pr.id_produto
LEFT JOIN fretes f      ON e.id_entrega = f.id_entrega
LEFT JOIN ocorrencias o ON e.id_entrega = o.id_entrega