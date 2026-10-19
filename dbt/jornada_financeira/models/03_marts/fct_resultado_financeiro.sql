-- ============================================================
-- MART: Resultado Financeiro por Pedido
-- Cruza vendas + produção + logística
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='id_pedido',
        incremental_strategy='merge',
        schema='marts'
    )
}}

WITH pedidos AS (
    SELECT
        id_pedido,
        id_cliente,
        id_produto,
        canal_venda,
        quantidade,
        preco_unitario,
        desconto_pct,
        valor_bruto,
        valor_desconto,
        valor_liquido,
        status          AS status_pedido,
        data_pedido,
        data_entrega_prev
    FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE data_carga >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

clientes AS (
    SELECT
        id_cliente,
        nome            AS nome_cliente,
        segmento,
        cidade,
        estado
    FROM {{ ref('stg_customers') }}
),

produtos AS (
    SELECT
        id_produto,
        nome            AS nome_produto,
        categoria,
        preco_venda,
        custo_producao  AS custo_producao_unitario
    FROM {{ ref('stg_products') }}
),

-- Custo de produção por pedido
custo_ordem AS (
    SELECT
        id_pedido,
        SUM(custo_total)        AS custo_ordem_producao,
        SUM(qtd_produzida)      AS qtd_total_produzida,
        MIN(data_inicio_prev)   AS data_inicio_producao,
        MAX(data_fim_prev)      AS data_fim_producao,
        DATE_DIFF(
            MAX(data_fim_prev),
            MIN(data_inicio_prev),
            DAY
        )                       AS dias_producao_planejado,
        COUNTIF(status = 'finalizado') AS ordens_finalizadas,
        COUNT(*)                AS total_ordens
    FROM {{ ref('stg_order_prod') }}
    GROUP BY id_pedido
),

-- Custo de insumos por pedido (via ordens de produção)
custo_insumos AS (
    SELECT
        op.id_pedido,
        SUM(pi.custo_total)     AS custo_insumos_total
    FROM {{ ref('stg_production_inputs') }} pi
    INNER JOIN {{ ref('stg_order_prod') }} op
        ON pi.id_ordem = op.id_ordem
    GROUP BY op.id_pedido
),

-- Dados de entrega por pedido
-- Um pedido pode ter várias entregas: mantém só a mais recente,
-- senão o join duplica id_pedido e o MERGE falha
entregas AS (
    SELECT
        id_pedido,
        id_entrega,
        transportadora,
        status          AS status_entrega,
        data_coleta_prev,
        data_entrega_prev,
        data_entrega_real,
        dias_atraso,
        tentativas,
        CASE
            WHEN status = 'entregue' AND dias_atraso <= 0 THEN 'no_prazo'
            WHEN status = 'entregue' AND dias_atraso > 0  THEN 'atrasado'
            WHEN status = 'devolvido'                     THEN 'devolvido'
            ELSE 'em_andamento'
        END             AS sla_status,
        COUNT(*) OVER (PARTITION BY id_pedido) AS total_entregas
    FROM {{ ref('stg_deliveries') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_pedido
        ORDER BY data_coleta_prev DESC, id_entrega DESC
    ) = 1
),

-- Custo de frete por pedido
custo_frete AS (
    SELECT
        id_pedido,
        SUM(custo_frete_brl)    AS custo_frete_total,
        SUM(seguro_brl)         AS custo_seguro_total,
        SUM(custo_total_brl)    AS custo_logistico_total,
        AVG(cotacao_dolar)      AS cotacao_dolar_media
    FROM {{ ref('stg_freight_costs') }}
    GROUP BY id_pedido
),

-- Custo de ocorrências por pedido
custo_ocorrencias AS (
    SELECT
        id_pedido,
        COUNT(*)                AS total_ocorrencias,
        SUM(custo_ocorrencia)   AS custo_ocorrencias_total,
        COUNTIF(LOWER(tipo_ocorrencia) LIKE '%devolu%') AS total_devolucoes,
        SUM(CASE
            WHEN LOWER(tipo_ocorrencia) LIKE '%devolu%'
            THEN custo_ocorrencia ELSE 0
        END)                    AS custo_devolucoes
    FROM {{ ref('stg_occurrences') }}
    GROUP BY id_pedido
),

-- Juntando tudo
resultado AS (
    SELECT
        -- Identificação
        p.id_pedido,
        p.id_cliente,
        p.id_produto,

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
        p.quantidade,
        p.preco_unitario,
        p.desconto_pct,
        p.status_pedido,
        p.data_pedido,

        -- ── RECEITA ──────────────────────────────────────
        p.valor_bruto,
        p.valor_desconto,
        p.valor_liquido,

        -- ── CUSTO DE PRODUÇÃO ────────────────────────────
        COALESCE(co.custo_ordem_producao, 0)    AS custo_ordem_producao,
        COALESCE(ci.custo_insumos_total, 0)     AS custo_insumos_total,
        COALESCE(co.custo_ordem_producao, 0)
            + COALESCE(ci.custo_insumos_total, 0) AS custo_producao_total,
        co.dias_producao_planejado,
        co.data_inicio_producao,
        co.data_fim_producao,
        co.ordens_finalizadas,
        co.total_ordens,

        -- ── CUSTO LOGÍSTICO ──────────────────────────────
        COALESCE(cf.custo_frete_total, 0)       AS custo_frete_total,
        COALESCE(cf.custo_seguro_total, 0)      AS custo_seguro_total,
        COALESCE(oc.custo_ocorrencias_total, 0) AS custo_ocorrencias_total,
        COALESCE(oc.custo_devolucoes, 0)        AS custo_devolucoes,
        COALESCE(cf.custo_logistico_total, 0)
            + COALESCE(oc.custo_ocorrencias_total, 0) AS custo_logistico_total,

        -- ── MARGEM ───────────────────────────────────────
        p.valor_liquido
            - COALESCE(co.custo_ordem_producao, 0)
            - COALESCE(ci.custo_insumos_total, 0)   AS margem_bruta,

        p.valor_liquido
            - COALESCE(co.custo_ordem_producao, 0)
            - COALESCE(ci.custo_insumos_total, 0)
            - COALESCE(cf.custo_logistico_total, 0)
            - COALESCE(oc.custo_ocorrencias_total, 0) AS margem_liquida,

        ROUND(SAFE_DIVIDE(
            p.valor_liquido
                - COALESCE(co.custo_ordem_producao, 0)
                - COALESCE(ci.custo_insumos_total, 0)
                - COALESCE(cf.custo_logistico_total, 0)
                - COALESCE(oc.custo_ocorrencias_total, 0),
            NULLIF(p.valor_liquido, 0)
        ) * 100, 2)                             AS margem_liquida_pct,

        -- ── LOGÍSTICA ────────────────────────────────────
        COALESCE(e.total_entregas, 0)           AS total_entregas,
        e.transportadora,
        e.status_entrega,
        e.sla_status,
        e.dias_atraso,
        e.tentativas,
        e.data_entrega_prev,
        e.data_entrega_real,
        COALESCE(oc.total_ocorrencias, 0)       AS total_ocorrencias,
        COALESCE(oc.total_devolucoes, 0)        AS total_devolucoes,

        -- ── TEMPOS ───────────────────────────────────────
        -- Pedido → início da produção
        DATE_DIFF(
            co.data_inicio_producao,
            p.data_pedido,
            DAY
        )                                       AS dias_pedido_ate_producao,

        -- Pedido → fim da produção (carregamento)
        DATE_DIFF(
            co.data_fim_producao,
            p.data_pedido,
            DAY
        )                                       AS dias_pedido_ate_carregamento,

        -- Pedido → entrega real
        DATE_DIFF(
            e.data_entrega_real,
            p.data_pedido,
            DAY
        )                                       AS dias_pedido_ate_entrega,

        -- ── CONTROLE ─────────────────────────────────────
        CURRENT_TIMESTAMP()                     AS dbt_updated_at

    FROM pedidos p
    LEFT JOIN clientes c       ON p.id_cliente = c.id_cliente
    LEFT JOIN produtos pr      ON p.id_produto = pr.id_produto
    LEFT JOIN custo_ordem co   ON p.id_pedido  = co.id_pedido
    LEFT JOIN custo_insumos ci ON p.id_pedido  = ci.id_pedido
    LEFT JOIN entregas e       ON p.id_pedido  = e.id_pedido
    LEFT JOIN custo_frete cf   ON p.id_pedido  = cf.id_pedido
    LEFT JOIN custo_ocorrencias oc ON p.id_pedido = oc.id_pedido
)

SELECT * FROM resultado