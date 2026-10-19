-- ============================================================
-- TESTE: valores monetários e quantidades não podem ser negativos
-- Retorna uma linha por registro inválido, indicando a tabela
-- ============================================================

SELECT 'stg_orders' AS tabela, id_pedido AS id
FROM {{ ref('stg_orders') }}
WHERE quantidade < 0
   OR preco_unitario < 0
   OR valor_bruto < 0
   OR valor_liquido < 0

UNION ALL

SELECT 'stg_order_prod', id_ordem
FROM {{ ref('stg_order_prod') }}
WHERE qtd_planejada < 0
   OR qtd_produzida < 0
   OR custo_total < 0

UNION ALL

SELECT 'stg_production_inputs', id_insumo
FROM {{ ref('stg_production_inputs') }}
WHERE qtd_utilizada < 0
   OR custo_total < 0

UNION ALL

SELECT 'stg_freight_costs', id_frete
FROM {{ ref('stg_freight_costs') }}
WHERE custo_frete_brl < 0
   OR seguro_brl < 0
   OR custo_total_brl < 0

UNION ALL

SELECT 'stg_occurrences', id_ocorrencia
FROM {{ ref('stg_occurrences') }}
WHERE custo_ocorrencia < 0
