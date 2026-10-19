-- ============================================================
-- TESTE: custo total do frete = frete + seguro
-- Retorna os fretes que quebram a regra (tolerância de 1 centavo)
-- ============================================================

SELECT
    id_frete,
    custo_frete_brl,
    seguro_brl,
    custo_total_brl
FROM {{ ref('stg_freight_costs') }}
WHERE ABS(custo_frete_brl + seguro_brl - custo_total_brl) > 0.01
