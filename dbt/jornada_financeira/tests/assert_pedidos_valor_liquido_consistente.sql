-- ============================================================
-- TESTE: valor líquido do pedido = valor bruto - desconto
-- Retorna os pedidos que quebram a regra (tolerância de 1 centavo)
-- ============================================================

SELECT
    id_pedido,
    valor_bruto,
    valor_desconto,
    valor_liquido
FROM {{ ref('stg_orders') }}
WHERE ABS(valor_bruto - valor_desconto - valor_liquido) > 0.01
