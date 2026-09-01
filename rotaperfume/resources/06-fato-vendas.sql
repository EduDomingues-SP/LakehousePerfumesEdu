-- ============================================================================
-- GOLD · 06-fato-vendas.sql
-- O contrato, escrito antes do SQL num comentário no topo.
--
-- GRANULARIDADE: uma linha por ITEM de pedido
-- FILTRO: exclui pedidos cancelados. NÃO exclui devolução.
-- DIMENSÕES: data_pedido, ano, mes, canal, cliente_id, razao_social,
--            segmento, cidade, vendedor_id, sku, categoria, marca,
--            nota_olfativa
-- MÉTRICAS:  quantidade, preco_praticado, receita, custo, margem, devolucao
--   receita = quantidade * preco_praticado
--   custo  = quantidade_abs * custo_unitario do produto
--   margem = receita - custo
--   Devolução entra com quantidade e receita NEGATIVAS, com flag devolucao.
-- PARTICIONAMENTO: por ano e mes.
-- ============================================================================

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.fato_vendas
PARTITIONED BY (ano, mes) AS
SELECT
    -- ── Dimensões ────────────────────────────────────────────────────────
    p.pedido_id,
    i.item_id,
    p.cliente_id,
    c.razao_social,
    c.segmento,
    c.cidade,
    p.data_pedido,
    p.ano,
    p.mes,
    p.canal,
    i.sku,
    pr.categoria,
    pr.marca,
    pr.nota_olfativa,
    p.vendedor_id,

    -- ── Métricas ─────────────────────────────────────────────────────────
    -- Quantidade original: pode ser negativa (devolução)
    i.quantidade                                                AS quantidade,
    i.quantidade_abs                                            AS quantidade_abs,
    i.preco_praticado                                           AS preco_praticado,
    -- Receita = quantidade * preço praticado (pode ser negativa)
    (i.quantidade * i.preco_praticado)                       AS receita,
    -- Custo = quantidade_abs * custo_unitario (sempre positivo)
    (i.quantidade_abs * pr.custo_unitario)                   AS custo,
    -- Margem = receita - custo
    ((i.quantidade * i.preco_praticado) -
     (i.quantidade_abs * pr.custo_unitario))                 AS margem,
    i.devolucao                                                 AS devolucao,
    i.sku_descontinuado                                         AS sku_descontinuado

FROM lakehouse_rotaperfume.silver.pedidos      p
LEFT JOIN lakehouse_rotaperfume.silver.clientes     c  ON p.cliente_id   = c.cliente_id
JOIN     lakehouse_rotaperfume.silver.itens_pedido i  ON p.pedido_id    = i.pedido_id
JOIN     lakehouse_rotaperfume.silver.produtos     pr ON i.sku          = pr.sku
WHERE NOT p.cancelado
  AND i.quantidade      IS NOT NULL
  AND i.preco_praticado IS NOT NULL
  AND pr.custo_unitario IS NOT NULL;

-- ── COMMENT — contrato da tabela fato_vendas ───────────────────────────
ALTER TABLE lakehouse_rotaperfume.gold.fato_vendas
    SET TBLPROPERTIES ('comment' =
        'Fato de vendas — grão de ITEM DE PEDIDO. Exclui pedidos cancelados '
        'mas mantém devoluções (com valores negativos e flag devolucao=true). '
        'Particionado por ano/mes. FILTRO: cancelado=false.');

-- ── COMMENT em TODAS as colunas de negócio ──────────────────────────────
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.quantidade IS
    'Quantidade do item. Pode ser NEGATIVA para devolução. Usar quantidade_abs para somas.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.receita IS
    'Quantidade * preco_praticado. Negativa quando devolucao=true. Não desconta devolução.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.custo IS
    'quantidade_abs * custo_unitario do produto. Sempre positivo. Custo do item vendido/devolvido.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.margem IS
    'Receita menos custo do produto. Não considera desconto comercial nem frete.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.devolucao IS
    'Boolean: true quando a quantidade do item era negativa (devolução). Devolução NÃO é excluída.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.sku_descontinuado IS
    'true quando o SKU foi descontinuado no momento do pedido. Não conserta — expõe.';