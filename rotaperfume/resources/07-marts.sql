-- ============================================================================
-- GOLD · 07-marts.sql
-- Um mart por diretoria, todos sobre o MESMO fato.
-- Leem APENAS de gold.fato_vendas.
-- ============================================================================

-- ── mart_vendas_por_vendedor ──────────────────────────────────────────
-- Grão: vendedor × mês. Receita, margem, meta, atingimento,
-- clientes atendidos, ticket médio.
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.mart_vendas_por_vendedor AS
SELECT
    f.vendedor_id,
    ano,
    mes,
    COUNT(DISTINCT pedido_id)                                       AS pedidos_unicos,
    COUNT(DISTINCT cliente_id)                                       AS clientes_atendidos,
    ROUND(SUM(receita), 2)                                           AS receita,
    ROUND(SUM(margem), 2)                                            AS margem,
    ROUND(SUM(receita) - SUM(margem), 2)                             AS custo,
    meta_mensal                                                      AS meta_mensal,
    CASE
        WHEN meta_mensal > 0 THEN ROUND(SUM(receita) / meta_mensal * 100, 2)
        ELSE NULL
    END                                                              AS atingimento_pct,
    ROUND(SUM(receita) / COUNT(DISTINCT cliente_id), 2)              AS ticket_medio
FROM lakehouse_rotaperfume.gold.fato_vendas f
JOIN lakehouse_rotaperfume.gold.dim_vendedor v
    ON f.vendedor_id = v.vendedor_id
GROUP BY f.vendedor_id, ano, mes, meta_mensal;

ALTER TABLE lakehouse_rotaperfume.gold.mart_vendas_por_vendedor
    SET TBLPROPERTIES ('comment' =
        'Mart vendas por vendedor — grão vendedor × mês. Receita, margem, '
        'meta, atingimento, clientes atendidos, ticket médio.');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_vendas_por_vendedor.atingimento_pct IS
    'Receita / meta_mensal * 100. Percentual de cumprimento da meta.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_vendas_por_vendedor.clientes_atendidos IS
    'COUNT(DISTINCT cliente_id) por vendedor × mês.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_vendas_por_vendedor.ticket_medio IS
    'Receita / clientes_atendidos. Ticket médio por cliente.';


-- ── mart_produto_performance ──────────────────────────────────────────
-- Grão: SKU × mês. Receita, margem, margem %, quantidade, curva ABC.
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.mart_produto_performance AS
WITH base AS (
    SELECT
        sku,
        categoria,
        marca,
        ano,
        mes,
        SUM(quantidade_abs) AS quantidade,
        ROUND(SUM(receita), 2) AS receita,
        ROUND(SUM(margem), 2)  AS margem,
        ROUND(SUM(margem) / SUM(receita) * 100, 2) AS margem_pct
    FROM lakehouse_rotaperfume.gold.fato_vendas
    GROUP BY sku, categoria, marca, ano, mes
),
ranking AS (
    SELECT
        *,
        SUM(receita) OVER (PARTITION BY ano, mes ORDER BY receita DESC) AS receita_acumulada,
        SUM(receita) OVER (PARTITION BY ano, mes) AS total_geral,
        ROW_NUMBER() OVER (PARTITION BY ano, mes ORDER BY receita DESC) AS posicao
    FROM base
)
SELECT
    sku,
    categoria,
    marca,
    ano,
    mes,
    quantidade,
    receita,
    margem,
    margem_pct,
    -- Curva ABC por receita acumulada: A = top 80%, B = 80-95%, C = restante
    CASE
        WHEN receita_acumulada / NULLIF(total_geral, 0) <= 0.80 THEN 'A'
        WHEN receita_acumulada / NULLIF(total_geral, 0) <= 0.95 THEN 'B'
        ELSE 'C'
    END AS curva_abc
FROM ranking;

ALTER TABLE lakehouse_rotaperfume.gold.mart_produto_performance
    SET TBLPROPERTIES ('comment' =
        'Mart performance de produto — grão SKU × mês. Receita, margem, '
        'margem %, quantidade e curva ABC por receita acumulada.');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_produto_performance.curva_abc IS
    'Curva ABC: A = top 80% da receita, B = 80-95%, C = 95-100%. '
    'Baseada na receita acumulada por ano/mês.';


-- ── mart_financeiro_recebimento ───────────────────────────────────────
-- Grão: mês de vencimento. Valor a receber, recebido, atraso médio,
-- custo de taxa.
-- Lê de silver.pagamentos (fato_vendas não tem dados financeiros).
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.mart_financeiro_recebimento AS
SELECT
    year(data_vencimento)                                            AS ano,
    month(data_vencimento)                                           AS mes,
    COUNT(DISTINCT pedido_id)                                        AS pedidos,
    COUNT(DISTINCT pagamento_id)                                     AS pagamentos,
    ROUND(SUM(valor), 2)                                             AS valor_a_receber,
    ROUND(SUM(valor_liquido), 2)                                     AS recebido,
    ROUND(AVG(DATEDIFF(CURRENT_DATE(), data_vencimento)), 1)         AS atraso_medio_dias,
    ROUND(SUM(valor * taxa_pct / 100), 2)                            AS custo_taxa
FROM lakehouse_rotaperfume.silver.pagamentos
GROUP BY year(data_vencimento), month(data_vencimento);

ALTER TABLE lakehouse_rotaperfume.gold.mart_financeiro_recebimento
    SET TBLPROPERTIES ('comment' =
        'Mart financeiro de recebimento — grão mês de vencimento. '
        'Valor a receber, recebido, atraso médio e custo de taxa. '
        'Lê de silver.pagamentos.');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_financeiro_recebimento.valor_a_receber IS
    'Soma de valor dos pagamentos no mês de vencimento.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_financeiro_recebimento.recebido IS
    'Soma de valor_liquido efetivamente recebido no mês.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_financeiro_recebimento.atraso_medio_dias IS
    'Média de dias de atraso por pagamento (data atual - data_vencimento).';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_financeiro_recebimento.custo_taxa IS
    'Soma de valor * taxa_pct / 100. Custo das taxas cobradas no mês.';