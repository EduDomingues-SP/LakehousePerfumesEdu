-- ============================================================================
-- ML · 13-fila-v2.sql  (versão 2 — renomeado para evitar cache do warehouse)
-- Fila semanal dos 200 clientes prioritários + 4 funções para o agente + testes.
-- ============================================================================

-- BLOCO A: gold.fila_semanal
-- Estratégia: DROP + CREATE TABLE + INSERT (RTAS não funciona com UC + schema)

DROP TABLE IF EXISTS lakehouse_rotaperfume.gold.fila_semanal;

CREATE TABLE lakehouse_rotaperfume.gold.fila_semanal (
    vendedor STRING COMMENT 'Nome do vendedor que deve fazer a ligação.',
    ordem INT COMMENT 'Posição na fila do vendedor (1 = cliente mais prioritário).',
    cliente_id STRING COMMENT 'ID do cliente — usar nas funções de contexto.',
    razao_social STRING COMMENT 'Razão social do cliente.',
    cidade STRING COMMENT 'Cidade do cliente.',
    uf STRING COMMENT 'UF do cliente.',
    score DOUBLE COMMENT 'Score de propensão (0-1). Quanto maior, maior a chance de compra em 7 dias.',
    faixa STRING COMMENT 'Faixa de propensão: Fria / Morna / Quente / Muito quente (quartis do score).',
    ticket_medio DOUBLE COMMENT 'Ticket médio histórico do cliente (R$).',
    motivo STRING COMMENT 'Motivo em português para o vendedor entender por que este cliente está no topo.',
    sugestao STRING COMMENT 'SKU para cross-sell: produto da marca preferida do cliente que ele não comprou recentemente.'
)
COMMENT 'Fila semanal dos 200 clientes prioritários para ligação, distribuída por vendedor. Score do modelo de propensão (0-1) + motivo em português + SKU sugerido para cross-sell. Referência: 2026-08-31.'
TBLPROPERTIES ('delta.autoOptimize.autoCompact'='true', 'delta.autoOptimize.optimizeWrite'='true');

INSERT INTO lakehouse_rotaperfume.gold.fila_semanal
WITH
-- Base: todos os elegíveis (carteira vigente + vendedor ativo)
elegiveis AS (
    SELECT
        s.cliente_id,
        s.score,
        s.faixa,
        c.vendedor_id,
        v.nome AS vendedor,
        d.razao_social,
        d.cidade,
        d.uf,
        f.ticket_medio,
        f.atraso_relativo,
        f.recencia_dias,
        f.intervalo_medio_dias,
        f.valor_total,
        f.comprou_lancamento
    FROM lakehouse_rotaperfume.gold.score_propensao s
    INNER JOIN lakehouse_rotaperfume.silver.carteira c
        ON s.cliente_id = c.cliente_id
       AND c.vigente = true
       AND c.orfao_vendedor_desligado = false
    INNER JOIN lakehouse_rotaperfume.silver.vendedores v
        ON c.vendedor_id = v.vendedor_id
       AND v.ativo = true
    INNER JOIN lakehouse_rotaperfume.gold.dim_cliente d
        ON s.cliente_id = d.cliente_id
    INNER JOIN lakehouse_rotaperfume.gold.features_cliente f
        ON s.cliente_id = f.cliente_id
),
-- Top 200 por score (single source of truth para o LIMIT)
top200 AS (
    SELECT cliente_id FROM elegiveis
    ORDER BY score DESC
    LIMIT 200
),
fila_base AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.vendedor_id ORDER BY e.score DESC) AS ordem
    FROM elegiveis e
    INNER JOIN top200 t ON e.cliente_id = t.cliente_id
),
-- Marca preferida dos TOP 200 (limitada ao conjunto relevante)
-- SUM dentro de ROW_NUMBER — usa subquery para evitar QUALIFY com agregação
marca_pref AS (
    SELECT cliente_id, marca
    FROM (
        SELECT
            f.cliente_id,
            f.marca,
            ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY SUM(f.receita) DESC) AS rn
        FROM lakehouse_rotaperfume.gold.fato_vendas f
        INNER JOIN top200 t ON f.cliente_id = t.cliente_id
        WHERE f.data_pedido >= DATE('2026-08-31') - INTERVAL 90 DAY
        GROUP BY f.cliente_id, f.marca
    ) ranked
    WHERE rn = 1
),
-- SKU sugerido: marca preferida, não comprou nos últimos 90d, tem estoque
sugestao AS (
    SELECT
        mp.cliente_id,
        dp.sku,
        dp.nome AS sku_nome,
        CONCAT('Saldo: ', CAST(er.saldo AS STRING)) AS estoque_info
    FROM marca_pref mp
    INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON mp.marca = dp.marca
    LEFT JOIN (
        SELECT DISTINCT cliente_id, sku
        FROM lakehouse_rotaperfume.gold.fato_vendas
        WHERE data_pedido >= DATE('2026-06-02')
    ) ja_comprou ON dp.sku = ja_comprou.sku AND mp.cliente_id = ja_comprou.cliente_id
    LEFT JOIN (
        SELECT sku, saldo
        FROM lakehouse_rotaperfume.silver.estoque
        WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)
    ) er ON dp.sku = er.sku
    WHERE ja_comprou.sku IS NULL
      AND er.saldo > 0
)
SELECT
    fb.vendedor,
    fb.ordem,
    fb.cliente_id,
    fb.razao_social,
    fb.cidade,
    fb.uf,
    ROUND(fb.score, 4) AS score,
    fb.faixa,
    ROUND(fb.ticket_medio, 2) AS ticket_medio,
    CASE
        WHEN fb.atraso_relativo > 3
            THEN CONCAT('Compra a cada ', FORMAT_NUMBER(fb.intervalo_medio_dias, 0), ' dias e está há ', FORMAT_NUMBER(fb.recencia_dias, 0), ' sem pedido. Risco de perder para o concorrente.')
        WHEN fb.atraso_relativo > 1.5
            THEN CONCAT('Está ', FORMAT_NUMBER(fb.atraso_relativo, 1), 'x mais atrasado que o ritmo dele.')
        WHEN fb.comprou_lancamento = 1
            THEN 'Comprou lançamento recente. Alta chance de repetir.'
        WHEN fb.valor_total >= (SELECT APPROX_PERCENTILE(valor_total, 0.75) FROM lakehouse_rotaperfume.gold.features_cliente)
            THEN CONCAT('Cliente grande, R$ ', FORMAT_NUMBER(fb.valor_total, 2), ' no ano. Manter próximo.')
        ELSE 'Dentro do ritmo. Contato de manutenção.'
    END AS motivo,
    COALESCE(CONCAT(sg.sku, ' — ', sg.sku_nome, ' (', sg.estoque_info, ')'), 'Sem sugestão disponível no momento') AS sugestao
FROM fila_base fb
LEFT JOIN sugestao sg ON fb.cliente_id = sg.cliente_id
ORDER BY fb.score DESC
LIMIT 200;


-- BLOCO B: As 4 funções SQL para o agente

CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.priorizar_carteira(
    p_vendedor STRING,
    p_quantos  INT
)
RETURNS TABLE (
    vendedor STRING COMMENT 'Nome do vendedor.',
    ordem INT COMMENT 'Posição.',
    cliente_id STRING COMMENT 'ID.',
    razao_social STRING COMMENT 'Razão social.',
    cidade STRING COMMENT 'Cidade.',
    uf STRING COMMENT 'UF.',
    score DOUBLE COMMENT 'Score (0-1).',
    faixa STRING COMMENT 'Faixa.',
    ticket_medio DOUBLE COMMENT 'Ticket médio (R$).',
    motivo STRING COMMENT 'Motivo.',
    sugestao STRING COMMENT 'SKU sugerido.'
)
COMMENT 'Retorna a fatia da fila_semanal de um vendedor, ordenada por score. Use para saber quais clientes um vendedor deve contatar primeiro.'
RETURN SELECT * FROM lakehouse_rotaperfume.gold.fila_semanal WHERE vendedor = p_vendedor AND ordem <= p_quantos;

CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.contexto_cliente(
    p_cliente_id STRING
)
RETURNS TABLE (
    cliente_id STRING COMMENT 'ID.',
    razao_social STRING COMMENT 'Razão social.',
    cidade STRING COMMENT 'Cidade.',
    uf STRING COMMENT 'UF.',
    ticket_medio DOUBLE COMMENT 'Ticket médio (R$).',
    valor_total DOUBLE COMMENT 'Receita total (R$).',
    marca_pref STRING COMMENT 'Marca preferida.',
    segunda_marca STRING COMMENT 'Segunda marca.',
    ultima_compra DATE COMMENT 'Última compra.',
    pedidos_totais BIGINT COMMENT 'Total pedidos.',
    saldo_score DOUBLE COMMENT 'Score.',
    saldo_faixa STRING COMMENT 'Faixa.'
)
COMMENT 'Retorna contexto completo de um cliente: ticket médio, marcas preferidas, última compra e histórico.'
RETURN
SELECT
    dc.cliente_id, dc.razao_social, dc.cidade, dc.uf,
    COALESCE(ROUND(fc.ticket_medio, 2), 0) AS ticket_medio,
    COALESCE(ROUND(fc.valor_total, 2), 0) AS valor_total,
    m1.marca AS marca_pref, m2.marca AS segunda_marca,
    h.ultima_compra, dc.total_pedidos AS pedidos_totais,
    sp.score AS saldo_score, sp.faixa AS saldo_faixa
FROM lakehouse_rotaperfume.gold.dim_cliente dc
LEFT JOIN lakehouse_rotaperfume.gold.features_cliente fc ON dc.cliente_id = fc.cliente_id
LEFT JOIN lakehouse_rotaperfume.gold.score_propensao sp ON dc.cliente_id = sp.cliente_id
LEFT JOIN (
    SELECT cliente_id, marca
    FROM (
        SELECT f.cliente_id, f.marca,
               ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY SUM(f.receita) DESC) AS rn
        FROM lakehouse_rotaperfume.gold.fato_vendas f
        GROUP BY f.cliente_id, f.marca
    ) ranked
    WHERE rn = 1
) m1 ON dc.cliente_id = m1.cliente_id
LEFT JOIN (
    SELECT cliente_id, marca
    FROM (
        SELECT f.cliente_id, f.marca,
               ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY SUM(f.receita) DESC) AS rn
        FROM lakehouse_rotaperfume.gold.fato_vendas f
        GROUP BY f.cliente_id, f.marca
    ) ranked
    WHERE rn = 2
) m2 ON dc.cliente_id = m2.cliente_id
LEFT JOIN (SELECT cliente_id, MAX(data_pedido) AS ultima_compra FROM lakehouse_rotaperfume.gold.fato_vendas GROUP BY cliente_id) h ON h.cliente_id = dc.cliente_id
WHERE dc.cliente_id = p_cliente_id;

CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.sugerir_produtos(
    p_cliente_id STRING
)
RETURNS TABLE (
    cliente_id STRING COMMENT 'ID.',
    sku STRING COMMENT 'SKU.',
    sku_nome STRING COMMENT 'Nome.',
    marca STRING COMMENT 'Marca.',
    categoria STRING COMMENT 'Categoria.',
    receita_total DOUBLE COMMENT 'Receita (R$).',
    ult_compra DATE COMMENT 'Última compra.',
    saldo_estoque INT COMMENT 'Saldo.',
    em_ruptura BOOLEAN COMMENT 'Ruptura.'
)
COMMENT 'SKUs que o cliente comprou mas não nos últimos 90 dias, ordenados por receita.'
RETURN
SELECT f.cliente_id, dp.sku, dp.nome AS sku_nome, dp.marca, dp.categoria,
    SUM(f.receita) AS receita_total, MAX(f.data_pedido) AS ult_compra,
    COALESCE(e.saldo, 0) AS saldo_estoque, COALESCE(e.ruptura, false) AS em_ruptura
FROM lakehouse_rotaperfume.gold.fato_vendas f
INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON f.sku = dp.sku
LEFT JOIN (SELECT sku, saldo, ruptura FROM lakehouse_rotaperfume.silver.estoque WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)) e ON f.sku = e.sku
WHERE f.cliente_id = p_cliente_id AND f.data_pedido < DATE('2026-06-02')
GROUP BY f.cliente_id, dp.sku, dp.nome, dp.marca, dp.categoria, e.saldo, e.ruptura
ORDER BY SUM(f.receita) DESC LIMIT 20;

CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.checar_disponibilidade(
    p_sku STRING
)
RETURNS TABLE (
    sku STRING COMMENT 'SKU.',
    sku_nome STRING COMMENT 'Nome.',
    marca STRING COMMENT 'Marca.',
    saldo INT COMMENT 'Saldo.',
    ruptura BOOLEAN COMMENT 'Ruptura.',
    data_snapshot DATE COMMENT 'Snapshot.'
)
COMMENT 'Retorna saldo e ruptura do SKU no snapshot mais recente.'
RETURN
SELECT e.sku, dp.nome AS sku_nome, dp.marca, e.saldo, e.ruptura, e.data_snapshot
FROM (SELECT sku, saldo, ruptura, data_snapshot FROM lakehouse_rotaperfume.silver.estoque WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)) e
INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON e.sku = dp.sku
WHERE e.sku = p_sku;


-- BLOCO C: Três testes de qualidade

SELECT CASE WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal) = 200
THEN 'TESTE 1 OK — 200 linhas na fila'
ELSE raise_error('TESTE 1 FALHOU: fila_semanal tem ' || (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal) || ' linhas (esperado: 200).')
END AS resultado, '200 linhas na fila' AS teste;

SELECT CASE WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal WHERE motivo IS NULL OR motivo = '') = 0
THEN 'TESTE 2 OK — nenhum motivo vazio'
ELSE raise_error('TESTE 2 FALHOU: ' || (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal WHERE motivo IS NULL OR motivo = '') || ' motivos vazios.')
END AS resultado, 'motivo preenchido' AS teste;

SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM lakehouse_rotaperfume.gold.fila_semanal WHERE score < 0 OR score > 1)
THEN 'TESTE 3 OK — todos os scores em [0, 1]'
ELSE raise_error('TESTE 3 FALHOU: scores fora do intervalo [0, 1] em fila_semanal.')
END AS resultado, 'score em [0, 1]' AS teste;