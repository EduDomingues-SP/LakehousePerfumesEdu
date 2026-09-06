-- ============================================================================
-- ML · 13-fila.sql
-- Fila semanal dos 200 clientes prioritários + 4 funções para o agente + testes.
-- Deploy nº 3 da noite — "A fila e o agente".
-- ============================================================================

-- ============================================================================
-- BLOCO A: gold.fila_semanal
-- ============================================================================

USE SCHEMA lakehouse_rotaperfume.gold;

-- Cria a tabela com COMMENTS nas colunas (obrigatório — sem eles, a auditoria UC quebra o job)
CREATE OR REPLACE TABLE fila_semanal (
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
AS
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
    COALESCE(CONCAT(ss.sku, ' — ', ss.sku_nome, ' (', ss.estoque_info, ')'), 'Sem sugestão disponível no momento') AS sugestao
FROM (
    SELECT
        fb.vendedor,
        fb.ordem,
        fb.cliente_id,
        fb.razao_social,
        fb.cidade,
        fb.uf,
        fb.score,
        fb.faixa,
        fb.ticket_medio,
        fb.atraso_relativo,
        fb.recencia_dias,
        fb.intervalo_medio_dias,
        fb.valor_total,
        fb.comprou_lancamento
    FROM (
        SELECT
            s.score,
            s.faixa,
            s.versao_modelo,
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
            f.comprou_lancamento,
            ROW_NUMBER() OVER (PARTITION BY vendedor_id ORDER BY s.score DESC) AS ordem
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
        ORDER BY s.score DESC
        LIMIT 200
    ) fb
    LEFT JOIN (
        SELECT
            e.cliente_id,
            dp.sku,
            dp.nome AS sku_nome,
            CONCAT('Saldo: ', CAST(e2.saldo AS STRING)) AS estoque_info
        FROM (
            SELECT cliente_id, marca
            FROM (
                SELECT
                    f.cliente_id,
                    f.marca,
                    ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY SUM(f.receita) DESC) AS rn
                FROM lakehouse_rotaperfume.gold.fato_vendas f
                WHERE f.data_pedido >= DATE('2026-08-31') - INTERVAL 90 DAY
                GROUP BY f.cliente_id, f.marca
            )
            WHERE rn = 1
        ) mp
        INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON mp.marca = dp.marca
        LEFT JOIN (
            SELECT DISTINCT cliente_id, sku, data_pedido
            FROM lakehouse_rotaperfume.gold.fato_vendas
            WHERE data_pedido >= DATE('2026-06-02')
        ) ja_comprou ON dp.sku = ja_comprou.sku
        LEFT JOIN (
            SELECT sku, saldo, ruptura
            FROM lakehouse_rotaperfume.silver.estoque
            WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)
        ) e2 ON dp.sku = e2.sku
        WHERE ja_comprou.cliente_id IS NULL
          AND e2.saldo > 0
    ) ss ON fb.cliente_id = ss.cliente_id
) fb
ORDER BY score DESC;

ALTER TABLE fila_semanal
SET TBLPROPERTIES ('delta.autoOptimize.autoCompact'='true', 'delta.autoOptimize.optimizeWrite'='true');


-- ============================================================================
-- BLOCO B: As 4 funções SQL para o agente
-- ============================================================================

-- Função 1: priorizar_carteira
CREATE FUNCTION IF NOT EXISTS priorizar_carteira(
    p_vendedor STRING,
    p_quantos  INT
)
COMMENT 'Retorna a fatia da fila_semanal de um vendedor, ordenada por score. Use para saber quais clientes um vendedor deve contatar primeiro. p_vendedor: nome do vendedor. p_quantos: quantos clientes retornar (top N).'
RETURNS TABLE (
    vendedor     STRING COMMENT 'Nome do vendedor.',
    ordem        INT    COMMENT 'Posição na fila do vendedor (1 = mais prioritário).',
    cliente_id   STRING COMMENT 'ID do cliente.',
    razao_social STRING COMMENT 'Razão social do cliente.',
    cidade       STRING COMMENT 'Cidade do cliente.',
    score        DOUBLE COMMENT 'Score de propensão (0-1).',
    faixa        STRING COMMENT 'Faixa: Fria / Morna / Quente / Muito quente.',
    motivo       STRING COMMENT 'Motivo em português.',
    sugestao     STRING COMMENT 'SKU sugerido para cross-sell.'
)
RETURN SELECT * FROM fila_semanal WHERE vendedor = p_vendedor AND ordem <= p_quantos;


-- Função 2: contexto_cliente
CREATE FUNCTION IF NOT EXISTS contexto_cliente(
    p_cliente_id STRING
)
COMMENT 'Retorna o contexto completo de um cliente: ticket médio, marcas preferidas, última compra e histórico de compras. Use para preparar a ligação antes de contatar o cliente. p_cliente_id: ID do cliente.'
RETURNS TABLE (
    cliente_id         STRING  COMMENT 'ID do cliente.',
    razao_social       STRING  COMMENT 'Nome fantasia / razão social.',
    cidade             STRING  COMMENT 'Cidade.',
    uf                 STRING  COMMENT 'UF.',
    ticket_medio       DOUBLE  COMMENT 'Ticket médio histórico (R$).',
    valor_total        DOUBLE  COMMENT 'Receita total acumulada (R$).',
    marca_pref         STRING  COMMENT 'Marca mais comprada pelo cliente.',
    segunda_marca      STRING  COMMENT 'Segunda marca mais comprada.',
    ultima_compra      DATE    COMMENT 'Data da última compra.',
    pedidos_totais     BIGINT  COMMENT 'Total de pedidos históricos.',
    saldo_score        DOUBLE  COMMENT 'Score de propensão do modelo (0-1).',
    saldo_faixa        STRING  COMMENT 'Faixa de propensão.'
)
RETURN
SELECT
    dc.cliente_id,
    dc.razao_social,
    dc.cidade,
    dc.uf,
    COALESCE(ROUND(fc.ticket_medio, 2), 0) AS ticket_medio,
    COALESCE(ROUND(fc.valor_total, 2), 0) AS valor_total,
    m1.marca AS marca_pref,
    m2.marca AS segunda_marca,
    h.ultima_compra,
    dc.total_pedidos AS pedidos_totais,
    sp.score AS saldo_score,
    sp.faixa AS saldo_faixa
FROM lakehouse_rotaperfume.gold.dim_cliente dc
LEFT JOIN lakehouse_rotaperfume.gold.features_cliente fc ON dc.cliente_id = fc.cliente_id
LEFT JOIN lakehouse_rotaperfume.gold.score_propensao   sp ON dc.cliente_id = sp.cliente_id
LEFT JOIN (
    SELECT f.cliente_id, f.marca
    FROM lakehouse_rotaperfume.gold.fato_vendas f
    GROUP BY f.cliente_id, f.marca
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY SUM(f.receita) DESC) = 1
) m1 ON dc.cliente_id = m1.cliente_id
LEFT JOIN (
    SELECT f.cliente_id, f.marca
    FROM lakehouse_rotaperfume.gold.fato_vendas f
    GROUP BY f.cliente_id, f.marca
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY SUM(f.receita) DESC) = 2
) m2 ON dc.cliente_id = m2.cliente_id
LEFT JOIN (
    SELECT cliente_id, MAX(data_pedido) AS ultima_compra
    FROM lakehouse_rotaperfume.gold.fato_vendas
    GROUP BY cliente_id
    HAVING cliente_id = p_cliente_id
) h ON h.cliente_id = dc.cliente_id
WHERE dc.cliente_id = p_cliente_id;


-- Função 3: sugerir_produtos
CREATE FUNCTION IF NOT EXISTS sugerir_produtos(
    p_cliente_id STRING
)
COMMENT 'Retorna os SKUs que o cliente já comprou mas não comprou nos últimos 90 dias, ordenados por receita. Use para identificar oportunidades de cross-sell e reativação. p_cliente_id: ID do cliente.'
RETURNS TABLE (
    cliente_id     STRING  COMMENT 'ID do cliente.',
    sku            STRING  COMMENT 'Código do SKU.',
    sku_nome       STRING  COMMENT 'Nome / descrição do produto.',
    marca          STRING  COMMENT 'Marca do produto.',
    categoria      STRING  COMMENT 'Categoria do produto.',
    receita_total  DOUBLE  COMMENT 'Receita total que o cliente gerou neste SKU (R$).',
    ult_compra     DATE    COMMENT 'Data da última compra deste SKU.',
    saldo_estoque  INT     COMMENT 'Saldo disponível no snapshot mais recente (0 = ruptura).',
    em_ruptura     BOOLEAN COMMENT 'True se saldo = 0.'
)
RETURN
SELECT
    f.cliente_id,
    dp.sku,
    dp.nome      AS sku_nome,
    dp.marca,
    dp.categoria,
    SUM(f.receita) AS receita_total,
    MAX(f.data_pedido) AS ult_compra,
    COALESCE(e.saldo, 0) AS saldo_estoque,
    COALESCE(e.ruptura, false) AS em_ruptura
FROM lakehouse_rotaperfume.gold.fato_vendas f
INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON f.sku = dp.sku
LEFT JOIN (
    SELECT sku, saldo, ruptura
    FROM lakehouse_rotaperfume.silver.estoque
    WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)
) e ON f.sku = e.sku
WHERE f.cliente_id = p_cliente_id
  AND f.data_pedido < DATE('2026-06-02')
GROUP BY f.cliente_id, dp.sku, dp.nome, dp.marca, dp.categoria, e.saldo, e.ruptura
ORDER BY SUM(f.receita) DESC
LIMIT 20;


-- Função 4: checar_disponibilidade
CREATE FUNCTION IF NOT EXISTS checar_disponibilidade(
    p_sku STRING
)
COMMENT 'Retorna saldo, ruptura e snapshot do SKU. Use antes de oferecer um produto para confirmar que há estoque. p_sku: código do SKU.'
RETURNS TABLE (
    sku             STRING  COMMENT 'Código do SKU.',
    sku_nome        STRING  COMMENT 'Nome do produto.',
    marca           STRING  COMMENT 'Marca do produto.',
    saldo           INT     COMMENT 'Saldo disponível (0 = ruptura).',
    ruptura         BOOLEAN COMMENT 'True se saldo = 0.',
    data_snapshot   DATE    COMMENT 'Data do snapshot do estoque.'
)
RETURN
SELECT
    e.sku,
    dp.nome  AS sku_nome,
    dp.marca,
    e.saldo,
    e.ruptura,
    e.data_snapshot
FROM (
    SELECT sku, saldo, ruptura, data_snapshot
    FROM lakehouse_rotaperfume.silver.estoque
    WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)
) e
INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON e.sku = dp.sku
WHERE e.sku = p_sku;


-- ============================================================================
-- BLOCO C: Três testes de qualidade
-- ============================================================================

-- Teste 1: exatamente 200 linhas
SELECT CASE WHEN (SELECT COUNT(*) FROM fila_semanal) = 200
       THEN 'TESTE 1 OK — 200 linhas na fila'
       ELSE raise_error('TESTE 1 FALHOU: fila_semanal tem '
                        || (SELECT COUNT(*) FROM fila_semanal)
                        || ' linhas (esperado: 200). Verifique filtro de carteira + vendedor ativo.')
       END AS resultado,
       '200 linhas na fila' AS teste;

-- Teste 2: nenhum motivo nulo ou vazio
SELECT CASE WHEN (SELECT COUNT(*) FROM fila_semanal
                  WHERE motivo IS NULL OR motivo = '') = 0
       THEN 'TESTE 2 OK — nenhum motivo vazio'
       ELSE raise_error('TESTE 2 FALHOU: '
                        || (SELECT COUNT(*) FROM fila_semanal
                            WHERE motivo IS NULL OR motivo = '')
                        || ' linhas com motivo nulo ou vazio. Verifique o ELSE do CASE WHEN.')
       END AS resultado,
       'motivo preenchido' AS teste;

-- Teste 3: todos os scores no intervalo [0, 1]
SELECT CASE WHEN NOT EXISTS (
         SELECT 1 FROM fila_semanal
         WHERE score < 0 OR score > 1)
       THEN 'TESTE 3 OK — todos os scores em [0, 1]'
       ELSE raise_error('TESTE 3 FALHOU: scores fora do intervalo [0, 1] em fila_semanal. Verifique se o modelo não está retornando valores inesperados.')
       END AS resultado,
       'score em [0, 1]' AS teste;