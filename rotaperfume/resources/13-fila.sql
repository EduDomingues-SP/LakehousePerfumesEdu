-- ============================================================================
-- ML · 13-fila.sql
-- Fila semanal dos 200 clientes prioritários + 4 funções para o agente + testes.
-- Deploy nº 3 da noite — "A fila e o agente".
-- ============================================================================

-- Snapshot mais recente do estoque (semana do treinamento)
-- snapshot: 2026-08-30
-- Referência para filtro de 90 dias: data de referência do score (2026-08-31)
-- subtrai 90 dias → 2026-06-02
-- ============================================================================

-- ── BLOCO A: gold.fila_semanal ────────────────────────────────────────────────
-- COMMENT na tabela (obrigatório — sem ele a auditoria UC quebra o job).
-- COMMENT em cada coluna (é o que o Genie lê para responder sem inventar).
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.fila_semanal
COMMENT 'Fila semanal dos 200 clientes prioritários para ligação, distribuída por vendedor. Score do modelo de propensão (0-1) + motivo em português + SKU sugerido para cross-sell. Referência: 2026-08-31.'
AS
WITH
-- CTE 1: snapshot do estoque mais recente
estoque_recente AS (
    SELECT sku, saldo, ruptura
    FROM lakehouse_rotaperfume.silver.estoque
    WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)
),

-- CTE 2: scores com elegibilidade de carteira e vendedor
--
--  A ORDEM DAS OPERAÇÕES IMPORTA — erro mais fácil de cometer:
--  1º  JOIN silver.carteira E DESCARTE elegíveis (vigente=true, orfao=false)
--  2º  ORDER BY score DESC LIMIT 200           ← SEMPRE antes da partição
--  3º  ROW_NUMBER() OVER (PARTITION BY vendedor ORDER BY score DESC)
--
--  Se o descarte vier DEPOIS do LIMIT, a fila sai com ~172 linhas em vez
--  de 200 — seis vendedores estão desligados e levam junto os clientes deles.
elegiveis AS (
    SELECT
        s.cliente_id,
        s.score,
        s.faixa,
        s.versao_modelo,
        c.vendedor_id,
        v.nome    AS vendedor,
        d.razao_social,
        d.cidade,
        d.uf,
        f.ticket_medio,
        -- atraso_relativo é a feature mais importante do modelo
        f.atraso_relativo,
        f.recencia_dias,
        f.intervalo_medio_dias,
        f.valor_total,
        f.comprou_lancamento
    FROM lakehouse_rotaperfume.gold.score_propensao s

    -- Só clientes com carteira vigente
    INNER JOIN lakehouse_rotaperfume.silver.carteira c
        ON s.cliente_id = c.cliente_id
       AND c.vigente = true
       AND c.orfao_vendedor_desligado = false

    -- Só vendedores ativos (desligados não recebem ligação)
    INNER JOIN lakehouse_rotaperfume.silver.vendedores v
        ON c.vendedor_id = v.vendedor_id
       AND v.ativo = true

    -- Dados do cliente
    INNER JOIN lakehouse_rotaperfume.gold.dim_cliente d
        ON s.cliente_id = d.cliente_id

    -- Features para montar o motivo e ticket médio
    INNER JOIN lakehouse_rotaperfume.gold.features_cliente f
        ON s.cliente_id = f.cliente_id
),

-- CTE 3: top 200 por score (ANTES da partição por vendedor)
top200 AS (
    SELECT *
    FROM elegiveis
    ORDER BY score DESC
    LIMIT 200
),

-- CTE 4: partição por vendedor com numeração
fila_base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vendedor_id
            ORDER BY score DESC
        ) AS ordem
    FROM top200
),

-- CTE 5: marca preferida do cliente (mais receita)
marca_pref AS (
    SELECT
        f.cliente_id,
        f.marca
    FROM lakehouse_rotaperfume.gold.fato_vendas f
    INNER JOIN elegiveis e ON f.cliente_id = e.cliente_id
    GROUP BY f.cliente_id, f.marca
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY f.cliente_id
        ORDER BY SUM(f.receita) DESC
    ) = 1
),

-- CTE 6: SKU top da marca preferida que o cliente NÃO comprou nos últimos 90d
-- (referência 2026-08-31, 90 dias antes = 2026-06-02)
sugestao_sku AS (
    SELECT
        e.cliente_id,
        dp.sku,
        dp.nome        AS sku_nome,
        e2.saldo,
        CASE WHEN e2.ruptura THEN 'RUPTURA' ELSE CONCAT('Saldo: ', CAST(e2.saldo AS STRING)) END AS estoque_info
    FROM elegiveis e
    INNER JOIN marca_pref mp ON e.cliente_id = mp.cliente_id
    INNER JOIN lakehouse_rotaperfume.gold.dim_produto dp ON mp.marca = dp.marca
    -- Exclui SKUs que o cliente comprou nos últimos 90 dias
    LEFT JOIN (
        SELECT DISTINCT cliente_id, sku
        FROM lakehouse_rotaperfume.gold.fato_vendas
        WHERE data_pedido >= DATE('2026-06-02')
    ) ja_comprou
        ON dp.sku = ja_comprou.sku AND e.cliente_id = ja_comprou.cliente_id
    LEFT JOIN estoque_recente e2 ON dp.sku = e2.sku
    WHERE ja_comprou.cliente_id IS NULL   -- não comprou nos últimos 90d
      AND e2.saldo > 0                    -- tem estoque disponível
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.cliente_id
        ORDER BY e2.saldo DESC NULLS LAST   -- prioriza SKUs com mais saldo
    ) = 1
)
SELECT
    fb.vendedor                                           AS vendedor          COMMENT 'Nome do vendedor que deve fazer a ligação.',
    fb.ordem                                              AS ordem             COMMENT 'Posição na fila do vendedor (1 = cliente mais prioritário).',
    fb.cliente_id                                         AS cliente_id        COMMENT 'ID do cliente — usar nas funções de contexto.',
    fb.razao_social                                      AS razao_social      COMMENT 'Razão social do cliente.',
    fb.cidade                                             AS cidade            COMMENT 'Cidade do cliente.',
    fb.uf                                                 AS uf                COMMENT 'UF do cliente.',
    ROUND(fb.score, 4)                                   AS score             COMMENT 'Score de propensão (0-1). Quanto maior, maior a chance de compra em 7 dias.',
    fb.faixa                                              AS faixa             COMMENT 'Faixa de propensão: Fria / Morna / Quente / Muito quente (quartis do score).',
    ROUND(fb.ticket_medio, 2)                            AS ticket_medio      COMMENT 'Ticket médio histórico do cliente (R$).',

    -- MOTIVO: CASE WHEN encadeado do mais RARO para o mais comum.
    -- Se o mais comum vier primeiro, ele come todos os outros.
    CASE
        WHEN fb.atraso_relativo > 3
            THEN CONCAT(
                'Compra a cada ', FORMAT_NUMBER(fb.intervalo_medio_dias, 0),
                ' dias e está há ', FORMAT_NUMBER(fb.recencia_dias, 0),
                ' sem pedido. Risco de perder para o concorrente.'
            )
        WHEN fb.atraso_relativo > 1.5
            THEN CONCAT(
                'Está ', FORMAT_NUMBER(fb.atraso_relativo, 1),
                'x mais atrasado que o ritmo dele.'
            )
        WHEN fb.comprou_lancamento = 1
            THEN 'Comprou lançamento recente. Alta chance de repetir.'
        WHEN fb.valor_total >= (
            SELECT APPROX_PERCENTILE(valor_total, 0.75)
            FROM lakehouse_rotaperfume.gold.features_cliente
        )
            THEN CONCAT(
                'Cliente grande, R$ ', FORMAT_NUMBER(fb.valor_total, 2),
                ' no ano. Manter próximo.'
            )
        ELSE 'Dentro do ritmo. Contato de manutenção.'
    END                                                   AS motivo            COMMENT 'Motivo em português para o vendedor entender por que este cliente está no topo.',

    -- SUGESTÃO: SKU mais comprado na marca preferida que o cliente
    -- NÃO comprou nos últimos 90 dias + saldo disponível.
    COALESCE(
        CONCAT(ss.sku, ' — ', ss.sku_nome, ' (', ss.estoque_info, ')'),
        'Sem sugestão disponível no momento'
    )                                                      AS sugestao          COMMENT 'SKU para cross-sell: produto da marca preferida do cliente que ele não comprou recentemente e tem saldo.'

FROM fila_base fb
LEFT JOIN sugestao_sku ss ON fb.cliente_id = ss.cliente_id
ORDER BY fb.score DESC;


-- ── BLOCO B: As 4 funções SQL para o agente ────────────────────────────────
-- Todas com COMMENT em português — é o que o agente lê para saber quando usar.
-- Parâmetros prefixados com p_ para evitar ambiguidade com nomes de colunas.

-- Função 1: priorizar_carteira
-- Retorna a fatia da fila_semanal de um vendedor, em ordem.
CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.priorizar_carteira(
    p_vendedor STRING,
    p_quantos  INT
)
COMMENT 'Retorna a fila_semanal de um vendedor específico, ordenada por score. Use para saber quais clientes um vendedor deve contatar primeiro. p_vendedor: nome do vendedor. p_quantos: quantos clientes retornar (top N).'
RETURNS TABLE (
    vendedor     STRING  COMMENT 'Nome do vendedor.',
    ordem        INT     COMMENT 'Posição na fila do vendedor (1 = mais prioritário).',
    cliente_id   STRING  COMMENT 'ID do cliente.',
    razao_social STRING  COMMENT 'Razão social do cliente.',
    cidade       STRING  COMMENT 'Cidade do cliente.',
    score        DOUBLE  COMMENT 'Score de propensão (0-1).',
    faixa        STRING  COMMENT 'Faixa: Fria / Morna / Quente / Muito quente.',
    motivo       STRING  COMMENT 'Motivo em português.',
    sugestao     STRING  COMMENT 'SKU sugerido para cross-sell.'
)
RETURN
SELECT vendedor, ordem, cliente_id, razao_social, cidade, score, faixa, motivo, sugestao
FROM lakehouse_rotaperfume.gold.fila_semanal
WHERE vendedor = p_vendedor
  AND ordem <= p_quantos   -- usa <= em vez de LIMIT p_quantos (Databricks exige LIMIT constante em função);


-- Função 2: contexto_cliente
-- Histórico, ticket médio, marcas preferidas e última compra.
CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.contexto_cliente(
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
    ROUND(fc.ticket_medio, 2)    AS ticket_medio,
    ROUND(fc.valor_total, 2)     AS valor_total,
    m.marca_pref,
    m.segunda_marca,
    h.ultima_compra,
    dc.total_pedidos              AS pedidos_totais,
    sp.score                      AS saldo_score,
    sp.faixa                      AS saldo_faixa
FROM lakehouse_rotaperfume.gold.dim_cliente dc
LEFT JOIN lakehouse_rotaperfume.gold.features_cliente fc ON dc.cliente_id = fc.cliente_id
LEFT JOIN lakehouse_rotaperfume.gold.score_propensao   sp ON dc.cliente_id = sp.cliente_id
LEFT JOIN (
    SELECT cliente_id, marca AS marca_pref
    FROM lakehouse_rotaperfume.gold.fato_vendas
    GROUP BY cliente_id, marca
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY SUM(receita) DESC) = 1
) m1 ON dc.cliente_id = m1.cliente_id
LEFT JOIN (
    SELECT cliente_id, marca AS segunda_marca
    FROM lakehouse_rotaperfume.gold.fato_vendas
    GROUP BY cliente_id, marca
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY SUM(receita) DESC) = 2
) m2 ON dc.cliente_id = m2.cliente_id
LEFT JOIN LATERAL (
    SELECT MAX(data_pedido) AS ultima_compra
    FROM lakehouse_rotaperfume.gold.fato_vendas
    WHERE cliente_id = dc.cliente_id
) h
WHERE dc.cliente_id = p_cliente_id;


-- Função 3: sugerir_produtos
-- O que o cliente compra e parou de comprar nos últimos 90 dias.
CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.sugerir_produtos(
    p_cliente_id STRING
)
COMMENT 'Retorna os SKUs que o cliente já comprou mas não comprou nos últimos 90 dias, ordenados por receita. Use para identificar oportunidades de cross-sell e reativação. p_cliente_id: ID do cliente.'
RETURNS TABLE (
    cliente_id     STRING  COMMENT 'ID do cliente.',
    sku            STRING  COMMENT 'Código do SKU.',
    sku_nome       STRING  COMMENT 'Nome/descrição do produto.',
    marca          STRING  COMMENT 'Marca do produto.',
    categoria      STRING  COMMENT 'Categoria do produto.',
    receita_total  DOUBLE  COMMENT 'Receita total que o cliente gerou neste SKU (R$).',
    ult_compra     DATE    COMMENT 'Data da última compra deste SKU.',
    saldo_estoque  INT     COMMENT 'Saldo disponível no snapshot mais recente (0 = ruptura).',
    em_ruptura     BOOLEAN COMMENT 'True se saldo = 0 (produto em ruptura).'
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
  -- Comprou o SKU mas NÃO comprou nos últimos 90 dias
  AND f.data_pedido < DATE('2026-06-02')
GROUP BY f.cliente_id, dp.sku, dp.nome, dp.marca, dp.categoria, e.saldo, e.ruptura
ORDER BY SUM(f.receita) DESC
LIMIT 20;


-- Função 4: checar_disponibilidade
-- Saldo e ruptura do SKU no snapshot semanal mais recente.
CREATE FUNCTION IF NOT EXISTS lakehouse_rotaperfume.gold.checar_disponibilidade(
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


-- ── BLOCO C: Três testes de qualidade ───────────────────────────────────────
-- Padrão CASE WHEN ... THEN 'OK' ELSE raise_error(...) END
-- Qualquer valor fora da tolerância interrompe a tarefa.

-- Teste 1: exatamente 200 linhas
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal) = 200
            THEN 'TESTE 1 OK — 200 linhas na fila'
        ELSE raise_error(
            'TESTE 1 FALHOU: fila_semanal tem '
            || (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal)
            || ' linhas (esperado: 200). '
            || 'Verifique se o filtro de carteira + vendedor ativo está antes do LIMIT 200.'
        )
    END AS resultado,
    '200 linhas na fila' AS teste;

-- Teste 2: nenhum motivo nulo ou vazio
SELECT
    CASE
        WHEN (
            SELECT COUNT(*)
            FROM lakehouse_rotaperfume.gold.fila_semanal
            WHERE motivo IS NULL OR motivo = ''
        ) = 0
            THEN 'TESTE 2 OK — nenhum motivo vazio'
        ELSE raise_error(
            'TESTE 2 FALHOU: '
            || (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal
                WHERE motivo IS NULL OR motivo = '')
            || ' linhas com motivo nulo ou vazio. '
            || 'Verifique o ELSE do CASE WHEN do motivo.'
        )
    END AS resultado,
    'motivo preenchido' AS teste;

-- Teste 3: todos os scores no intervalo [0, 1]
SELECT
    CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM lakehouse_rotaperfume.gold.fila_semanal
            WHERE score < 0 OR score > 1
        )
            THEN 'TESTE 3 OK — todos os scores em [0, 1]'
        ELSE raise_error(
            'TESTE 3 FALHOU: scores fora do intervalo [0, 1] em fila_semanal. '
            || 'Verifique se o modelo não está retornando valores inesperados.'
        )
    END AS resultado,
    'score em [0, 1]' AS teste;
