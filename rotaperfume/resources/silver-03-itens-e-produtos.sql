-- ============================================================================
-- SILVER · 03-itens-e-produtos.sql
-- Produtos tipados + itens de pedido com marcação de devolução e SKU descontinuado.
-- NÃO descarta devolução — marca e sinaliza.
-- Bronze.produtos:  sku, descricao, categoria, marca, nota_olfativa,
--                   preco_tabela, custo_unitario, unidade, ativo, data_lancamento
-- Bronze.itens_pedido: item_id, pedido_id, sku, quantidade, preco_praticado,
--                       desconto_pct, valor_bruto
-- ============================================================================

-- ── PRODUTOS ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.produtos AS
SELECT
    trim(sku)                                                     AS sku,
    trim(descricao)                                               AS nome,
    trim(categoria)                                               AS categoria,
    trim(marca)                                                   AS marca,
    trim(nota_olfativa)                                           AS nota_olfativa,
    -- Data em dois formatos
    coalesce(
        try_to_date(data_lancamento, 'yyyy-MM-dd'),
        try_to_date(data_lancamento, 'dd/MM/yyyy')
    )                                                             AS data_lancamento,
    -- Valores decimais
    try_cast(preco_tabela    AS DECIMAL(18,2))                   AS preco_tabela,
    try_cast(custo_unitario  AS DECIMAL(18,2))                   AS custo_unitario,
    trim(unidade)                                                 AS unidade,
    -- Ativo como boolean
    CASE WHEN trim(ativo) = 'S' THEN true ELSE false END        AS ativo,
    -- Auditoria
    current_timestamp()                                      AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.produtos) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.produtos;

ALTER TABLE lakehouse_rotaperfume.silver.produtos
    SET TBLPROPERTIES ('comment' =
        'Catalogo de produtos do ERP — tipos corrigidos, ativo como boolean, '
        'preco_tabela e custo_unitario em decimal.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.produtos.nome IS
    'Descricao do produto. Usar para exibicao, nao para join.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.produtos.data_lancamento IS
    'Coalesce de try_to_date(ISO) e try_to_date(BR).';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.produtos.ativo IS
    'Boolean: true=S, false=N.';

-- ── ITENS DO PEDIDO ────────────────────────────────────────────────────────
-- Quantidade negativa NAO e erro — e devolucao. try_cast safe.
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.itens_pedido AS
WITH itens_bronze AS (
    SELECT
        trim(item_id)                                                     AS item_id,
        trim(pedido_id)                                                  AS pedido_id,
        trim(sku)                                                        AS sku,
        -- try_cast devolve NULL em vez de falhar
        try_cast(quantidade AS INT)                                       AS quantidade,
        -- Valores decimais safe
        try_cast(preco_praticado AS DECIMAL(18,2))                       AS preco_praticado,
        try_cast(desconto_pct  AS DECIMAL(18,2))                        AS desconto_pct,
        try_cast(valor_bruto   AS DECIMAL(18,2))                        AS valor_bruto,
        _arquivo_origem,
        _ingerido_em
    FROM lakehouse_rotaperfume.bronze.itens_pedido
),

com_sku AS (
    SELECT
        i.item_id,
        i.pedido_id,
        i.sku,
        i.quantidade,
        -- Devolucao: quantidade negativa. NAO e erro — e marcacao.
        i.quantidade < 0                                                 AS devolucao,
        -- Quantidade absoluta para somas (nunca descartar a linha)
        CASE WHEN i.quantidade < 0
             THEN abs(i.quantidade)
             ELSE i.quantidade
        END                                                               AS quantidade_abs,
        i.preco_praticado,
        i.desconto_pct,
        i.valor_bruto,
        -- Custo unitario do produto (para calcular custo na gold)
        p.custo_unitario,
        -- Verifica se o SKU foi descontinuado: join por sku
        CASE
            WHEN p.sku IS NOT NULL AND p.ativo = false THEN true
            ELSE false
        END                                                               AS sku_descontinuado,
        i._arquivo_origem,
        i._ingerido_em
    FROM itens_bronze i
    LEFT JOIN lakehouse_rotaperfume.silver.produtos p
        ON i.sku = p.sku
)
SELECT
    item_id,
    pedido_id,
    sku,
    quantidade,       -- original: pode ser negativa (devolucao)
    quantidade_abs,   -- sempre positiva: para somas
    devolucao,        -- true quando quantidade < 0
    preco_praticado,
    desconto_pct,
    valor_bruto,
    custo_unitario,
    sku_descontinuado,
    -- Auditoria
    current_timestamp()                                              AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.itens_pedido) AS _linhas_origem
FROM com_sku;

ALTER TABLE lakehouse_rotaperfume.silver.itens_pedido
    SET TBLPROPERTIES ('comment' =
        'Itens de pedido do ERP — devolucao marcada (quantidade<0), '
        'sku_descontinuado por join com produtos. Linhas de devolucao NAO sao descartadas.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.itens_pedido.devolucao IS
    'Boolean: true quando quantidade < 0. Devolucao NAO e erro nem e descartada — e marcada para analise.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.itens_pedido.quantidade_abs IS
    'abs(quantidade). Sempre positiva. Usar para somas — quantidade original pode ser negativa.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.itens_pedido.sku_descontinuado IS
    'true quando o SKU existe em produtos E produtos.ativo=false. Nao conserta — expoe.';

-- Constraint: quantidade_abs e sempre > 0
ALTER TABLE lakehouse_rotaperfume.silver.itens_pedido
    ADD CONSTRAINT quantidade_abs_positiva CHECK (quantidade_abs > 0);
