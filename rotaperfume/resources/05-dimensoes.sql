-- ============================================================================
-- GOLD · 05-dimensoes.sql
-- Quatro dimensões conformadas para o modelo de negócio.
-- Lidas SOMENTE da silver — nunca da bronze.
-- ============================================================================

-- ── dim_cliente ─────────────────────────────────────────────────────────────
-- Uma linha por cliente: segmento, cidade, uf, data de cadastro, data do
-- primeiro e do último pedido, total de pedidos, receita acumulada,
-- dias desde a última compra.
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_cliente AS
WITH pedidos_cliente AS (
    SELECT
        p.cliente_id,
        COUNT(DISTINCT p.pedido_id)                                  AS total_pedidos,
        SUM(i.valor_bruto)                                           AS receita_acumulada,
        MIN(p.data_pedido)                                           AS primeiro_pedido,
        MAX(p.data_pedido)                                           AS ultimo_pedido
    FROM lakehouse_rotaperfume.silver.pedidos p
    JOIN lakehouse_rotaperfume.silver.itens_pedido i
        ON p.pedido_id = i.pedido_id
    WHERE NOT p.cancelado
    GROUP BY p.cliente_id
)
SELECT
    c.cliente_id,
    c.cnpj,
    c.razao_social,
    c.segmento,
    c.cidade,
    c.uf,
    c.bairro,
    c.data_cadastro,
    c.ativo,
    COALESCE(pc.total_pedidos, 0)                                    AS total_pedidos,
    COALESCE(ROUND(pc.receita_acumulada, 2), 0)                     AS receita_acumulada,
    pc.primeiro_pedido,
    pc.ultimo_pedido,
    CASE
        WHEN pc.ultimo_pedido IS NOT NULL
        THEN DATEDIFF(CURRENT_DATE(), pc.ultimo_pedido)
        ELSE NULL
    END                                                              AS dias_sem_comprar
FROM lakehouse_rotaperfume.silver.clientes c
LEFT JOIN pedidos_cliente pc
    ON c.cliente_id = pc.cliente_id;

ALTER TABLE lakehouse_rotaperfume.gold.dim_cliente
    SET TBLPROPERTIES ('comment' =
        'Dimensão de cliente — uma linha por CNPJ deduplicado. Inclui métricas '
        'de comportamento derivadas dos pedidos: total_pedidos, receita_acumulada, '
        'primeiro/ultimo_pedido, dias_sem_comprar.');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_cliente.cnpj IS
    'CNPJ normalizado (14 dígitos). Chave natural da dimensão.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_cliente.dias_sem_comprar IS
    'Dias desde a última compra. NULL = nunca comprou. Usado para recência em RFM.';


-- ── dim_produto ─────────────────────────────────────────────────────────────
-- Uma linha por SKU: marca, categoria, nota olfativa, custo, preço de tabela,
-- data de lançamento, descontinuado.
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_produto AS
SELECT
    sku,
    nome,
    categoria,
    marca,
    nota_olfativa,
    preco_tabela,
    custo_unitario,
    unidade,
    ativo,
    data_lancamento,
    CASE WHEN ativo = false THEN true ELSE false END AS descontinuado
FROM lakehouse_rotaperfume.silver.produtos;

ALTER TABLE lakehouse_rotaperfume.gold.dim_produto
    SET TBLPROPERTIES ('comment' =
        'Dimensão de produto — uma linha por SKU. Inclui atributos do catálogo '
        'e flag descontinuado derivada de produtos.ativo=false.');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_produto.custo_unitario IS
    'Custo unitário do produto. Usado para calcular custo e margem na fato_vendas.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_produto.descontinuado IS
    'true quando produtos.ativo=false. Indica SKU descontinuado — não remove da dimensão.';


-- ── dim_vendedor ────────────────────────────────────────────────────────────
-- Uma linha por vendedor: região, meta mensal, ativo.
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_vendedor AS
SELECT
    vendedor_id,
    nome,
    regiao,
    uf,
    meta_mensal,
    ativo
FROM lakehouse_rotaperfume.silver.vendedores;

ALTER TABLE lakehouse_rotaperfume.gold.dim_vendedor
    SET TBLPROPERTIES ('comment' =
        'Dimensão de vendedor — uma linha por vendedor. Meta mensal em decimal. '
        'Flag ativo = (data_desligamento IS NULL).');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_vendedor.meta_mensal IS
    'Meta mensal de vendas do vendedor. Usada para calcular atingimento no mart_vendas_por_vendedor.';


-- ── dim_calendario ──────────────────────────────────────────────────────────
-- Uma linha por dia dos 24 meses: ano, mes, nome do mês, trimestre,
-- dia da semana, mes_pico_setor (abril, junho, outubro = TRUE).
CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_calendario AS
WITH datas AS (
    SELECT explode(sequence(
        DATE '2024-09-01',
        DATE '2026-08-31',
        INTERVAL 1 DAY
    )) AS data
)
SELECT
    data                                                         AS data,
    year(data)                                                   AS ano,
    month(data)                                                  AS mes,
    date_format(data, 'MMMM')                                    AS nome_mes,
    quarter(data)                                                AS trimestre,
    dayofweek(data)                                              AS dia_semana,  -- 1=domingo .. 7=sábado
    CASE
        WHEN month(data) IN (4, 6, 10) THEN true
        ELSE false
    END                                                          AS mes_pico_setor
FROM datas;

ALTER TABLE lakehouse_rotaperfume.gold.dim_calendario
    SET TBLPROPERTIES ('comment' =
        'Dimensão de calendário — uma linha por dia de 2024-09-01 a 2026-08-31. '
        'mes_pico_setor identifica meses de sazonalidade alta do setor (abril, junho, outubro).');

COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_calendario.mes_pico_setor IS
    'TRUE para abril, junho e outubro — meses de pico sazonal do setor de perfumaria.';