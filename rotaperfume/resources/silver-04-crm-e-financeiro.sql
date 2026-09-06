-- ============================================================================
-- SILVER · 04-crm-e-financeiro.sql
-- Vendedores, carteira, oportunidades, visitas, pagamentos, estoque.
-- Nao conserta dado — expõe o problema com coluna booleana.
-- ============================================================================

-- ── VENDEDORES ─────────────────────────────────────────────────────────────
-- Bronze: vendedor_id, nome, regiao, uf, data_admissao, data_desligamento, meta_mensal
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.vendedores AS
SELECT
    trim(vendedor_id)                                            AS vendedor_id,
    trim(nome)                                                   AS nome,
    trim(regiao)                                                 AS regiao,
    trim(uf)                                                     AS uf,
    -- Datas: admite e desligamento
    coalesce(
        try_to_date(data_admissao, 'yyyy-MM-dd'),
        try_to_date(data_admissao, 'dd/MM/yyyy')
    )                                                           AS data_admissao,
    coalesce(
        try_to_date(data_desligamento, 'yyyy-MM-dd'),
        try_to_date(data_desligamento, 'dd/MM/yyyy')
    )                                                           AS data_desligamento,
    -- ativo: true se nao tem data_desligamento
    CASE WHEN data_desligamento IS NULL THEN true ELSE false END AS ativo,
    -- Meta mensal como decimal
    try_cast(meta_mensal AS DECIMAL(18,2))                      AS meta_mensal,
    -- Auditoria
    current_timestamp()                                       AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.vendedores) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.vendedores;

ALTER TABLE lakehouse_rotaperfume.silver.vendedores
    SET TBLPROPERTIES ('comment' =
        'Equipe de vendas — tipagem corrigida, ativo como boolean, meta_mensal em decimal.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.vendedores.ativo IS
    'Boolean: true se data_desligamento e NULL, false caso contrario.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.vendedores.data_desligamento IS
    'NULL significa que o vendedor ainda esta ativo.';

-- ── CARTEIRA ───────────────────────────────────────────────────────────────
-- Vendedor desligado com carteira vigente: nao conserta — expõe.
-- Bronze: carteira_id, cliente_id, vendedor_id, data_inicio, data_fim
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.carteira AS
SELECT
    trim(carteira_id)                                          AS carteira_id,
    trim(cliente_id)                                           AS cliente_id,
    trim(vendedor_id)                                          AS vendedor_id,
    coalesce(
        try_to_date(data_inicio, 'yyyy-MM-dd'),
        try_to_date(data_inicio, 'dd/MM/yyyy')
    )                                                         AS data_inicio,
    coalesce(
        try_to_date(data_fim, 'yyyy-MM-dd'),
        try_to_date(data_fim, 'dd/MM/yyyy')
    )                                                         AS data_fim,

    -- vigente = sem data_fim E vendedor nao desligado
    CASE
        WHEN data_fim IS NULL
         AND NOT EXISTS (
             SELECT 1 FROM lakehouse_rotaperfume.silver.vendedores v
             WHERE v.vendedor_id = carteira.vendedor_id
               AND v.data_desligamento IS NOT NULL
         )
        THEN true
        ELSE false
    END                                                       AS vigente,

    -- orfao: vigente=true MAS o vendedor ja foi desligado (problema exposto)
    CASE
        WHEN data_fim IS NULL
         AND EXISTS (
             SELECT 1 FROM lakehouse_rotaperfume.silver.vendedores v
             WHERE v.vendedor_id = carteira.vendedor_id
               AND v.data_desligamento IS NOT NULL
         )
        THEN true
        ELSE false
    END                                                       AS orfao_vendedor_desligado,

    -- Auditoria
    current_timestamp()                                    AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.carteira) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.carteira;

ALTER TABLE lakehouse_rotaperfume.silver.carteira
    SET TBLPROPERTIES ('comment' =
        'Carteira de clientes por vendedor — vigente respeita data_fim E '
        'data_desligamento; orfao_vendedor_desligado expoe o problema, nao corrige.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.carteira.vigente IS
    'true quando data_fim=NULL e vendedor nao tem data_desligamento. false caso contrario.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.carteira.orfao_vendedor_desligado IS
    'true quando a carteira esta vigente MAS o vendedor ja foi desligado. Expõe o problema — nao corrige.';

-- ── OPORTUNIDADES ──────────────────────────────────────────────────────────
-- IMPORTANTE: conferir valores de etapa na bronze.
-- Etapas na origem: 'Fechado ganho', 'Fechado perdido', 'Negociaçao',
-- 'Proposta enviada', 'Prospecçao', 'Qualificaçao'
-- Bronze: oportunidade_id, cliente_id, vendedor_id, origem, data_abertura,
--         etapa, probabilidade_pct, valor_estimado, data_fechamento,
--         ciclo_dias, motivo_perda
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.oportunidades AS
SELECT
    trim(oportunidade_id)                                     AS oportunidade_id,
    trim(cliente_id)                                          AS cliente_id,
    trim(vendedor_id)                                         AS vendedor_id,
    trim(origem)                                              AS origem,

    -- Etapa: verbatim da origem, mapeada para texto normalizado
    CASE
        WHEN trim(etapa) = 'Fechado ganho'   THEN 'ganha'
        WHEN trim(etapa) = 'Fechado perdido'  THEN 'perdida'
        ELSE lower(trim(etapa))
    END                                                       AS etapa,

    -- Booleanos derivados da etapa
    CASE WHEN trim(etapa) = 'Fechado ganho'  THEN true ELSE false END AS ganancia,
    CASE WHEN trim(etapa) = 'Fechado perdido' THEN true ELSE false END AS perdida,

    coalesce(
        try_to_date(data_abertura, 'yyyy-MM-dd'),
        try_to_date(data_abertura, 'dd/MM/yyyy')
    )                                                         AS data_abertura,

    coalesce(
        try_to_date(data_fechamento, 'yyyy-MM-dd'),
        try_to_date(data_fechamento, 'dd/MM/yyyy')
    )                                                         AS data_fechamento,

    try_cast(probabilidade_pct AS DECIMAL(5,2))               AS probabilidade_pct,
    try_cast(valor_estimado AS DECIMAL(18,2))                 AS valor_estimado,
    try_cast(ciclo_dias AS INT)                               AS ciclo_dias,
    trim(motivo_perda)                                        AS motivo_perda,

    -- Auditoria
    current_timestamp()                                       AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.oportunidades) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.oportunidades;

ALTER TABLE lakehouse_rotaperfume.silver.oportunidades
    SET TBLPROPERTIES ('comment' =
        'Oportunidades comerciais — etapa normalizada para ganha/perdida, '
        'booleanos derivados, datas tipadas.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.oportunidades.etapa IS
    'Texto: ganha/perdida/negociacao/proposta-enviada/prospeccao/qualificacao. '
    'Mapeado de Fechado ganho/Fechado perdido na bronze.';

-- ── VISITAS ────────────────────────────────────────────────────────────────
-- Bronze: visita_id, cliente_id, vendedor_id, data_visita, resultado, duracao_min
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.visitas AS
SELECT
    trim(visita_id)                                          AS visita_id,
    trim(cliente_id)                                         AS cliente_id,
    trim(vendedor_id)                                        AS vendedor_id,
    coalesce(
        try_to_date(data_visita, 'yyyy-MM-dd'),
        try_to_date(data_visita, 'dd/MM/yyyy')
    )                                                       AS data_visita,
    trim(resultado)                                          AS resultado,
    try_cast(duracao_min AS INT)                             AS duracao_min,
    -- Auditoria
    current_timestamp()                                   AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.visitas) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.visitas;

ALTER TABLE lakehouse_rotaperfume.silver.visitas
    SET TBLPROPERTIES ('comment' =
        'Registros de visita — tipagem corrigida, duracao_min em INT.');

-- ── PAGAMENTOS ──────────────────────────────────────────────────────────────
-- Bronze: pagamento_id, pedido_id, forma_pagamento, parcelas, valor,
--         taxa_pct, valor_liquido, data_vencimento, data_pagamento, status_pagamento
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.pagamentos AS
SELECT
    trim(pagamento_id)                                       AS pagamento_id,
    trim(pedido_id)                                          AS pedido_id,
    trim(forma_pagamento)                                    AS forma_pagamento,
    try_cast(parcelas AS INT)                                AS parcelas,
    try_cast(valor AS DECIMAL(18,2))                         AS valor,
    try_cast(valor_liquido AS DECIMAL(18,2))                 AS valor_liquido,
    try_cast(taxa_pct AS DECIMAL(5,2))                      AS taxa_pct,
    coalesce(
        try_to_date(data_vencimento, 'yyyy-MM-dd'),
        try_to_date(data_vencimento, 'dd/MM/yyyy')
    )                                                       AS data_vencimento,
    coalesce(
        try_to_date(data_pagamento, 'yyyy-MM-dd'),
        try_to_date(data_pagamento, 'dd/MM/yyyy')
    )                                                       AS data_pagamento,
    trim(status_pagamento)                                  AS status_pagamento,
    -- Auditoria
    current_timestamp()                                    AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.pagamentos) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.pagamentos;

ALTER TABLE lakehouse_rotaperfume.silver.pagamentos
    SET TBLPROPERTIES ('comment' =
        'Pagamentos — data tipada com try_to_date, valores decimais, auditoria.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.pagamentos.data_pagamento IS
    'Coalesce de try_to_date(ISO) e try_to_date(BR). Sempre usar try_to_date em ANSI mode.';

-- ── ESTOQUE ────────────────────────────────────────────────────────────────
-- Bronze: data_snapshot, sku, saldo, ruptura (STRING com 'S'/'N')
CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.estoque AS
SELECT
    coalesce(
        try_to_date(data_snapshot, 'yyyy-MM-dd'),
        try_to_date(data_snapshot, 'dd/MM/yyyy')
    )                                                       AS data_snapshot,
    trim(sku)                                                AS sku,
    try_cast(saldo AS INT)                                   AS saldo,
    -- Ruptura: produto sem saldo. Nao conserta — sinaliza.
    CASE WHEN try_cast(saldo AS INT) = 0 THEN true ELSE false END AS ruptura,
    -- Auditoria
    current_timestamp()                                   AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.estoque) AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.estoque;

ALTER TABLE lakehouse_rotaperfume.silver.estoque
    SET TBLPROPERTIES ('comment' =
        'Controle de estoque — saldo como INT, ruptura como boolean (saldo=0). '
        'Nao corrige — sinaliza ruptura.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.estoque.ruptura IS
    'Boolean: true quando saldo=0. Sinaliza ruptura — nao cria pedido de reposicao automaticamente.';
