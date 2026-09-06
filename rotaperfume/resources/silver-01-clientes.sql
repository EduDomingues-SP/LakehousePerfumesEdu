-- ============================================================================
-- SILVER · 01-clientes.sql
-- Normaliza CNPJ, deduplica por CNPJ, tipa, declara constraints.
-- Bronze: cliente_id, cnpj, razao_social, segmento, cidade, uf, bairro,
--         data_cadastro, ativo
-- ============================================================================

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.clientes AS
WITH
-- ── 1. Normaliza campos ────────────────────────────────────────────────────
bronze_clientes AS (
    SELECT
        trim(cliente_id)                                                AS cliente_id,
        -- CNPJ: trim → só dígitos → lpad com zero à esquerda
        -- Nunca converter para número (perde zeros)
        lpad(regexp_replace(trim(cnpj), '[^0-9]', ''), 14, '0')       AS cnpj,
        -- razão social: initcap + colapsa espaço duplo
        regexp_replace(initcap(trim(razao_social)), ' +', ' ')          AS razao_social,
        -- data em dois formatos: ISO ou BR. try_to_date nunca aborta a query.
        coalesce(
            try_to_date(data_cadastro, 'yyyy-MM-dd'),
            try_to_date(data_cadastro, 'dd/MM/yyyy')
        )                                                               AS data_cadastro,
        -- ativo: S/N → boolean
        CASE WHEN trim(ativo) = 'S' THEN true ELSE false END          AS ativo,
        -- Colunas do CRM preservadas
        trim(segmento)                                                  AS segmento,
        trim(cidade)                                                    AS cidade,
        trim(uf)                                                        AS uf,
        trim(bairro)                                                    AS bairro
    FROM lakehouse_rotaperfume.bronze.clientes
),

-- ── 2. Deduplicação: 40 CNPJs com dois cadastros, mantém o mais antigo ───
com_ordem AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY cnpj
            ORDER BY data_cadastro ASC, cliente_id ASC
        ) AS rn,
        -- Coleta todos os cliente_id com o mesmo CNPJ para rastreabilidade
        collect_list(cliente_id) OVER (
            PARTITION BY cnpj
            ORDER BY data_cadastro ASC, cliente_id ASC
        ) AS _todos_ids
    FROM bronze_clientes
),

deduplicado AS (
    SELECT
        cliente_id,
        cnpj,
        razao_social,
        data_cadastro,
        ativo,
        segmento,
        cidade,
        uf,
        bairro,
        -- IDs duplicados = todos menos o que ficou (primeiro pelo mais antigo)
        CASE
            WHEN size(_todos_ids) > 1
            THEN array_except(_todos_ids, array(cliente_id))
            ELSE NULL
        END AS cliente_ids_duplicados
    FROM com_ordem
    WHERE rn = 1   -- mantém só o cadastro mais antigo
)

-- ── 3. Seleciona e adiciona auditoria ───────────────────────────────────
SELECT
    cliente_id,
    cnpj,
    razao_social,
    data_cadastro,
    ativo,
    segmento,
    cidade,
    uf,
    bairro,
    cliente_ids_duplicados,
    -- Auditoria
    current_timestamp()                                        AS _processado_em,
    (SELECT count(*) FROM lakehouse_rotaperfume.bronze.clientes) AS _linhas_origem
FROM deduplicado;

-- ── 4. COMMENT — documenta as decisões de limpeza ───────────────────────────
ALTER TABLE lakehouse_rotaperfume.silver.clientes
    SET TBLPROPERTIES ('comment' =
        'Clientes do CRM — CNPJ normalizado para 14 dígitos, deduplicado por CNPJ '
        'mantendo o cadastro mais antigo, razão social em initcap, colunas do CRM preservadas.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.cnpj IS
    'CNPJ normalizado: trim → regexp_replace(so digitos) → lpad(14, ''0''). Nunca convertido para número.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.cliente_ids_duplicados IS
    'Array com os cliente_id descartados na deduplicacaoo. Usado para rastrear pedidos antigos que apontam para o id antigo.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.ativo IS
    'Boolean: true=S, false=N. Campos em string na bronze foram convertidos.';

-- ── 5. Constraints — o contrato gravado na tabela ──────────────────────────
-- CNPJ tem sempre 14 dígitos
ALTER TABLE lakehouse_rotaperfume.silver.clientes
    ADD CONSTRAINT cnpj_14_digitos CHECK (length(cnpj) = 14);

-- Toda empresa tem data de cadastro
ALTER TABLE lakehouse_rotaperfume.silver.clientes
    ADD CONSTRAINT data_cadastro_obrigatoria CHECK (data_cadastro IS NOT NULL);
