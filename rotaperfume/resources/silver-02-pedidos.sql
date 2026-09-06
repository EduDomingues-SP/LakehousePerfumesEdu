-- ============================================================================
-- SILVER · 02-pedidos.sql
-- Tipa datas e valores, cria flags de cancelamento, extrai ano/mes.
-- Bronze: pedido_id, cliente_id, vendedor_id, data_pedido, canal, status, valor_total
-- ============================================================================

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.pedidos AS
WITH base AS (
    SELECT
        trim(pedido_id)      AS pedido_id,
        trim(cliente_id)     AS cliente_id,
        trim(vendedor_id)    AS vendedor_id,

        -- Data: dois formatos na bronze (ISO e BR) → um único DATE
        -- try_to_date nunca aborta a query com data malformada (ANSI mode!)
        coalesce(
            try_to_date(data_pedido, 'yyyy-MM-dd'),
            try_to_date(data_pedido, 'dd/MM/yyyy')
        ) AS data_pedido,

        trim(canal) AS canal,

        try_cast(valor_total AS DECIMAL(18,2)) AS valor_total,

        CASE WHEN trim(status) = 'Cancelado' THEN true ELSE false END AS cancelado,

        CASE
            WHEN trim(status) = 'Cancelado' THEN CAST(0 AS DECIMAL(18,2))
            ELSE try_cast(valor_total AS DECIMAL(18,2))
        END AS valor_liquido,

        current_timestamp() AS _processado_em,
        (SELECT count(*) FROM lakehouse_rotaperfume.bronze.pedidos) AS _linhas_origem
    FROM lakehouse_rotaperfume.bronze.pedidos
)
SELECT
    pedido_id,
    cliente_id,
    vendedor_id,
    data_pedido,
    canal,
    valor_total,
    cancelado,
    valor_liquido,
    -- Dimensões temporais extraídas da data já convertida
    year(data_pedido)  AS ano,
    month(data_pedido) AS mes,
    _processado_em,
    _linhas_origem
FROM base;

-- ── COMMENT ────────────────────────────────────────────────────────────────
ALTER TABLE lakehouse_rotaperfume.silver.pedidos
    SET TBLPROPERTIES ('comment' =
        'Pedidos do ERP — datas tipadas (ISO+BR aceitos), valores decimais, '
        'flag de cancelamento, valor_liquido=0 quando cancelado, canal preservado.');

COMMENT ON COLUMN lakehouse_rotaperfume.silver.pedidos.cancelado IS
    'Boolean: true se status=''Cancelado'', false caso contrário. O cancelado tem valor_liquido=0 pela constraint.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.pedidos.valor_liquido IS
    '0 quando cancelado, valor_total caso contrário. Não desconta devolução — isso é marcação no item (devolucao=true).';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.pedidos.data_pedido IS
    'Coalesce de try_to_date(ISO) e try_to_date(BR). Nunca aborta com data malformada (ANSI mode).';

-- ── Constraints ───────────────────────────────────────────────────────────
-- Data do pedido é obrigatória
ALTER TABLE lakehouse_rotaperfume.silver.pedidos
    ADD CONSTRAINT data_pedido_obrigatoria CHECK (data_pedido IS NOT NULL);

-- Pedido cancelado tem valor_liquido = ZERO.
-- ATENÇÃO: a regra intuitiva seria valor_liquido >= 0, mas 135 pedidos têm
-- valor negativo por causa de devolução. A constraint certa:
ALTER TABLE lakehouse_rotaperfume.silver.pedidos
    ADD CONSTRAINT pedido_cancelado_zerado CHECK (NOT cancelado OR valor_liquido = CAST(0 AS DECIMAL(18,2)));
