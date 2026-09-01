-- ============================================================================
-- GOLD · 08-testes.sql
-- Os 9 testes de qualidade que interrompem o pipeline se falharem.
-- Cada teste usa CASE WHEN ... THEN 'PASSOU' ELSE raise_error(...) END
-- para interromper a tarefa quando o valor estiver fora da tolerância.
-- ============================================================================

-- Teste 1: receita da gold = receita da silver
SELECT
    CASE
        WHEN ABS(
            ROUND((SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.fato_vendas), 2) -
            ROUND((SELECT SUM(valor_liquido) FROM lakehouse_rotaperfume.silver.pedidos), 2)
        ) <= 0.01
        THEN 'TESTE 1 OK — receita gold = silver'
        ELSE raise_error(
            'TESTE 1 FALHOU: receita da gold diferente da silver. ' ||
            'gold: ' || (SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.fato_vendas) ||
            ' | silver: ' || (SELECT ROUND(SUM(valor_liquido), 2) FROM lakehouse_rotaperfume.silver.pedidos)
        )
    END AS resultado,
    'receita gold = silver' AS teste;

-- Teste 2: CNPJ único na silver.clientes
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.silver.clientes) -
             (SELECT COUNT(DISTINCT cnpj) FROM lakehouse_rotaperfume.silver.clientes) = 0
        THEN 'TESTE 2 OK — CNPJ único'
        ELSE raise_error(
            'TESTE 2 FALHOU: CNPJs duplicados em silver.clientes. ' ||
            'Duplicados: ' ||
            ((SELECT COUNT(*) FROM lakehouse_rotaperfume.silver.clientes) -
             (SELECT COUNT(DISTINCT cnpj) FROM lakehouse_rotaperfume.silver.clientes))
        )
    END AS resultado,
    'CNPJ único' AS teste;

-- Teste 3: nenhuma data_pedido nula na silver.pedidos
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.silver.pedidos WHERE data_pedido IS NULL) = 0
        THEN 'TESTE 3 OK — sem datas nulas'
        ELSE raise_error(
            'TESTE 3 FALHOU: datas nulas em silver.pedidos. ' ||
            'Contagem: ' ||
            (SELECT COUNT(*) FROM lakehouse_rotaperfume.silver.pedidos WHERE data_pedido IS NULL)
        )
    END AS resultado,
    'data_pedido sem nulos' AS teste;

-- Teste 4: receita negativa só onde devolucao = true
SELECT
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM lakehouse_rotaperfume.gold.fato_vendas
            WHERE receita < 0 AND NOT devolucao
        )
        AND NOT EXISTS (
            SELECT 1 FROM lakehouse_rotaperfume.gold.fato_vendas
            WHERE receita >= 0 AND devolucao
        )
        THEN 'TESTE 4 OK — devolucao coerente'
        ELSE raise_error(
            'TESTE 4 FALHOU: inconsistência na devolucao. ' ||
            'receita < 0 sem devolucao=true OU receita >= 0 com devolucao=true'
        )
    END AS resultado,
    'devolução coerente' AS teste;

-- Teste 5: volume da gold.fato_vendas entre 140.000 e 250.000 linhas
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fato_vendas) BETWEEN 140000 AND 250000
        THEN 'TESTE 5 OK — volume dentro do esperado'
        ELSE raise_error(
            'TESTE 5 FALHOU: volume inesperado em gold.fato_vendas. ' ||
            'Linhas: ' || (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fato_vendas) ||
            ' (esperado: 140.000 a 250.000)'
        )
    END AS resultado,
    'volume fato_vendas' AS teste;

-- Teste 6: nenhum pedido_id na gold que não exista na silver.pedidos
SELECT
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM lakehouse_rotaperfume.gold.fato_vendas f
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse_rotaperfume.silver.pedidos p
                WHERE p.pedido_id = f.pedido_id
            )
        )
        THEN 'TESTE 6 OK — pedido_id válido'
        ELSE raise_error(
            'TESTE 6 FALHOU: pedido_id na gold que não existe na silver.pedidos'
        )
    END AS resultado,
    'pedido_id válido' AS teste;

-- Teste 7: clientes na gold.dim_cliente devem ter todos os CNPJs únicos
-- (e clientes órfãos em pedidos antigos, com id fora de silver.clientes,
-- são EXPOSITIVOS — não invalidam o teste)
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.dim_cliente) > 0
         AND (SELECT COUNT(DISTINCT cnpj) FROM lakehouse_rotaperfume.gold.dim_cliente) =
             (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.dim_cliente)
        THEN 'TESTE 7 OK — dim_cliente com CNPJs únicos'
        ELSE raise_error(
            'TESTE 7 FALHOU: dim_cliente com CNPJs duplicados ou vazia'
        )
    END AS resultado,
    'dim_cliente CNPJ único' AS teste;

-- Teste 8: mart_produto_performance soma o mesmo que fato_vendas
SELECT
    CASE
        WHEN ABS(
            ROUND((SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.mart_produto_performance), 2) -
            ROUND((SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.fato_vendas), 2)
        ) <= 0.01
        THEN 'TESTE 8 OK — mart conforme'
        ELSE raise_error(
            'TESTE 8 FALHOU: mart_produto_performance não confere com fato_vendas. ' ||
            'mart: ' || (SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.mart_produto_performance) ||
            ' | fato: ' || (SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.fato_vendas)
        )
    END AS resultado,
    'mart conforme' AS teste;

-- Teste 9: todo CNPJ com exatamente 14 dígitos na dim_cliente
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.dim_cliente WHERE length(cnpj) <> 14) = 0
        THEN 'TESTE 9 OK — CNPJ 14 dígitos'
        ELSE raise_error(
            'TESTE 9 FALHOU: CNPJ com diferente de 14 dígitos em dim_cliente. ' ||
            'Contagem: ' ||
            (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.dim_cliente WHERE length(cnpj) <> 14)
        )
    END AS resultado,
    'CNPJ 14 dígitos' AS teste;
