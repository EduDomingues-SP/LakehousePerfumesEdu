SELECT 'pedidos' as tbl, collect_set(column_name) as cols FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='pedidos'
UNION ALL SELECT 'vendedores', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='vendedores'
UNION ALL SELECT 'pagamentos', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='pagamentos'
UNION ALL SELECT 'produtos', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='produtos'
UNION ALL SELECT 'itens_pedido', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='itens_pedido'
UNION ALL SELECT 'oportunidades', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='oportunidades'
UNION ALL SELECT 'visitas', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='visitas'
UNION ALL SELECT 'carteira', collect_set(column_name) FROM lakehouse_rotaperfume.information_schema.columns WHERE table_schema='bronze' AND table_name='carteira';
