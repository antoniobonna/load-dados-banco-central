
-- dados_banco_central_dw.data
INSERT INTO dados_banco_central_dw.data
SELECT format ('%s-%s-01',ano,3*trimestre)::date AS data, ano, trimestre FROM 
(SELECT DISTINCT date_part('year',data)::smallint ano, date_part('quarter',data)::smallint trimestre FROM dados_banco_central.resumo_stg
UNION
SELECT DISTINCT date_part('year',data)::smallint ano, date_part('quarter',data)::smallint trimestre FROM dados_banco_central.reclamacoes_por_banco_stg) a 
--WHERE format('%s-%s-01',ano,3*trimestre)::date NOT IN (SELECT data from dados_banco_central_dw.data)
ORDER BY 1;

VACUUM VERBOSE ANALYZE dados_banco_central_dw.data;

----------------------------------------------------------------------------

-- dados_banco_central_dw.banco

-- INSERT INTO dados_banco_central_dw.banco(banco)
-- SELECT banco FROM
-- (SELECT DISTINCT conglomerado as banco FROM dados_banco_central.resumo_stg
 -- WHERE conglomerado not like '%DISTRIBUIDORA%' AND conglomerado not like '%MICROEMPREENDEDOR%'
 -- AND conglomerado not like '%CORRETORA%' AND conglomerado not like '%AGENCIA%' AND conglomerado not like '%AGÊNCIA%'
 -- AND conglomerado not like '%COOPERATIVA%' AND conglomerado not like '%ASSOCIACAO%' AND conglomerado not like '%COOPERATIVO%'
 -- AND conglomerado not like 'CC%' AND conglomerado not like 'CCE%' AND conglomerado not like 'CCF%' AND conglomerado not like 'CCM%'
 -- AND conglomerado not like 'CCMS%' AND conglomerado not like 'CECM%'
-- UNION
 -- SELECT banco from
-- (SELECT DISTINCT banco FROM dados_banco_central.reclamacoes_por_banco_stg
-- UNION ALL
-- SELECT DISTINCT banco FROM dados_banco_central.ranking_reclamacoes_stg where data < '2014-07-01' ) bc
-- ) a
-- WHERE banco NOT IN (SELECT banco from dados_banco_central_dw.banco)
-- ORDER BY 1;

INSERT INTO dados_banco_central_dw.banco(banco)
SELECT banco FROM
(SELECT DISTINCT conglomerado as banco FROM dados_banco_central.resumo_stg
 WHERE conglomerado not like '%DISTRIBUIDORA%' AND conglomerado not like '%MICROEMPREENDEDOR%'
 AND conglomerado not like '%CORRETORA%' AND conglomerado not like '%AGENCIA%' AND conglomerado not like '%AGÊNCIA%'
 AND conglomerado not like '%COOPERATIVA%' AND conglomerado not like '%ASSOCIACAO%' AND conglomerado not like '%COOPERATIVO%'
 AND conglomerado not like 'CC%' AND conglomerado not like 'CCE%' AND conglomerado not like 'CCF%' AND conglomerado not like 'CCM%'
 AND conglomerado not like 'CCMS%' AND conglomerado not like 'CECM%'
UNION
(SELECT DISTINCT banco FROM dados_banco_central.reclamacoes_por_banco_stg) a
WHERE banco NOT IN (SELECT banco from dados_banco_central_dw.banco)
ORDER BY 1;

VACUUM VERBOSE ANALYZE dados_banco_central_dw.banco;

------------------------------------------------------------------------------

-- dados_banco_central_dw.cidade
-- INSERT INTO dados_banco_central_dw.cidade(cidade,uf)
-- SELECT DISTINCT cidade,uf FROM dados_banco_central.resumo_stg
 -- WHERE conglomerado not like '%DISTRIBUIDORA%' AND conglomerado not like '%MICROEMPREENDEDOR%'
 -- AND conglomerado not like '%CORRETORA%' AND conglomerado not like '%AGENCIA%' AND conglomerado not like '%AGÊNCIA%'
 -- AND conglomerado not like '%COOPERATIVA%' AND conglomerado not like '%ASSOCIACAO%' AND conglomerado not like '%COOPERATIVO%'
 -- AND conglomerado not like 'CC%' AND conglomerado not like 'CCE%' AND conglomerado not like 'CCF%' AND conglomerado not like 'CCM%'
 -- AND conglomerado not like 'CCMS%' AND conglomerado not like 'CECM%'
-- AND (cidade,uf) NOT IN (SELECT cidade,uf from dados_banco_central_dw.cidade)
-- ORDER BY 2,1;

-- VACUUM VERBOSE ANALYZE dados_banco_central_dw.data;

-----------------------------------------------------------------------------------

-- dados_banco_central_dw.dados

-- WITH resumo AS
-- (SELECT data, conglomerado banco, SUM(ativo_total)::bigint ativo_total, SUM(patrimonio_liquido)::bigint patrimonio_liquido, 
	-- SUM(lucro_liquido)::bigint lucro_liquido, SUM(numero_agencias) numero_agencias, SUM(numero_postos_atendimento) numero_postos
	-- FROM dados_banco_central.resumo_stg
	-- GROUP BY data, conglomerado),
-- reclamacoes AS
-- (SELECT format ('%s-%s-01',ano,3*trimestre)::date AS data,banco, round(indice_reclamacoes::numeric,2)::real indice_reclamacoes, 
	-- quantidade_total_clientes qtd_clientes, qtd_reclamacoes, qtd_reclamacoes_procedentes::bigint
	-- FROM	
 -- (select date_part('year',data) ano,date_part('quarter',data) trimestre,data,banco,quantidade_total_clientes, 
  -- row_number() over(partition by banco,date_part('year',data),date_part('quarter',data) order by data desc) sortdate,
  -- (avg(indice) over(partition by banco,date_part('year',data),date_part('quarter',data)))::real indice_reclamacoes,
  -- SUM(quantidade_total_reclamacoes) over(partition by banco,date_part('year',data),date_part('quarter',data)) as qtd_reclamacoes,
  -- SUM(quantidade_reclamacoes_reguladas_procedentes) over(partition by banco,date_part('year',data),date_part('quarter',data)) as qtd_reclamacoes_procedentes
 -- FROM dados_banco_central.reclamacoes_por_banco_stg
  -- UNION ALL 
 -- SELECT date_part('year',data),date_part('quarter',data),data,banco,clientes, 
  -- row_number() over(partition by banco,date_part('year',data),date_part('quarter',data) order by data desc) sortdate,
  -- (avg(indice) over(partition by banco,date_part('year',data),date_part('quarter',data)))::real indice_reclamacoes,
  -- NULL::bigint as qtd_reclamacoes,
  -- SUM(reclamacoes_procedentes) over(partition by banco,date_part('year',data),date_part('quarter',data)) as qtd_reclamacoes_procedentes
  -- FROM dados_banco_central.ranking_reclamacoes_stg where data < '2014-07-01') a
 -- where sortdate = 1),
-- clientes AS
-- (SELECT r.data, r.banco, dados_banco_central.max_clientes(r.qtd_clientes,qtd_clientes_ativos) qtd_clientes, qtd_clientes_ativos, qtd_operacoes_credito,
-- case when dados_banco_central.max_clientes(r.qtd_clientes,qtd_clientes_ativos) >= 4000000 then 1::int else 2::int end as categoria_id
 -- FROM reclamacoes r LEFT JOIN
 -- (SELECT data, instituicao_financeira banco, SUM(quantidade_de_clientes)::bigint qtd_clientes_ativos, SUM(quantidade_de_operacoes)::bigint qtd_operacoes_credito
 -- FROM dados_banco_central.clientes_operacoes_stg 
 -- GROUP BY data, instituicao_financeira) cli ON r.data=cli.data AND r.banco=cli.banco)

-- SELECT d.data, b.banco_id, c.categoria_id, o.indice indice_ouvidoria, indice_reclamacoes, c.qtd_clientes, qtd_clientes_ativos, qtd_operacoes_credito, 
-- qtd_reclamacoes, qtd_reclamacoes_procedentes, ativo_total, patrimonio_liquido, lucro_liquido, numero_agencias, numero_postos
-- FROM dados_banco_central_dw.banco b
-- JOIN resumo ON resumo.banco = b.banco
-- JOIN dados_banco_central_dw.data d ON d.data = resumo.data
-- LEFT JOIN dados_banco_central.ranking_ouvidoria_stg o ON o.banco=b.banco AND o.data = d.data
-- LEFT JOIN reclamacoes r ON r.data=d.data AND r.banco=b.banco
-- LEFT JOIN clientes c ON c.banco=b.banco AND c.data=d.data
-- ORDER BY d.data;

COPY (
WITH resumo AS
(SELECT data, conglomerado banco, SUM(ativo_total)::bigint ativo_total, SUM(patrimonio_liquido)::bigint patrimonio_liquido, 
	SUM(lucro_liquido)::bigint lucro_liquido, SUM(numero_agencias) numero_agencias, SUM(numero_postos_atendimento) numero_postos
	FROM dados_banco_central.resumo_stg
	GROUP BY data, conglomerado),
reclamacoes AS
(SELECT data, banco, indice indice_reclamacoes, quantidade_total_clientes qtd_clientes, quantidade_total_reclamacoes qtd_reclamacoes, 
 quantidade_reclamacoes_reguladas_procedentes qtd_reclamacoes_procedentes
	FROM dados_banco_central.reclamacoes_por_banco_stg),
clientes AS
(SELECT r.data, r.banco, dados_banco_central.max_clientes(r.qtd_clientes,qtd_clientes_ativos) qtd_clientes, qtd_clientes_ativos, qtd_operacoes_credito,
case when dados_banco_central.max_clientes(r.qtd_clientes,qtd_clientes_ativos) >= 4000000 then 1::int else 2::int end as categoria_id
 FROM reclamacoes r LEFT JOIN
 (SELECT data, instituicao_financeira banco, SUM(quantidade_de_clientes)::bigint qtd_clientes_ativos, SUM(quantidade_de_operacoes)::bigint qtd_operacoes_credito
 FROM dados_banco_central.clientes_operacoes_stg 
 GROUP BY data, instituicao_financeira) cli ON r.data=cli.data AND r.banco=cli.banco)

SELECT d.data, b.banco_id, c.categoria_id, o.indice indice_ouvidoria, indice_reclamacoes, c.qtd_clientes, qtd_clientes_ativos, qtd_operacoes_credito, 
qtd_reclamacoes, qtd_reclamacoes_procedentes, ativo_total, patrimonio_liquido, lucro_liquido, numero_agencias, numero_postos
FROM dados_banco_central_dw.banco b
JOIN resumo ON resumo.banco = b.banco
JOIN dados_banco_central_dw.data d ON d.data = resumo.data
LEFT JOIN dados_banco_central.ranking_ouvidoria_stg o ON o.banco=b.banco AND o.data = d.data
LEFT JOIN reclamacoes r ON r.data=d.data AND r.banco=b.banco
LEFT JOIN clientes c ON c.banco=b.banco AND c.data=d.data
) to '/home/ubuntu/dump/dados_bancos.txt';

COPY dados_banco_central_dw.dados_bancos FROM '/home/ubuntu/dump/dados_bancos.txt';

VACUUM VERBOSE ANALYZE dados_banco_central_dw.dados_bancos;