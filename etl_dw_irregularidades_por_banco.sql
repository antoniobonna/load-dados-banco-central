
-- dados_banco_central_dw.irregularidade

\! echo "Carregando dados na dimensão irregularidade..."

INSERT INTO dados_banco_central_dw.irregularidade
SELECT distinct irregularidade FROM dados_banco_central.irregularidades_por_banco_stg 
	WHERE irregularidade NOT IN (SELECT irregularidade FROM dados_banco_central_dw.irregularidade);

VACUUM VERBOSE ANALYZE dados_banco_central_dw.irregularidade;

-- dados_banco_central_dw.irregularidades_por_banco

\! echo "Carregando dados tabela fato irregularidades_por_banco..."

COPY (
SELECT d.data, b.banco_id, i.irregularidade_id, ir.quantidade_total_reclamacoes qtd_reclamacoes, 
	ir.quantidade_reclamacoes_reguladas_procedentes qtd_reclamacoes_procedentes
	FROM dados_banco_central.irregularidades_por_banco_stg ir
	JOIN dados_banco_central_dw.data d ON d.data=ir.data
	JOIN dados_banco_central_dw.banco b ON b.banco=ir.banco
	JOIN dados_banco_central_dw.irregularidade i ON i.irregularidade=ir.irregularidade
) to '/home/ubuntu/dump/irregularidades_por_banco.txt';
COPY dados_banco_central_dw.irregularidades_por_banco FROM '/home/ubuntu/dump/irregularidades_por_banco.txt';

VACUUM VERBOSE ANALYZE dados_banco_central_dw.irregularidades_por_banco;