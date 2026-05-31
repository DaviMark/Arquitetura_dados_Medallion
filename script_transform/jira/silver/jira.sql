-- Simulação da criação da tabela Silver com limpeza de dados
CREATE OR REPLACE TABLE silver.clientes AS
SELECT
    id,
    UPPER(nome) AS nome_formatado,
    status,
    current_timestamp() AS data_processamento
FROM delta.`/mnt/datalake/bronze/clientes`
WHERE id IS NOT NULL;