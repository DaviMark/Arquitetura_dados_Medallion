-- Simulação da criação da tabela Gold com agregações para o negócio
CREATE OR REPLACE TABLE gold.clientes_agregados AS
SELECT
    status,
    COUNT(id) AS total_clientes,
    MAX(data_processamento) AS ultima_atualizacao
FROM silver.clientes
GROUP BY status;