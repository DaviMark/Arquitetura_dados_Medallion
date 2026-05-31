### 1. Estrutura de Pastas Atualizada

```text
meu-portfolio-dados/
├── .github/workflows/
├── dags/
├── governance/                 # Nova pasta de governança
│   ├── data_policy.md          # Documento central de governança
│   └── data_dictionary.csv     # Dicionário de dados (opcional)
├── script_ingestion/
│   └── jira/
│       └── ingest_jira_api.py  # Nomeclatura explicativa
├── script_transform/
│   └── jira/
│       ├── silver/
│       │   └── transform_jira_silver.sql
│       └── gold/
│           └── transform_jira_gold.sql
└── ...

```

---

### 2. Convenções de Nomeclatura e Padrões (Best Practices)

Para garantir que qualquer pessoa (ou você mesmo no futuro) entenda o código rapidamente:

* **Scripts Python (`.py`):** Use o prefixo da ação e o domínio.
* Ex: `ingest_[fonte]_[objeto].py` (ex: `ingest_jira_issues.py`)


* **Scripts SQL (`.sql`):** Use o prefixo da transformação.
* Ex: `transform_[origem]_[camada].sql` (ex: `transform_jira_silver.sql`)


* **Aliasing no SQL (`AS`):** Use alias de forma descritiva e consistente, evitando nomes curtos como `a`, `b` ou `c`.
* **Errado:** `SELECT id, nome AS n FROM tabela AS t`
* **Certo:** `SELECT issue_id, issue_name AS project_task_name FROM stg_jira_issues AS jira_data`



---

### 3. Documento de Governança (`governance/data_policy.md`)

```markdown
# Política de Governança de Dados - Projeto JIRA

## 1. Princípios de Nomenclatura
- **Tabelas:** Utilizar `[camada].[objeto]`. Ex: `silver.jira_tasks`.
- **Colunas:** Utilizar `snake_case`, sempre padronizando nomes de chaves primárias.
- **Aliasing:** O uso de `AS` é obrigatório para colunas calculadas e para todos os `JOINs` de tabelas para garantir legibilidade.

## 2. Padrões de Transformação
- **Camada Bronze:** Sem transformações de regra de negócio, apenas alteração de formato (ex: JSON para Delta).
- **Camada Silver:** Limpeza de dados (trim, casting, remoção de nulos) e padronização (ex: conversão de fuso horário).
- **Camada Gold:** Agregações, cálculos de KPIs e modelos de dimensional (Star Schema).

## 3. Qualidade de Dados
- Toda carga deve validar se o `source_timestamp` não é nulo.
- Testes de integridade referencial devem ser executados na transição Silver -> Gold.

## 4. Segurança
- Dados sensíveis de usuários do Jira devem ser mascarados na camada Silver através de funções de hash ou supressão.

```

---

### 4. Exemplo de SQL Padronizado

Veja como aplicar os alias e a clareza exigida em uma arquitetura robusta:

**Arquivo: `script_transform/jira/silver/transform_jira_silver.sql**`

```sql
-- Padronização da camada Silver para tickets Jira
CREATE OR REPLACE TABLE silver.jira_tasks AS
SELECT 
    CAST(issue_id AS STRING) AS ticket_id,
    TRIM(summary) AS task_summary,
    LOWER(status) AS task_status,
    TO_TIMESTAMP(created_at) AS created_at_utc,
    current_timestamp() AS processed_at
FROM bronze.jira_raw_data AS raw_data
WHERE issue_id IS NOT NULL;

```

---