WITH jira_base_metrics AS (
    SELECT 
        -- Identificação e contexto original (colunas confirmadas no schema)
        issue_id,
        issue_key,
        summary,
        issue_type,
        is_subtask,
        parent_issue_key,
        status_id,
        status_name,
        assignee_id,
        priority_name,
        created_at,
        updated_at,

        -- Cálculo adaptado de tempo de abertura utilizando updated_at para fechados/alterados
        CASE 
            WHEN status_name IN ('Closed', 'Done', 'Resolved') OR status_id IN ('Closed', 'Done', 'Resolved') THEN 
                DATEDIFF(updated_at, created_at)
            ELSE 
                DATEDIFF(CURRENT_DATE(), created_at)
        END AS days_open,

        -- Nova métrica de controle: Dias desde a última atividade no ticket
        DATEDIFF(CURRENT_DATE(), updated_at) AS days_since_last_update

    FROM jira_silver.jira_issues_silver
)

SELECT 
    -- Identificação e contexto
    issue_id,
    issue_key,
    summary,
    issue_type,
    is_subtask,
    parent_issue_key,
    status_id,
    status_name,
    assignee_id,
    priority_name,
    
    -- Métricas de Tempo vinda da CTE adaptada
    days_open,
    days_since_last_update,
    
    -- Categorização de Duração
    CASE 
        WHEN days_open > 30 THEN 'Longa Duração'
        WHEN days_open BETWEEN 7 AND 30 THEN 'Média Duração'
        ELSE 'Curta Duração'
    END AS duration_category,
    
    -- Indicadores de Performance (KPIs de Produtividade)
    days_open - AVG(days_open) OVER(PARTITION BY issue_type) AS diff_from_avg_type,
    
    -- Status e Escalonamento
    CASE 
        WHEN priority_name IN ('High', 'Critical', 'Blocker') THEN 1 
        ELSE 0 
    END AS is_high_priority,
    
    -- Métricas de Carga de Trabalho (Workload)
    COUNT(issue_id) OVER(PARTITION BY assignee_id) AS total_issues_assignee,
    COUNT(issue_id) OVER(PARTITION BY status_id) AS total_issues_status,
    
    -- Percentual de carga do assignee no universo do pool de dados
    ROUND(
        COUNT(issue_id) OVER(PARTITION BY assignee_id) * 100.0 / 
        NULLIF(COUNT(*) OVER(), 0), 2
    ) AS pct_total_workload_assignee,

    -- Flags de monitoramento de risco / SLA (ajustado para usar status_name ou status_id)
    CASE 
        WHEN (status_name IN ('To Do', 'Open') OR status_id IN ('To Do', 'Open')) AND days_open > 15 THEN 1 
        ELSE 0 
    END AS is_stagnated_issue,
    
    CURRENT_TIMESTAMP() AS processed_at

FROM jira_base_metrics