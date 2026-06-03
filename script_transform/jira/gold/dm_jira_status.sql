-- -- Descrição: Tabela de dimensão para os status dos tickets do Jira.
SELECT DISTINCT
    status_id,
    status_name 
FROM jira_silver.jira_issues_silver