-- Descrição: Tabela de dimensão para os assignees dos tickets do Jira.
SELECT DISTINCT
    assignee_id,
    assignee_name 
FROM jira_silver.jira_issues_silver