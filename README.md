# Projeto: Arquitetura de Dados Escalável Medallion

## Visão Geral

Este repositório contém a infraestrutura de dados para o domínio de gestão de projetos (Jira), projetada para garantir escalabilidade, governança e reprodutibilidade analítica. O projeto segue a arquitetura **Medallion (Bronze/Silver/Gold)** e é orquestrado via **Airflow**, com automação de CI/CD para garantir a integridade do código.

## Arquitetura Técnica

* **Processamento**: Apache Spark (Databricks).
* **Orquestração**: Apache Airflow.
* **Versionamento e CI/CD**: GitHub Actions.
* **Governança**: Estrutura modular por domínio, separação clara entre ingestão (Python) e transformação (SQL).

## Estrutura do Repositório

```text
.
├── .github/workflows/      # Pipelines de CI/CD (Linting e Deploy)
├── dags/                   # Orquestração do fluxo de dados
├── governance/             # Governança de dados
├── script_ingestion/       # Camada Bronze: Extração via API
├── script_transform/       # Camadas Silver/Gold: Transformações SQL
├── tests/                  # Testes unitários para pipelines
└── README.md

```

## Destaques da Solução

* **Modularidade**: Arquitetura orientada a domínios (Jira), facilitando a manutenção e a integração de novas fontes de dados.
* **Esteira de CI/CD**: Implementação de *Linting* (Flake8 para Python, SQLFluff para SQL) que garante a qualidade do código antes de qualquer deploy em produção.
* **Governança de dados**: Estruturação de dicionário de dados centralizado e controle de acesso baseado em funções, com mascaramento de dados sensíveis na camada Silver, garantindo a privacidade e a padronização terminológica em todo o pipeline.
* **Pipeline Medallion**:
* **Bronze**: Ingestão bruta via API, mantendo a fidelidade da fonte.
* **Silver**: Limpeza, padronização e estruturação dos dados.
* **Gold**: Agregações de negócio para suporte à tomada de decisão estratégica.



## Como Executar

1. **Pré-requisitos**: Cluster Databricks configurado, Airflow (MWAA ou self-hosted) e integração configurada com GitHub.
2. **Setup**: Clone o repositório e configure as variáveis de ambiente necessárias (secrets) para conexão com o Databricks.
3. **CI/CD**: O pipeline é disparado automaticamente a cada `push` ou `pull request` para a branch `main`, validando a sintaxe e a estrutura do código.

## Profissional

Desenvolvido com foco em alta performance, escalabilidade e governança de dados. Este projeto reflete a aplicação de boas práticas em engenharia de dados para transformar informações em ativos estratégicos.

---