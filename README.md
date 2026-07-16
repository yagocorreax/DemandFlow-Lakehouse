# DemandFlow Lakehouse

Projeto de Engenharia de Dados criado para **simular localmente uma arquitetura Databricks + AWS**

## Objetivo

Construir um pipeline completo de dados utilizando ferramentas gratuitas rodando na infraesstrutura do Docker.

O projeto simulará:

- **Databricks:** Apache Spark, Delta Lake, arquitetura Medalhão, workflows e processamento incremental;
- **AWS:** S3, IAM, Secrets Manager e outros serviços por meio do LocalStack.

## Arquitetura

```text
PostgreSQL
    ↓
Debezium
    ↓
Apache Kafka
    ↓
Apache Spark
    ↓
LocalStack S3
    ↓
Raw → Bronze → Silver → Gold
    ↓
Trino
    ↓
Apache Superset
```

## Tecnologias

- Python
- PostgreSQL
- Docker
- LocalStack (AWS enviroment)
- Apache Kafka
- Debezium
- Apache Spark
- Delta Lake
- Apache Airflow
- Trino
- Apache Superset
- Pytest
- GitHub Actions

## Contexto

A aplicação representará uma empresa fictícia com dados de:

- produtos;
- vendas;
- pedidos;
- estoque;
- lojas;
- promoções;
- previsões de demanda.

### Status

**Em desenvolvimento**